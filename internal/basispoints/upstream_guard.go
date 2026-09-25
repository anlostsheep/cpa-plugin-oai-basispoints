package basispoints

import (
	"sync"
	"time"
)

// upstreamGuard 统一管理一次 HTTP 上游往返的中止：客户端断开（stop）、插件停止（shutdown）
// 或超过 timeout_seconds 时，由唯一的看守协程记录中止原因并中止上游。
//
// 中止分两段，覆盖整个往返：
//   - 头部阶段：host.http.do_stream 会同步等待上游响应头。守卫在发请求前通过
//     host.http.operation_open 申请 operation_id 并随请求携带，中止时调用
//     host.http.cancel 取消该 operation，从而解除阻塞中的 do_stream（CPA 7.3.17
//     http_operation_bridge.go）。宿主不支持时 operation_id 为空，退化为只能在
//     拿到 stream ID 后中止。
//   - 读取阶段：拿到 stream ID 后 attach；中止时调用 host.http.stream_close，宿主据此
//     取消上游 ctx，解除阻塞中的 stream_read（http_stream_bridge.go）。
//
// release 必须在往返结束时调用：它停止看守协程并**等待其退出**，随后恰好一次地关闭
// 上游流并清理未使用的 operation，保证往返结束（WaitGroup Done）之后不再有宿主回调。
type upstreamGuard struct {
	s           *Service
	callbackID  string
	operationID string
	secret      string

	mu       sync.Mutex
	streamID string
	reason   error

	closeOnce sync.Once
	released  sync.Once
	done      chan struct{}
	exited    chan struct{}

	// 中止条件本身（供 aborted() 同步判定，不依赖看守协程是否已被调度）。
	stopC         <-chan struct{}
	shutdownC     <-chan struct{}
	deadline      time.Time
	timeoutReason error

	// headers 在 do_stream 返回（响应头已到或建连失败）时关闭，供延迟心跳判断「状态码已知」。
	headers     chan struct{}
	headersOnce sync.Once
}

// markHeaders 标记 do_stream 已返回：此后状态码已知，错误正文的读取是有界的。
func (g *upstreamGuard) markHeaders() {
	g.headersOnce.Do(func() { close(g.headers) })
}

type hostOperationOpenResult struct {
	OperationID string `json:"operation_id"`
}

// newUpstreamGuard 申请 operation 并启动看守协程。stop/shutdown 可为 nil；timeout<=0 表示不计时。
func (s *Service) newUpstreamGuard(callbackID, secret string, stop, shutdown <-chan struct{}, timeout time.Duration, abortReasonOnTimeout error) *upstreamGuard {
	g := &upstreamGuard{
		s:          s,
		callbackID: callbackID,
		secret:     secret,
		done:       make(chan struct{}),
		exited:     make(chan struct{}),
		headers:    make(chan struct{}),

		stopC:         stop,
		shutdownC:     shutdown,
		timeoutReason: abortReasonOnTimeout,
	}
	if timeout > 0 {
		g.deadline = time.Now().Add(timeout)
	}
	// operation 绑定在宿主回调上下文（host_callback_id）上；CPA 执行器调用总会提供它。
	if callbackID != "" {
		var opened hostOperationOpenResult
		if err := s.call("host.http.operation_open", map[string]any{"host_callback_id": callbackID}, &opened); err == nil {
			g.operationID = opened.OperationID
		}
	}
	var timerC <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerC = timer.C
	}
	go func() {
		defer close(g.exited)
		if timer != nil {
			defer timer.Stop()
		}
		// 看守协程只负责「被唤醒」；原因一律由 resolveLocked 按统一优先级判定
		// （插件停止 > 客户端断开 > 超时），不受 select 在多个信号同时就绪时的随机选择影响。
		timerFired := false
		select {
		case <-g.done:
			return
		case <-stop:
		case <-shutdown:
		case <-timerC:
			timerFired = true
		}
		g.mu.Lock()
		g.resolveLocked(timerFired)
		g.mu.Unlock()
		g.abortUpstream()
	}()
	return g
}

// abortUpstream 取消 operation（解除头部等待）并关闭已 attach 的流（解除读取）。
func (g *upstreamGuard) abortUpstream() {
	if g.operationID != "" {
		_ = g.s.call("host.http.cancel", map[string]any{"host_callback_id": g.callbackID, "operation_id": g.operationID}, nil)
	}
	g.mu.Lock()
	streamID := g.streamID
	g.mu.Unlock()
	if streamID != "" {
		g.closeStream(streamID)
	}
}

func (g *upstreamGuard) closeStream(streamID string) {
	g.closeOnce.Do(func() {
		_ = g.s.call("host.http.stream_close", map[string]any{"stream_id": streamID}, nil)
	})
}

// attach 记录 do_stream 返回的 stream ID；若此前已中止，立即关闭该流。
func (g *upstreamGuard) attach(streamID string) {
	if streamID == "" {
		return
	}
	g.mu.Lock()
	g.streamID = streamID
	aborted := g.reason != nil
	g.mu.Unlock()
	if aborted {
		g.closeStream(streamID)
	}
}

// aborted 返回中止原因（未中止为 nil）。除看守协程已记录的原因外，还**同步**检查中止
// 条件本身：本代已停止、客户端已断开或已过总截止时间，但看守协程尚未被调度时，也能
// 立即得出原因（并记录下来，保证之后一致）。插件停止优先，避免被误报为凭据错误。
func (g *upstreamGuard) aborted() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.resolveLocked(false)
}

// resolveLocked 是中止原因的唯一判定点（调用方持有 g.mu），看守协程与同步检查共用：
// 一旦记录就保持不变；否则按优先级检查 插件停止 > 客户端断开 > 超时（到达截止时间，
// 或看守协程的计时器已触发）。
func (g *upstreamGuard) resolveLocked(timerFired bool) error {
	if g.reason != nil {
		return g.reason
	}
	timedOut := timerFired || (!g.deadline.IsZero() && !time.Now().Before(g.deadline))
	switch {
	case isClosed(g.shutdownC):
		g.reason = stoppedError()
	case isClosed(g.stopC):
		g.reason = fail(499, "client_disconnected", "client disconnected while receiving Basis Points stream")
	case timedOut && g.timeoutReason != nil:
		g.reason = g.timeoutReason
	}
	return g.reason
}

func isClosed(c <-chan struct{}) bool {
	if c == nil {
		return false
	}
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// release 停止看守协程并等待其退出，然后关闭上游流（恰好一次）并清理 operation。
func (g *upstreamGuard) release() {
	g.released.Do(func() {
		close(g.done)
		<-g.exited
		g.mu.Lock()
		streamID := g.streamID
		g.mu.Unlock()
		if streamID != "" {
			g.closeStream(streamID) // 流关闭时宿主会 finish 对应 operation
		} else if g.operationID != "" {
			// 从未拿到流（do_stream 失败或未发出）：清理可能未被认领的 operation。
			_ = g.s.call("host.http.cancel", map[string]any{"host_callback_id": g.callbackID, "operation_id": g.operationID}, nil)
		}
	})
}

// redact 按当前令牌精确脱敏后再做前缀脱敏。宿主错误串不可控，不能只依赖 "Bearer " 前缀。
func (g *upstreamGuard) redact(err error) string {
	if err == nil {
		return ""
	}
	secret := ""
	if g != nil {
		secret = g.secret
	}
	return redactSecret(err.Error(), secret)
}

// guardedDo 在守卫下执行一次 host.http.do：携带 operation_id，插件停止或超时时取消，
// 使附件上传与非流式请求同样可被 shutdown/超时中断。返回原始宿主错误，由调用方包装。
func (s *Service) guardedDo(request ExecutorRequest, secret string, payload map[string]any, out *upstreamResponse) error {
	cfg := s.config()
	var shutdown <-chan struct{}
	if request.lifeCtx != nil {
		shutdown = request.lifeCtx.Done()
	}
	g := s.newUpstreamGuard(request.HostCallbackID, secret, nil, shutdown, time.Duration(cfg.TimeoutSeconds)*time.Second, timeoutError(cfg))
	defer g.release()
	if g.operationID != "" {
		payload["operation_id"] = g.operationID
	}
	err := s.call("host.http.do", payload, out)
	if reason := g.aborted(); reason != nil {
		return reason
	}
	return err
}

// ensureLife 为请求绑定生命周期（若 execute 尚未绑定，例如测试直接调用执行分支）。
// 返回的 done 必须在往返结束时调用。
func (s *Service) ensureLife(request *ExecutorRequest) (func(), error) {
	if request.lifeCtx != nil && request.lifeDone != nil {
		return request.lifeDone, nil
	}
	ctx, done, err := s.beginStream()
	if err != nil {
		return nil, err
	}
	var once sync.Once
	request.lifeCtx = ctx
	request.lifeDone = func() { once.Do(done) }
	return request.lifeDone, nil
}
