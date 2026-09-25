package basispoints

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// streamSession 管理一次 executor.execute_stream 的下游输出，供 http 与 ws 两种传输共用。
//
// 背景：两种传输都要等上游完整回复（需在完整 item 上安全转换工具调用）后才回放，缓冲期间
// 下游收不到任何字节，长回合会被 sub2api stream_data_interval_timeout(180s) 或 Codex
// 空闲超时(300s) 切断。会话在开始时立即发出 response.created，并在缓冲期间按间隔发送
// response.in_progress 心跳；心跳 emit 失败即视为客户端已断开，立刻回调 onDisconnect
// 取消上游（ws 会据此发送 basispoints.response.cancel）。
//
// 失败语义（#6）：host.stream.close 的 error 只是一段字符串、不带状态码，CPA 一律按瞬时
// 错误冷却凭据。因此只有凭据/限流/传输类错误带 error 关闭，交给 CPA 处理；属于「本次
// 请求」的问题（模型输出了非法工具 code、工具调用不符合目录、上游半途失败/排空）改为
// 向客户端发送 response.failed 后正常关闭，避免凭据被无谓冷却。已输出的内容绝不重放。
type streamSession struct {
	s            *Service
	streamID     string
	responseID   string
	heartbeat    time.Duration
	onDisconnect func()

	mu           sync.Mutex
	sequence     int
	started      bool
	closed       bool
	disconnected bool

	stopHeartbeat chan struct{}
	heartbeatDone chan struct{}

	// closeOnce 保证 host.stream.close 恰好一次：宿主在关闭后会冲刷队列，若下游不读，
	// 第二次 close 会阻塞。forced 表示流已被看门狗强制关闭，之后不再 emit。
	closeOnce sync.Once
	forced    atomic.Bool
	ended     chan struct{}
	endOnce   sync.Once
	lifeCtx   context.Context
}

// shutdownForceGrace 是插件停止后给会话正常收尾（发 response.failed 并关闭）的宽限期；
// 超时仍未结束（通常是下游不读导致 emit 阻塞在宿主队列上）就强制关闭流。必须小于
// shutdownWait，保证往返能在 shutdown 等待期内退出、宿主回调全部返回。
var shutdownForceGrace = time.Second

// bindLifecycle 把会话绑定到往返所属的那一代。插件停止时：finish 改为发 plugin_stopped
// 失败（最终输出边界的停止检查），且看门狗在宽限期后强制关闭仍未结束的流——宿主的
// host.stream.close 即使在队列已满时也会被接受，并解除阻塞中的 emit。
func (ss *streamSession) bindLifecycle(ctx context.Context) {
	if ctx == nil {
		return
	}
	ss.lifeCtx = ctx
	grace := shutdownForceGrace // 绑定时取值，看门狗之后不再读取全局变量
	go func() {
		select {
		case <-ss.ended:
			return
		case <-ctx.Done():
		}
		if !stoppedByShutdown(ctx) {
			return
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-ss.ended:
		case <-timer.C:
			ss.forceClose()
		}
	}()
}

// forceClose 不持有 ss.mu（阻塞中的 emit 正持有它），直接关闭宿主流以解除阻塞。
func (ss *streamSession) forceClose() {
	ss.forced.Store(true)
	ss.closeStream(nil)
}

func (ss *streamSession) markEnded() {
	ss.endOnce.Do(func() { close(ss.ended) })
}

func (s *Service) newStreamSession(streamID string, heartbeat time.Duration, onDisconnect func()) *streamSession {
	return &streamSession{
		s:            s,
		streamID:     streamID,
		responseID:   "resp_bp_" + randomHex(16),
		heartbeat:    heartbeat,
		onDisconnect: onDisconnect,
		ended:        make(chan struct{}),
	}
}

// placeholderResponse 是心跳与失败事件里使用的最小响应对象，id 与最终 completed 保持一致。
func (ss *streamSession) placeholderResponse(status string) map[string]any {
	return map[string]any{
		"id":     ss.responseID,
		"object": "response",
		"status": status,
		"output": []any{},
	}
}

// emitLocked 写出事件；调用方持有 ss.mu。emit 失败即标记断开并回调取消上游。
func (ss *streamSession) emitLocked(events []sseEvent, done bool) error {
	if ss.closed || ss.disconnected || ss.forced.Load() {
		return errClientDisconnected
	}
	payload, next := renderSSE(events, ss.sequence, done)
	if err := ss.s.call("host.stream.emit", map[string]any{"stream_id": ss.streamID, "payload": payload}, nil); err != nil {
		ss.disconnected = true
		if ss.onDisconnect != nil {
			go ss.onDisconnect()
		}
		return errClientDisconnected
	}
	ss.sequence = next
	return nil
}

var errClientDisconnected = errors.New("client disconnected while receiving stream")

// start 立即发出 response.created + response.in_progress，并启动心跳。heartbeat<=0 时
// 不做任何提前输出，保持 v0.1.10 的一次性回放行为。
func (ss *streamSession) start() {
	if ss.heartbeat <= 0 {
		return
	}
	ss.mu.Lock()
	ss.started = true
	_ = ss.emitLocked([]sseEvent{
		{name: "response.created", value: map[string]any{"response": ss.placeholderResponse("in_progress")}},
		{name: "response.in_progress", value: map[string]any{"response": ss.placeholderResponse("in_progress")}},
	}, false)
	ss.mu.Unlock()

	ss.stopHeartbeat = make(chan struct{})
	ss.heartbeatDone = make(chan struct{})
	go func() {
		defer close(ss.heartbeatDone)
		ticker := time.NewTicker(ss.heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ss.stopHeartbeat:
				return
			case <-ticker.C:
				ss.mu.Lock()
				err := ss.emitLocked([]sseEvent{
					{name: "response.in_progress", value: map[string]any{"response": ss.placeholderResponse("in_progress")}},
				}, false)
				ss.mu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()
}

func (ss *streamSession) stop() {
	if ss.stopHeartbeat != nil {
		close(ss.stopHeartbeat)
		<-ss.heartbeatDone
		ss.stopHeartbeat = nil
	}
}

func (ss *streamSession) closeStream(err error) {
	ss.closeOnce.Do(func() {
		payload := map[string]any{"stream_id": ss.streamID}
		if err != nil {
			payload["error"] = safeError(err)
		}
		_ = ss.s.call("host.stream.close", payload, nil)
	})
}

// finish 回放已转换的完整响应并正常关闭。心跳开启时 id 统一为会话 id，并省略已发出的开场事件。
func (ss *streamSession) finish(response map[string]any) {
	defer ss.markEnded()
	ss.stop()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	// 最终输出边界的停止检查：插件已停止时不再回放完整响应。
	if ss.lifeCtx != nil && stoppedByShutdown(ss.lifeCtx) {
		ss.failLocked(stoppedError())
		return
	}
	if ss.disconnected || ss.forced.Load() {
		ss.closed = true
		ss.closeStream(nil)
		return
	}
	var events []sseEvent
	if ss.started {
		aligned := cloneObject(response)
		aligned["id"] = ss.responseID
		events = syntheticEvents(aligned, false)
	} else {
		events = syntheticEvents(response, true)
	}
	_ = ss.emitLocked(events, true)
	ss.closed = true
	ss.closeStream(nil)
}

// fail 按错误来源决定：凭据/限流/传输错误带 error 关闭（CPA 冷却或换号）；请求层面的错误
// 发 response.failed 后正常关闭（不冷却凭据）；客户端已断开时静默关闭。
func (ss *streamSession) fail(err error) {
	defer ss.markEnded()
	ss.stop()
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.failLocked(err)
}

func (ss *streamSession) failLocked(err error) {
	defer func() { ss.closed = true }()
	if ss.disconnected || ss.forced.Load() || errors.Is(err, errClientDisconnected) || isKind(err, "client_disconnected") {
		ss.closeStream(nil)
		return
	}
	if !isRequestScoped(err) {
		ss.closeStream(err)
		return
	}
	failed := ss.placeholderResponse("failed")
	failed["error"] = map[string]any{"code": errorKind(err), "message": safeError(err)}
	events := []sseEvent{{name: "response.failed", value: map[string]any{"response": failed}}}
	if !ss.started {
		// 未提前发出开场事件时补上 created，保证客户端事件序列完整。
		events = append([]sseEvent{{name: "response.created", value: map[string]any{"response": ss.placeholderResponse("in_progress")}}}, events...)
	}
	_ = ss.emitLocked(events, true)
	ss.closeStream(nil)
}

func errorKind(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Kind != "" {
		return apiErr.Kind
	}
	return "plugin_error"
}

func isKind(err error, kind string) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Kind == kind
}

// isRequestScoped 判断错误是否只与本次请求（模型输出/请求内容）有关。凭据失效(401/403)、
// 资源不存在(404)、限流(429)、上游传输/超时属于凭据或链路问题，交给 CPA 处理。
func isRequestScoped(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Kind {
	case "upstream_transport", "upstream_timeout", "upstream_response_too_large":
		return false
	}
	switch apiErr.Status {
	case 401, 402, 403, 404, 429:
		return false
	}
	return true
}
