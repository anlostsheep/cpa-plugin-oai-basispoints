package basispoints

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---- 高 1：延迟心跳 ----

// slowHeaderHost：do_stream 在 delay 后返回 status（阻塞期间可被 host.http.cancel 取消）；
// 读取一次性返回完整 SSE。记录每次 emit 的时间与 do_stream 返回时间。
type slowHeaderHost struct {
	mu          sync.Mutex
	delay       time.Duration
	status      int
	emitted     []byte
	emitTimes   []time.Time
	headersAt   time.Time
	closed      chan any
	cancelled   chan struct{}
	cancelOnce  sync.Once
	streamClose atomic.Int32
}

func newSlowHeaderHost(delay time.Duration, status int) *slowHeaderHost {
	return &slowHeaderHost{delay: delay, status: status, closed: make(chan any, 1), cancelled: make(chan struct{})}
}

const completedSSE = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_up\",\"status\":\"completed\",\"output\":[]}}\n\n"

func (h *slowHeaderHost) call(method string, payload any, out any) error {
	switch method {
	case "host.http.operation_open":
		out.(*hostOperationOpenResult).OperationID = "op-1"
	case "host.http.cancel":
		h.cancelOnce.Do(func() { close(h.cancelled) })
	case "host.http.do_stream":
		select {
		case <-time.After(h.delay):
		case <-h.cancelled:
			return errors.New("context canceled")
		}
		h.mu.Lock()
		h.headersAt = time.Now()
		h.mu.Unlock()
		*out.(*upstreamStream) = upstreamStream{StatusCode: h.status, StreamID: "up-1"}
	case "host.http.stream_read":
		if h.status >= 400 {
			*out.(*streamChunk) = streamChunk{Payload: []byte(`{"error":{"message":"denied"}}`), Done: true}
		} else {
			*out.(*streamChunk) = streamChunk{Payload: []byte(completedSSE), Done: true}
		}
	case "host.http.stream_close":
		h.streamClose.Add(1)
	case "host.stream.emit":
		h.mu.Lock()
		h.emitted = append(h.emitted, payload.(map[string]any)["payload"].([]byte)...)
		h.emitTimes = append(h.emitTimes, time.Now())
		h.mu.Unlock()
	case "host.stream.close":
		h.closed <- payload.(map[string]any)["error"]
	}
	return nil
}

func heartbeatService(host HostCall) *Service {
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(1) // grace = 1s
	svc.SetHost(host)
	return svc
}

func streamRequest(streamID string) []byte {
	return jsonBytes(ExecutorRequest{
		Model:          DefaultModelID,
		Payload:        jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi", "stream": true}),
		Stream:         true,
		StreamID:       streamID,
		HostCallbackID: "cb-1",
		StorageJSON:    jsonBytes(map[string]any{"access_token": "tok-x", "account_id": "acct"}),
	})
}

// 响应头在宽限期内以 429 返回：同步返回带状态码的错误，下游零字节。
func TestHTTPFastHeaderErrorIsSynchronousWithStatus(t *testing.T) {
	h := newSlowHeaderHost(10*time.Millisecond, 429)
	svc := heartbeatService(h.call)
	_, err := svc.execute(streamRequest("fast-429"), true)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 429 {
		t.Fatalf("fast 429 must be returned synchronously with status, got %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.emitTimes) != 0 {
		t.Fatalf("no downstream bytes may be emitted before a synchronous error, got %d emits", len(h.emitTimes))
	}
}

// 响应头超过宽限期才到：先开流并发出 response.created（在响应头之前），最终正常回放。
func TestHTTPSlowHeadersStartHeartbeatBeforeHeaders(t *testing.T) {
	h := newSlowHeaderHost(1500*time.Millisecond, 200)
	svc := heartbeatService(h.call)
	start := time.Now()
	if _, err := svc.execute(streamRequest("slow-ok"), true); err != nil {
		t.Fatal(err)
	}
	if returned := time.Since(start); returned < 900*time.Millisecond || returned > 1400*time.Millisecond {
		t.Fatalf("execute should return after the ~1s grace, took %v", returned)
	}
	select {
	case closeErr := <-h.closed:
		if closeErr != nil {
			t.Fatalf("slow but successful turn must close cleanly, got %v", closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream never closed")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.emitTimes) == 0 || !h.emitTimes[0].Before(h.headersAt) {
		t.Fatal("response.created must be emitted before the upstream headers arrived")
	}
	if !strings.Contains(string(h.emitted), "response.created") || !strings.Contains(string(h.emitted), "response.completed") {
		t.Fatalf("expected created ... completed, got %q", h.emitted)
	}
}

// 响应头超过宽限期后才以 401 返回：只能带 error 关闭（已接受的折中），且不回放完成事件。
func TestHTTPSlowHeaderErrorClosesStreamWithError(t *testing.T) {
	h := newSlowHeaderHost(1500*time.Millisecond, 401)
	svc := heartbeatService(h.call)
	if _, err := svc.execute(streamRequest("slow-401"), true); err != nil {
		t.Fatalf("after the grace window the stream is already handed over, got sync error %v", err)
	}
	select {
	case closeErr := <-h.closed:
		if closeErr == nil {
			t.Fatal("late 401 must close the stream with an error so CPA reacts")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream never closed")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if strings.Contains(string(h.emitted), "response.completed") || strings.Contains(string(h.emitted), "response.failed") {
		t.Fatal("credential-level late failure must not replay or convert to response.failed")
	}
}

// WS 同理：握手超过宽限期时先开流并发 response.created。
func TestWSSlowHandshakeStartsHeartbeatFirst(t *testing.T) {
	mock := &mockBPS{respond: func(ctx context.Context, conn *websocket.Conn) {
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.completed", "response": completedResponseWithUsage()})
		_, _, _ = conn.Read(ctx)
	}}
	var handshakeAt atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1500 * time.Millisecond)
		handshakeAt.Store(time.Now().UnixNano())
		mock.handler(w, r)
	}))
	defer server.Close()
	svc := newWSService(t, server)
	svc.cfg.HeartbeatSeconds = intPtr(1)
	h := newSessionHost()
	var firstEmit atomic.Int64
	svc.SetHost(func(method string, payload any, out any) error {
		if method == "host.stream.emit" {
			firstEmit.CompareAndSwap(0, time.Now().UnixNano())
		}
		return h.call(method, payload, out)
	})
	source := jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi"})
	if _, err := svc.executeStreamWS(ExecutorRequest{StreamID: "ws-slow", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok", AccountID: "a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.closed:
	case <-time.After(6 * time.Second):
		t.Fatal("stream never closed")
	}
	if firstEmit.Load() == 0 || firstEmit.Load() >= handshakeAt.Load() {
		t.Fatal("response.created must be emitted before the slow handshake completed")
	}
	if types := eventTypes(h.events(t)); types[len(types)-1] != "response.completed" {
		t.Fatalf("expected completed replay, got %v", types)
	}
}

// ---- 高 2：下游不读（宿主队列满，emit 阻塞）时 shutdown 仍有界 ----

type stalledDownstreamHost struct {
	*blockingStreamHost
	stall      atomic.Bool
	downClosed chan struct{}
	closeOnce  sync.Once
	closes     atomic.Int32
}

func (h *stalledDownstreamHost) call(method string, payload any, out any) error {
	switch method {
	case "host.stream.emit":
		if h.stall.Load() {
			// 模拟宿主队列已满：emit 阻塞，直到 host.stream.close 标记流关闭。
			<-h.downClosed
			return errors.New("stream is not open")
		}
	case "host.stream.close":
		h.closes.Add(1)
		h.closeOnce.Do(func() { close(h.downClosed) })
		return nil
	}
	return h.blockingStreamHost.call(method, payload, out)
}

func TestShutdownIsBoundedWhenDownstreamStalls(t *testing.T) {
	oldWait, oldGrace := shutdownWait, shutdownForceGrace
	shutdownWait, shutdownForceGrace = 3*time.Second, 200*time.Millisecond
	defer func() { shutdownWait, shutdownForceGrace = oldWait, oldGrace }()

	h := &stalledDownstreamHost{blockingStreamHost: newBlockingStreamHost(), downClosed: make(chan struct{})}
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(1)
	svc.SetHost(h.call)
	if _, err := svc.execute(httpStreamRequest("stall"), true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 开场事件已发出；上游读取阻塞
	h.stall.Store(true)
	time.Sleep(1300 * time.Millisecond) // 下一次心跳（~1s）已阻塞在 emit 上
	start := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= shutdownWait {
		t.Fatalf("shutdown waited the full %v: stalled emit was never released", shutdownWait)
	}
	if n := h.closes.Load(); n != 1 {
		t.Fatalf("host.stream.close must be called exactly once (a second close can block on a full queue), got %d", n)
	}
}

// ---- 中 1：附件上传在登记之后，且可被 shutdown 取消 ----

func TestShutdownCancelsInFlightAttachmentUpload(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	svc := NewService()
	svc.SetHost(func(method string, payload any, out any) error {
		switch method {
		case "host.http.operation_open":
			out.(*hostOperationOpenResult).OperationID = "op-up"
		case "host.http.cancel":
			cancelOnce.Do(func() { close(cancelled) })
		case "host.http.do":
			<-cancelled // 上传一直不返回，直到 operation 被取消
			return errors.New("context canceled")
		}
		return nil
	})
	request := imageRequest(map[string]any{"type": "input_image", "image_url": dataURL})
	request.Stream = true
	request.StreamID = "img"
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Handle("executor.execute_stream", jsonBytes(request))
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= shutdownWait {
		t.Fatal("shutdown could not cancel the blocked attachment upload")
	}
	select {
	case err := <-errCh:
		if !isKind(err, "plugin_stopped") {
			t.Fatalf("want plugin_stopped, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("execute_stream never returned")
	}
}

// ---- 中 2：最终输出边界的停止检查 ----

func TestFinishAfterShutdownDoesNotReplay(t *testing.T) {
	h := newSessionHost()
	svc := sessionService(h)
	ctx, cancel := context.WithCancelCause(context.Background())
	ss := svc.newStreamSession("fin", time.Hour, nil)
	ss.bindLifecycle(ctx)
	ss.start()
	cancel(errPluginStopped) // 转换完成前插件停止
	ss.finish(map[string]any{"id": "resp_up", "status": "completed", "output": []any{messageItem("assistant", "late")}})
	h.mu.Lock()
	emitted, closeErr := string(h.emitted), h.closeErr
	h.mu.Unlock()
	if strings.Contains(emitted, "response.completed") || !strings.Contains(emitted, "plugin_stopped") {
		t.Fatalf("finish after shutdown must emit response.failed(plugin_stopped), got %q", emitted)
	}
	if closeErr != nil {
		t.Fatalf("shutdown path should close cleanly, got %v", closeErr)
	}
}

// ---- 复审 4：窗口内到达的非 2xx 响应头 + 慢正文，仍同步返回带状态码的错误 ----

func TestHTTPFastErrorHeadersSlowBodyStaySynchronous(t *testing.T) {
	oldLimit := errorBodyReadLimit
	errorBodyReadLimit = 1500 * time.Millisecond // 比心跳窗口(1s)更长：正文读取会跨过窗口
	defer func() { errorBodyReadLimit = oldLimit }()

	bodyReleased := make(chan struct{})
	var releaseOnce sync.Once
	var emits atomic.Int32
	svc := heartbeatService(func(method string, payload any, out any) error {
		switch method {
		case "host.http.do_stream":
			time.Sleep(100 * time.Millisecond) // 响应头在窗口内到达
			*out.(*upstreamStream) = upstreamStream{StatusCode: 401, StreamID: "err-1"}
		case "host.http.stream_read":
			<-bodyReleased // 错误正文迟迟不来
			*out.(*streamChunk) = streamChunk{Done: true}
		case "host.http.stream_close":
			releaseOnce.Do(func() { close(bodyReleased) })
		case "host.stream.emit":
			emits.Add(1)
		}
		return nil
	})
	start := time.Now()
	_, err := svc.execute(streamRequest("fast-hdr-slow-body"), true)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 401 {
		t.Fatalf("401 headers inside the window must stay a synchronous status error, got %v", err)
	}
	if emits.Load() != 0 {
		t.Fatalf("no downstream bytes may precede the synchronous error, got %d emits", emits.Load())
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("error body read was not bounded (%v)", elapsed)
	}
}

// 计时器与已完成的结果同时就绪时，必须优先领取结果（select 随机性不得把同步错误变成开流）。
func TestAwaitConnectPrefersReadyResult(t *testing.T) {
	for i := 0; i < 2000; i++ {
		connected := make(chan int, 1)
		connected <- 42
		if got := awaitConnect(time.Nanosecond, connected, nil); got == nil || *got != 42 {
			t.Fatalf("iteration %d: ready result lost to the timer", i)
		}
	}
}

// ---- 复审 5：读取部分错误正文期间发生 shutdown / 总超时，中止原因优先于状态码 ----

// partialBodyHost：401 响应头及时到达；第一次读取返回部分正文，第二次读取阻塞直到 stream_close。
func partialBodyHost(released chan struct{}, once *sync.Once) HostCall {
	var reads atomic.Int32
	return func(method string, payload any, out any) error {
		switch method {
		case "host.http.operation_open":
			out.(*hostOperationOpenResult).OperationID = "op-p"
		case "host.http.do_stream":
			*out.(*upstreamStream) = upstreamStream{StatusCode: 401, StreamID: "partial-1"}
		case "host.http.stream_read":
			if reads.Add(1) == 1 {
				*out.(*streamChunk) = streamChunk{Payload: []byte(`{"error":{"message":"exp`)}
				return nil
			}
			<-released
			*out.(*streamChunk) = streamChunk{Done: true}
		case "host.http.stream_close":
			once.Do(func() { close(released) })
		}
		return nil
	}
}

func TestPartialErrorBodyThenShutdownReportsPluginStopped(t *testing.T) {
	oldLimit := errorBodyReadLimit
	errorBodyReadLimit = 5 * time.Second // 正文上限远大于触发 shutdown 的时刻
	defer func() { errorBodyReadLimit = oldLimit }()

	released := make(chan struct{})
	var once sync.Once
	svc := heartbeatService(partialBodyHost(released, &once))
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.execute(streamRequest("partial-shutdown"), true)
		errCh <- err
	}()
	time.Sleep(200 * time.Millisecond) // 部分正文已读，第二次读取阻塞
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !isKind(err, "plugin_stopped") {
			t.Fatalf("shutdown during a partial error body must surface plugin_stopped, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("execute never returned")
	}
}

func TestPartialErrorBodyThenOverallTimeoutReportsTimeout(t *testing.T) {
	oldLimit := errorBodyReadLimit
	errorBodyReadLimit = 5 * time.Second
	defer func() { errorBodyReadLimit = oldLimit }()

	released := make(chan struct{})
	var once sync.Once
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(0) // 同步等待，便于直接观察返回的错误
	svc.cfg.TimeoutSeconds = 1           // 总超时早于错误正文上限
	svc.SetHost(partialBodyHost(released, &once))
	_, err := svc.execute(streamRequest("partial-timeout"), true)
	if !isKind(err, "upstream_timeout") {
		t.Fatalf("overall timeout during a partial error body must surface upstream_timeout, got %v", err)
	}
}

// 仅错误正文自身的读取上限造成的截断可忽略：仍返回带状态码的错误（含已读到的部分）。
func TestPartialErrorBodyTruncatedByBodyLimitKeepsStatus(t *testing.T) {
	oldLimit := errorBodyReadLimit
	errorBodyReadLimit = 300 * time.Millisecond
	defer func() { errorBodyReadLimit = oldLimit }()

	released := make(chan struct{})
	var once sync.Once
	svc := heartbeatService(partialBodyHost(released, &once))
	_, err := svc.execute(streamRequest("partial-truncated"), true)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 401 {
		t.Fatalf("body-limit truncation must keep the 401 status, got %v", err)
	}
}

// ---- 复审 6：aborted() 同步判定，不依赖看守协程调度 ----

// 直接构造不带看守协程的守卫：中止条件已成立但无人记录原因时，aborted() 仍须立即给出。
func TestGuardAbortedResolvesSynchronously(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	cases := []struct {
		name string
		g    *upstreamGuard
		kind string
	}{
		{"shutdown", &upstreamGuard{shutdownC: closed}, "plugin_stopped"},
		{"client", &upstreamGuard{stopC: closed}, "client_disconnected"},
		{"deadline", &upstreamGuard{deadline: time.Now().Add(-time.Second), timeoutReason: fail(504, "upstream_timeout", "t")}, "upstream_timeout"},
		{"shutdown_wins", &upstreamGuard{shutdownC: closed, stopC: closed, deadline: time.Now().Add(-time.Second), timeoutReason: fail(504, "upstream_timeout", "t")}, "plugin_stopped"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.g.aborted(); !isKind(err, tc.kind) {
				t.Fatalf("want %s, got %v", tc.kind, err)
			}
			if err := tc.g.aborted(); !isKind(err, tc.kind) {
				t.Fatal("resolved reason must stay stable")
			}
		})
	}
	if err := (&upstreamGuard{}).aborted(); err != nil {
		t.Fatalf("no condition met must be nil, got %v", err)
	}
}

// 可控时序：守卫不启动看守协程（模拟其尚未被调度）；本代在读取错误正文期间被停止，
// 随后正文上限先截断读取 → 仍须报告 plugin_stopped，而不是 401。
func TestErrorBodyLimitRacingShutdownStillReportsStopped(t *testing.T) {
	oldLimit := errorBodyReadLimit
	errorBodyReadLimit = 50 * time.Millisecond
	defer func() { errorBodyReadLimit = oldLimit }()

	released := make(chan struct{})
	var once sync.Once
	stopped := make(chan struct{})
	var stopOnce sync.Once
	inner := partialBodyHost(released, &once)
	svc := NewService()
	svc.SetHost(func(method string, payload any, out any) error {
		if method == "host.http.stream_read" {
			stopOnce.Do(func() { close(stopped) }) // 响应头之后、正文读取期间本代被停止
		}
		return inner(method, payload, out)
	})
	g := &upstreamGuard{
		s:         svc,
		shutdownC: stopped,
		done:      make(chan struct{}),
		exited:    make(chan struct{}),
		headers:   make(chan struct{}),
	}
	close(g.exited) // 无看守协程：原因只能靠 aborted() 同步判定
	_, err := svc.upstreamStream(ExecutorRequest{}, map[string]any{}, credential{}, g)
	if !isKind(err, "plugin_stopped") {
		t.Fatalf("shutdown during the error-body read must win over the 401 status, got %v", err)
	}
}

// ---- 复审 7：看守协程与同步检查共用同一优先级 ----

// 多个信号预先就绪、看守协程实际运行：记录的原因必须始终是 plugin_stopped。
func TestGuardWatcherUsesSamePriority(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	disconnected := make(chan struct{})
	close(disconnected)
	svc := NewService()
	svc.SetHost(func(string, any, any) error { return nil })
	for i := 0; i < 500; i++ {
		g := svc.newUpstreamGuard("", "", disconnected, stopped, time.Nanosecond, fail(504, "upstream_timeout", "t"))
		<-g.exited // 看守协程已运行并记录原因
		g.mu.Lock()
		reason := g.reason
		g.mu.Unlock()
		if !isKind(reason, "plugin_stopped") {
			t.Fatalf("iteration %d: watcher recorded %v, want plugin_stopped", i, reason)
		}
		g.release()
	}
	// 只有断开 + 超时：断开优先于超时。
	for i := 0; i < 200; i++ {
		g := svc.newUpstreamGuard("", "", disconnected, nil, time.Nanosecond, fail(504, "upstream_timeout", "t"))
		<-g.exited
		g.mu.Lock()
		reason := g.reason
		g.mu.Unlock()
		if !isKind(reason, "client_disconnected") {
			t.Fatalf("iteration %d: watcher recorded %v, want client_disconnected", i, reason)
		}
		g.release()
	}
}
