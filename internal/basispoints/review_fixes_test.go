package basispoints

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---- 高 2：令牌不得经握手/写帧错误外泄 ----

func TestRedactTokenMessageCoversAllBearerForms(t *testing.T) {
	in := `sub "openai-bearer.eyJ.aaa.bbb" then Bearer tok1 and again Bearer tok2, bearer tok3;openai-bearer.zzz`
	out := redactTokenMessage(in)
	for _, leaked := range []string{"eyJ.aaa.bbb", "tok1", "tok2", "tok3", "zzz"} {
		if strings.Contains(out, leaked) {
			t.Fatalf("token fragment %q leaked: %q", leaked, out)
		}
	}
	if got := redactSecret("dial tcp: raw-secret-value here", "raw-secret-value"); strings.Contains(got, "raw-secret-value") {
		t.Fatalf("exact secret not redacted: %q", got)
	}
}

// 服务器回显带令牌的意外子协议：依赖库会把它写进错误，插件必须不外泄。
func TestWSHandshakeEchoedSubprotocolDoesNotLeakToken(t *testing.T) {
	const token = "tok-SECRET-echo-123"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(sum[:]) + "\r\n" +
			"Sec-WebSocket-Protocol: openai-bearer." + token + "\r\n\r\n")
		_ = rw.Flush()
		_, _ = bufio.NewReader(conn).ReadByte()
	}))
	defer server.Close()

	svc := newWSService(t, server)
	_, err := svc.streamOverWS(context.Background(), ExecutorRequest{StreamID: "leak"}, wsBody(), credential{AccessToken: token, AccountID: "a"})
	if err == nil {
		t.Fatal("mismatched subprotocol must fail the handshake")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("token leaked through handshake error: %v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Kind != "upstream_transport" {
		t.Fatalf("handshake failure should be upstream_transport, got %#v", err)
	}
}

// ---- 高 1：阻塞中的 HTTP 读取必须能被断开/超时打断 ----

// blockingStreamHost：stream_read 一直阻塞，直到 stream_close 被调用（模拟宿主取消上游）。
type blockingStreamHost struct {
	mu          sync.Mutex
	emitted     []byte
	failEmits   atomic.Bool
	upstreamOff chan struct{}
	closeOnce   sync.Once
	streamClose atomic.Int32
	closed      chan any
}

func newBlockingStreamHost() *blockingStreamHost {
	return &blockingStreamHost{upstreamOff: make(chan struct{}), closed: make(chan any, 1)}
}

func (h *blockingStreamHost) call(method string, payload any, out any) error {
	switch method {
	case "host.http.do_stream":
		*out.(*upstreamStream) = upstreamStream{StatusCode: 200, StreamID: "blocked-upstream"}
	case "host.http.stream_read":
		<-h.upstreamOff
		// 宿主取消后通常以 Done 返回（可能带部分数据）：插件不得把它当作完整响应。
		*out.(*streamChunk) = streamChunk{Payload: []byte("data: {\"type\":\"response.created\"}\n\n"), Done: true}
	case "host.http.stream_close":
		h.streamClose.Add(1)
		h.closeOnce.Do(func() { close(h.upstreamOff) })
	case "host.stream.emit":
		if h.failEmits.Load() {
			return errors.New("client gone")
		}
		h.mu.Lock()
		h.emitted = append(h.emitted, payload.(map[string]any)["payload"].([]byte)...)
		h.mu.Unlock()
	case "host.stream.close":
		h.closed <- payload.(map[string]any)["error"]
	}
	return nil
}

func httpStreamRequest(streamID string) []byte {
	return jsonBytes(ExecutorRequest{
		Model:       DefaultModelID,
		Payload:     jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi", "stream": true}),
		Stream:      true,
		StreamID:    streamID,
		StorageJSON: jsonBytes(map[string]any{"access_token": "test-access", "account_id": "test-account"}),
	})
}

func TestHTTPStreamHeartbeatFailureUnblocksPendingRead(t *testing.T) {
	h := newBlockingStreamHost()
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(1)
	svc.SetHost(h.call)
	if _, err := svc.execute(httpStreamRequest("blocked"), true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // 开场事件已发出，读取已阻塞
	h.failEmits.Store(true)            // 客户端断开：下一次心跳（~1s）emit 失败

	select {
	case closeErr := <-h.closed:
		if closeErr != nil {
			t.Fatalf("client disconnect should close cleanly, got %v", closeErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked upstream read was never interrupted after heartbeat failure")
	}
	if h.streamClose.Load() == 0 {
		t.Fatal("upstream stream was not closed")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if strings.Contains(string(h.emitted), "response.completed") {
		t.Fatal("truncated upstream must not be replayed as completed")
	}
}

func TestHTTPStreamTimeoutInterruptsPendingRead(t *testing.T) {
	h := newBlockingStreamHost()
	svc := NewService()
	svc.cfg.TimeoutSeconds = 1 // 绕过 normalize 下限，仅用于测试
	svc.SetHost(h.call)
	start := time.Now()
	_, err := svc.readUpstreamStreamUntil(upstreamStream{StreamID: "blocked-upstream"}, nil, nil)
	if !isKind(err, "upstream_timeout") {
		t.Fatalf("want upstream_timeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("timeout did not interrupt blocked read (took %v)", elapsed)
	}
}

// ---- 中 3：流内失败帧按状态/错误码分流 ----

func TestClassifyUpstreamFailure(t *testing.T) {
	cases := []struct {
		name   string
		frame  map[string]any
		status int
		scoped bool
	}{
		{"model_failure", map[string]any{"type": "response.failed", "response": map[string]any{"error": map[string]any{"code": "server_error"}}}, 502, true},
		{"incomplete", map[string]any{"type": "response.incomplete", "response": map[string]any{"incomplete_details": map[string]any{"reason": "max_output_tokens"}}}, 502, true},
		{"rate_limited_code", map[string]any{"type": "response.failed", "response": map[string]any{"error": map[string]any{"code": "rate_limit_exceeded"}}}, 429, false},
		{"usage_limit", map[string]any{"type": "error", "error": map[string]any{"type": "usage_limit_reached"}}, 429, false},
		{"status_401", map[string]any{"type": "error", "status": float64(401), "error": map[string]any{"message": "expired"}}, 401, false},
		{"token_invalidated", map[string]any{"type": "error", "error": map[string]any{"code": "token_invalidated"}}, 401, false},
		{"deactivated", map[string]any{"type": "response.failed", "response": map[string]any{"error": map[string]any{"code": "account_deactivated"}}}, 403, false},
		{"bad_request", map[string]any{"type": "error", "status": float64(400), "error": map[string]any{"code": "invalid_request_error"}}, 400, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyUpstreamFailure(tc.frame, stringValue(tc.frame["type"]))
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Status != tc.status {
				t.Fatalf("status = %#v, want %d", err, tc.status)
			}
			if isRequestScoped(err) != tc.scoped {
				t.Fatalf("request-scoped = %v, want %v", !tc.scoped, tc.scoped)
			}
		})
	}
	// 上游任意文本不得进入错误串。
	err := classifyUpstreamFailure(map[string]any{"type": "error", "error": map[string]any{"code": "Bearer abc secret text"}}, "error")
	if strings.Contains(err.Error(), "abc") {
		t.Fatalf("unsafe upstream code leaked: %v", err)
	}
}

func TestWSInStreamRateLimitClosesWithError(t *testing.T) {
	mock := &mockBPS{respond: func(ctx context.Context, conn *websocket.Conn) {
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.created", "response": map[string]any{"status": "in_progress"}})
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]any{"code": "rate_limit_exceeded"}}})
		_, _, _ = conn.Read(ctx)
	}}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()
	svc := newWSService(t, server)
	h := newSessionHost()
	svc.SetHost(h.call)
	source := jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi"})
	if _, err := svc.executeStreamWS(ExecutorRequest{StreamID: "rl", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok", AccountID: "a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("stream never closed")
	}
	h.mu.Lock()
	closeErr, emitted := h.closeErr, string(h.emitted)
	h.mu.Unlock()
	if closeErr == nil {
		t.Fatal("in-stream rate limit must close with error so CPA cools down / rotates")
	}
	if strings.Contains(emitted, "response.failed") {
		t.Fatal("credential-level failure must not be converted into response.failed")
	}
}

func TestHTTPSSEFailureEventIsClassified(t *testing.T) {
	raw := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"rate_limit_exceeded\"}}}\n\n")
	_, err := parseFinalStreamResponse(raw)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 429 {
		t.Fatalf("SSE rate limit should map to 429, got %v", err)
	}
}

// ---- 中 4：文件名精确匹配，任意位置空白都非法 ----

func TestDedicatedMatchUsesRawFileName(t *testing.T) {
	cfg := defaultConfig()
	cfg.DedicatedAuthFiles = []string{"excel-only.json"}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	padded, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: " excel-only.json ", RawJSON: codexStorage()}), cfg.dedicatedSet())
	if err != nil {
		t.Fatal(err)
	}
	if got := padded["Auths"].([]any); len(got) != 2 {
		t.Fatalf("padded file name must not match dedicated entry; Auths=%d", len(got))
	}
	byPath, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, Path: "/root/.cli-proxy-api/excel-only.json", RawJSON: codexStorage()}), cfg.dedicatedSet())
	if err != nil {
		t.Fatal(err)
	}
	if got := byPath["Auths"].([]any); len(got) != 1 {
		t.Fatalf("path basename should match dedicated entry; Auths=%d", len(got))
	}
	for _, bad := range []string{"excel only.json", "excel\t.json", "excel .json", "excel .json", "excel\x01.json"} {
		c := defaultConfig()
		c.DedicatedAuthFiles = []string{bad}
		if err := c.normalize(); err == nil {
			t.Fatalf("entry %q with whitespace/control must be rejected", bad)
		}
	}
}

// ---- 中 5：损坏的镜像是配置错误，不得被当作空列表覆盖 ----

func TestCorruptSettingsMirrorFailsClosed(t *testing.T) {
	cases := map[string]string{
		"not_json":   "{broken",
		"not_object": "[1,2]",
		"bad_type":   `{"dedicated_auth_files": "excel.json"}`,
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "settings.json")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			svc := NewService()
			_, err := svc.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: " + dir + "\n")}))
			if !isKind(err, "invalid_config") {
				t.Fatalf("corrupt mirror must be invalid_config, got %v", err)
			}
			if raw, _ := os.ReadFile(path); string(raw) != content {
				t.Fatalf("corrupt mirror was overwritten: %q", raw)
			}
		})
	}
	// YAML 已提供该键时，镜像中同名键的类型错误不影响（YAML 权威，不解码镜像值）。
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"dedicated_auth_files": "excel.json"}`), 0600); err != nil {
		t.Fatal(err)
	}
	svc := NewService()
	if _, err := svc.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: " + dir + "\ndedicated_auth_files:\n  - excel.json\n")})); err != nil {
		t.Fatalf("YAML-provided key should win over a bad mirror value: %v", err)
	}
}

// ---- 中 6：plugin.shutdown 取消进行中的 WS 往返并等待其退出 ----

func TestShutdownCancelsInFlightWSTurn(t *testing.T) {
	cancelFrame := make(chan map[string]any, 1)
	firstFrameSeen := make(chan struct{})
	mock := &mockBPS{respond: func(ctx context.Context, conn *websocket.Conn) {
		close(firstFrameSeen)
		_, data, err := conn.Read(ctx)
		if err != nil {
			cancelFrame <- nil
			return
		}
		frame, _ := rawObject(data)
		cancelFrame <- frame
	}}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()
	svc := newWSService(t, server)
	h := newSessionHost()
	svc.SetHost(h.call)
	source := jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi"})
	if _, err := svc.executeStreamWS(ExecutorRequest{StreamID: "sd", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok", AccountID: "a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstFrameSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("response.create never reached the server")
	}
	start := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= shutdownWait {
		t.Fatalf("shutdown did not cancel the in-flight turn (waited %v)", elapsed)
	}
	// shutdown 返回前，往返已结束并完成最后一次宿主回调。
	h.mu.Lock()
	closes, closeErr, emitted := h.closes, h.closeErr, string(h.emitted)
	h.mu.Unlock()
	if closes != 1 {
		t.Fatalf("stream should be closed exactly once before shutdown returns, got %d", closes)
	}
	if closeErr != nil || !strings.Contains(emitted, "plugin_stopped") {
		t.Fatalf("shutdown should end the turn with response.failed(plugin_stopped) and a clean close; err=%v", closeErr)
	}
	select {
	case frame := <-cancelFrame:
		if stringValue(frame["type"]) != "basispoints.response.cancel" {
			t.Fatalf("expected cancel frame on shutdown, got %#v", frame)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel frame not sent on shutdown")
	}
	if _, err := svc.executeStreamWS(ExecutorRequest{StreamID: "after", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok"}); !isKind(err, "plugin_stopped") {
		t.Fatalf("new streams after shutdown must be rejected, got %v", err)
	}
}

// testGuard 为直接调用 upstreamStream 的旧测试提供一个不计时、无停止信号的守卫。
func testGuard(t *testing.T, s *Service) *upstreamGuard {
	t.Helper()
	g := s.newUpstreamGuard("", "", nil, nil, 0, nil)
	// 只停止看守协程，不发宿主回调（旧测试的假宿主不认识 stream_close）。
	t.Cleanup(func() { g.released.Do(func() { close(g.done); <-g.exited }) })
	return g
}
