package basispoints

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// mockBPS 是一个内存 WebSocket 服务器，按 HAR 帧序回放，用于验证 WS 传输。
type mockBPS struct {
	mu         sync.Mutex
	conns      int
	firstFrame map[string]any
	handshakes []*http.Request
	cancelSeen map[string]any
	respond    func(ctx context.Context, conn *websocket.Conn)
}

func (m *mockBPS) handler(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{"responses"},
		CompressionMode: websocket.CompressionContextTakeover,
		// 插件按真实加载项发送 Origin: https://bps.openai.com；mock 运行在 127.0.0.1，
		// 需只在测试端放开同源校验，生产代码继续发送该 Origin。
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(1 << 20)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	m.mu.Lock()
	m.conns++
	m.handshakes = append(m.handshakes, r)
	m.mu.Unlock()

	_, data, err := conn.Read(ctx)
	if err != nil {
		return
	}
	var first map[string]any
	_ = json.Unmarshal(data, &first)
	m.mu.Lock()
	m.firstFrame = first
	m.mu.Unlock()

	if m.respond != nil {
		m.respond(ctx, conn)
	}
}

func writeWSFrame(ctx context.Context, conn *websocket.Conn, frame map[string]any) error {
	raw, _ := json.Marshal(frame)
	return conn.Write(ctx, websocket.MessageText, raw)
}

func completedResponseWithUsage() map[string]any {
	return map[string]any{
		"status": "completed",
		"output": []any{messageItem("assistant", "hello from ws")},
		"usage": map[string]any{
			"input_tokens":          10,
			"output_tokens":         5,
			"input_tokens_details":  map[string]any{"cached_tokens": 3},
			"cache_write_tokens":    2,
			"output_tokens_details": map[string]any{"reasoning_tokens": 4},
		},
	}
}

func newWSService(t *testing.T, server *httptest.Server) *Service {
	t.Helper()
	cfg := defaultConfig()
	cfg.ResponsesURL = server.URL + "/basispoints/api/responses"
	cfg.Transport = TransportWS
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	svc := NewService()
	svc.cfg = cfg
	return svc
}

func wsBody() map[string]any {
	// 模拟 prepareResponsesBody 产物：input 为全量历史。
	return map[string]any{
		"model": "gpt-6-astra",
		"input": []any{
			messageItem("user", "turn one"),
			messageItem("assistant", "reply one"),
			messageItem("user", "turn two"),
		},
	}
}

// P2c (a)(b)(c)：首帧 response.create 含三个新字段与全量 input；一次调用一条连接、
// 读到 completed 即返回；usage 三类 token 透传。
func TestWSStreamCreateFrameAndUsagePassthrough(t *testing.T) {
	mock := &mockBPS{}
	mock.respond = func(ctx context.Context, conn *websocket.Conn) {
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "basispoints.response.resume_token", "resume_token": "resume_srv"})
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "basispoints.response.metadata"})
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "basispoints.response.upstream_sent"})
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.created", "response": map[string]any{"status": "in_progress"}})
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.in_progress", "response": map[string]any{"status": "in_progress"}})
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.completed", "response": completedResponseWithUsage()})
		// 等待客户端读完并关闭，避免过早断开。
		_, _, _ = conn.Read(ctx)
	}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()

	svc := newWSService(t, server)
	body := wsBody()
	completed, err := svc.streamOverWS(context.Background(), ExecutorRequest{StreamID: "s1"}, body, credential{AccessToken: "bearer-secret", AccountID: "a", AuthMode: "chatgpt"})
	if err != nil {
		t.Fatalf("streamOverWS error: %v", err)
	}

	// (a) 首帧 response.create + 三个新字段 + 全量 input。
	first := mock.firstFrame
	if stringValue(first["type"]) != "response.create" {
		t.Fatalf("first frame type = %v, want response.create", first["type"])
	}
	if stringValue(first["basispoints_request_id"]) == "" {
		t.Fatal("missing basispoints_request_id")
	}
	if !strings.HasPrefix(stringValue(first["basispoints_resume_token"]), "resume_") {
		t.Fatalf("resume token = %v", first["basispoints_resume_token"])
	}
	if input, ok := first["input"].([]any); !ok || len(input) != 3 {
		t.Fatalf("first frame input not full history: %#v", first["input"])
	}

	// (b) 一次调用一条连接。
	if mock.conns != 1 {
		t.Fatalf("connections = %d, want 1", mock.conns)
	}

	// (c) usage 三类 token 透传到 completed，并经 transformResponseBody 保留。
	usage := objectValue(completed["usage"])
	if numberValue(objectValue(usage["input_tokens_details"])["cached_tokens"]) != 3 ||
		numberValue(usage["cache_write_tokens"]) != 2 ||
		numberValue(objectValue(usage["output_tokens_details"])["reasoning_tokens"]) != 4 {
		t.Fatalf("usage tokens not passed through: %#v", usage)
	}
	source := map[string]any{"input": "turn two"}
	_, transformed, _, terr := transformResponseBody(jsonBytes(completed), source)
	if terr != nil {
		t.Fatal(terr)
	}
	if objectValue(transformed["usage"]) == nil {
		t.Fatal("transform dropped usage")
	}

	// 握手：查询参数与子协议符合 HAR；bearer 只在 Sec-WebSocket-Protocol 出现。
	r := mock.handshakes[0]
	q := r.URL.Query()
	if q.Get("bps_auth_mode") != "chatgpt" || !strings.HasPrefix(q.Get("bps_ws_affinity"), "affinity_") || q.Get("bps_control_frames") == "" {
		t.Fatalf("handshake query missing HAR params: %v", r.URL.RawQuery)
	}
	if !strings.Contains(q.Get("bps_client_info"), "X-Openai-Internal-Basispoints-Client-Runtime") {
		t.Fatal("bps_client_info not packed into query")
	}
	if strings.Contains(r.URL.RawQuery, "bearer-secret") {
		t.Fatal("bearer leaked into query")
	}
	if !strings.Contains(r.Header.Get("Sec-WebSocket-Protocol"), "openai-bearer.bearer-secret") {
		t.Fatalf("bearer subprotocol missing: %q", r.Header.Get("Sec-WebSocket-Protocol"))
	}
}

// P2c (d)：取消时发送 cancel 帧。
func TestWSStreamSendsCancelFrameOnCancel(t *testing.T) {
	received := make(chan map[string]any, 1)
	firstFrameSeen := make(chan struct{})
	mock := &mockBPS{}
	mock.respond = func(ctx context.Context, conn *websocket.Conn) {
		// respond 在服务器读到首帧 response.create 之后才被调用。
		close(firstFrameSeen)
		// 不发送任何响应帧；等待客户端在取消后发来 cancel 帧。
		_, data, err := conn.Read(ctx)
		if err != nil {
			received <- nil
			return
		}
		var frame map[string]any
		_ = json.Unmarshal(data, &frame)
		received <- frame
	}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()

	svc := newWSService(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.streamOverWS(ctx, ExecutorRequest{StreamID: "s2"}, wsBody(), credential{AccessToken: "tok", AccountID: "a"})
		errCh <- err
	}()
	// 等服务器确认收到首帧再取消，不依赖固定睡眠。
	select {
	case <-firstFrameSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("response.create never reached the server")
	}
	cancel()

	select {
	case frame := <-received:
		if stringValue(frame["type"]) != "basispoints.response.cancel" || stringValue(frame["request_id"]) == "" || stringValue(frame["resume_token"]) == "" {
			t.Fatalf("cancel frame malformed: %#v", frame)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel frame not received")
	}
	if err := <-errCh; err == nil {
		t.Fatal("cancelled stream should return an error")
	}
}

// P2c (e)：server_draining / 断线 / failed / 缺少响应对象的终态帧报错，且不产生已输出内容的重放。
// v0.1.14 起带完整响应对象的 response.incomplete 是合法终态（见 TestWSIncompleteIsTerminal）。
func TestWSStreamErrorsWithoutReplay(t *testing.T) {
	cases := []struct {
		name    string
		respond func(ctx context.Context, conn *websocket.Conn)
		kind    string
	}{
		{"server_draining", func(ctx context.Context, conn *websocket.Conn) {
			_ = writeWSFrame(ctx, conn, map[string]any{"type": "basispoints.response.server_draining"})
			_, _, _ = conn.Read(ctx)
		}, "server_draining"},
		{"disconnect", func(ctx context.Context, conn *websocket.Conn) {
			// 不发 completed 直接关闭连接。
			conn.Close(websocket.StatusNormalClosure, "")
		}, "upstream_transport"},
		{"failed", func(ctx context.Context, conn *websocket.Conn) {
			_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.failed"})
			_, _, _ = conn.Read(ctx)
		}, "upstream_incomplete"},
		{"incomplete_without_response", func(ctx context.Context, conn *websocket.Conn) {
			_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.incomplete"})
			_, _, _ = conn.Read(ctx)
		}, "invalid_upstream_response"},
		{"cancelled", func(ctx context.Context, conn *websocket.Conn) {
			_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.cancelled"})
			_, _, _ = conn.Read(ctx)
		}, "upstream_incomplete"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockBPS{respond: tc.respond}
			server := httptest.NewServer(http.HandlerFunc(mock.handler))
			defer server.Close()
			svc := newWSService(t, server)
			completed, err := svc.streamOverWS(context.Background(), ExecutorRequest{StreamID: "s3"}, wsBody(), credential{AccessToken: "tok", AccountID: "a"})
			if completed != nil {
				t.Fatalf("no response should be produced on %s", tc.name)
			}
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Kind != tc.kind {
				t.Fatalf("%s: kind = %v, want %s (err=%v)", tc.name, apiErrKind(err), tc.kind, err)
			}
			// 这些均为上游/传输来源，不得误判为 4xx 客户端错误（客户端断开除外）。
			if apiErr.Status >= 400 && apiErr.Status < 500 {
				t.Fatalf("%s classified as client error %d", tc.name, apiErr.Status)
			}
		})
	}
}

func apiErrKind(err error) string {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Kind
	}
	return "<not APIError>"
}

// P2c + #6：上游半途失败（response.failed）属于请求层面的问题：会话只发出开场事件与
// response.failed，绝不 emit 任何输出内容（不重放），并「不带 error」关闭流，避免 CPA
// 因无状态码的流错误把凭据当成瞬时故障冷却。
func TestExecuteStreamWSRequestScopedFailureDoesNotReplayOrCooldown(t *testing.T) {
	mock := &mockBPS{respond: func(ctx context.Context, conn *websocket.Conn) {
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.failed"})
		_, _, _ = conn.Read(ctx)
	}}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()

	svc := newWSService(t, server)
	var emitted []byte
	var closeCalls int
	var closeErr any
	closed := make(chan struct{}, 1)
	svc.SetHost(func(method string, payload any, out any) error {
		switch method {
		case "host.stream.emit":
			emitted = append(emitted, payload.(map[string]any)["payload"].([]byte)...)
		case "host.stream.close":
			closeCalls++
			closeErr = payload.(map[string]any)["error"]
			closed <- struct{}{}
		}
		return nil
	})
	source := jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi"})
	if _, err := svc.executeStreamWS(ExecutorRequest{StreamID: "s4", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok", AccountID: "a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("stream was never closed")
	}
	if closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1", closeCalls)
	}
	if closeErr != nil {
		t.Fatalf("request-scoped failure must close cleanly (no cooldown), got error %#v", closeErr)
	}
	var types []string
	for _, event := range clientStreamEvents(t, emitted) {
		types = append(types, stringValue(event["type"]))
	}
	want := []string{"response.created", "response.in_progress", "response.failed"}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v (no output replay)", types, want)
	}
}

// http 传输默认不变：默认配置 transport 为 http。
func TestDefaultTransportIsHTTP(t *testing.T) {
	if defaultConfig().Transport != TransportHTTP {
		t.Fatal("default transport must remain http")
	}
	cfg := defaultConfig()
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	if cfg.Transport != TransportHTTP {
		t.Fatal("normalize changed default transport")
	}
}

// 延迟心跳：建连（握手）在一个心跳间隔内失败时，下游零字节，executeStreamWS 同步返回
// 带状态码的错误（不开流、不 close），CPA 能按 401 正确换号/冷却；错误不含令牌。
func TestExecuteStreamWSConnectFailureEmitsNothing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	svc := newWSService(t, server)
	var emits, closes int
	svc.SetHost(func(method string, payload any, out any) error {
		switch method {
		case "host.stream.emit":
			emits++
		case "host.stream.close":
			closes++
		}
		return nil
	})
	source := jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi"})
	_, err := svc.executeStreamWS(ExecutorRequest{StreamID: "s5", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok-handshake-SECRET", AccountID: "a"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnauthorized {
		t.Fatalf("handshake 401 within the grace window must be returned synchronously with its status, got %v", err)
	}
	if !strings.Contains(err.Error(), "HTTP 401") || strings.Contains(err.Error(), "tok-handshake-SECRET") {
		t.Fatalf("unexpected error text: %q", err.Error())
	}
	if emits != 0 || closes != 0 {
		t.Fatalf("sync connect failure must not touch the downstream stream: emits=%d closes=%d", emits, closes)
	}
}
