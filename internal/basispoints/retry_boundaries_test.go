package basispoints

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRoundTripTimeoutSharesDeadline(t *testing.T) {
	cfg := defaultConfig()
	full := time.Duration(cfg.TimeoutSeconds) * time.Second
	if got, ok := (ExecutorRequest{}).roundTripTimeout(cfg); !ok || got != full {
		t.Fatalf("no deadline: %v %t", got, ok)
	}
	if got, ok := (ExecutorRequest{deadline: time.Now().Add(2 * time.Second)}).roundTripTimeout(cfg); !ok || got > 2*time.Second || got <= 0 {
		t.Fatalf("remaining budget not used: %v %t", got, ok)
	}
	if _, ok := (ExecutorRequest{deadline: time.Now().Add(-time.Millisecond)}).roundTripTimeout(cfg); ok {
		t.Fatal("expired deadline still allowed a round trip")
	}
}

// 重新生成共享截止时间：剩余时长到期即取消阻塞中的上游请求；已到期则不再发起请求。
func TestGuardedDoHonorsSharedDeadline(t *testing.T) {
	svc := NewService()
	var mu sync.Mutex
	calls := map[string]int{}
	cancelled := make(chan struct{})
	var once sync.Once
	svc.SetHost(func(method string, payload any, out any) error {
		mu.Lock()
		calls[method]++
		mu.Unlock()
		switch method {
		case "host.http.operation_open":
			out.(*hostOperationOpenResult).OperationID = "op-1"
		case "host.http.cancel":
			once.Do(func() { close(cancelled) })
		case "host.http.do":
			select {
			case <-cancelled:
				return fmt.Errorf("operation cancelled")
			case <-time.After(5 * time.Second):
				return fmt.Errorf("deadline was not enforced")
			}
		}
		return nil
	})
	request := ExecutorRequest{HostCallbackID: "cb", deadline: time.Now().Add(150 * time.Millisecond)}
	started := time.Now()
	err := svc.guardedDo(request, "", map[string]any{}, &upstreamResponse{})
	if !isKind(err, "upstream_timeout") || time.Since(started) > 2*time.Second {
		t.Fatalf("shared deadline not enforced: err=%v after %v", err, time.Since(started))
	}

	mu.Lock()
	before := calls["host.http.do"]
	mu.Unlock()
	expired := ExecutorRequest{HostCallbackID: "cb", deadline: time.Now().Add(-time.Second)}
	if err := svc.guardedDo(expired, "", map[string]any{}, &upstreamResponse{}); !isKind(err, "upstream_timeout") {
		t.Fatalf("expired deadline: %v", err)
	}
	mu.Lock()
	after := calls["host.http.do"]
	mu.Unlock()
	if after != before {
		t.Fatal("request sent after the shared deadline expired")
	}
}

// 重新生成时下游流已开启：第二次往返遇到限流/凭据错误，只能以带 error 的流关闭交给 CPA
// （不伪装成 response.failed 或 completed），两次上游流都关闭，插件停止不被拖住。
func TestRetryCredentialFailureAfterStreamOpenHTTP(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	bad, _ := relayFixture(t.Name()+"-bad", true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		attempt := attempts
		mu.Unlock()
		if attempt == 2 {
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
			return
		}
		response := map[string]any{"id": "resp_retry_429", "status": "completed", "output": []any{bad}}
		w.Header().Set("Content-Type", "text/event-stream")
		var events strings.Builder
		writeSSE(&events, "response.completed", map[string]any{"type": "response.completed", "response": response})
		_, _ = io.WriteString(w, events.String())
	}))
	defer server.Close()
	svc := NewService()
	svc.cfg.ResponsesURL = server.URL
	var hostMu sync.Mutex
	bodies := map[string][]byte{}
	var emitted []byte
	closes, next := 0, 0
	done := make(chan map[string]any, 1)
	svc.SetHost(func(method string, payload any, out any) error {
		p := payload.(map[string]any)
		hostMu.Lock()
		defer hostMu.Unlock()
		switch method {
		case "host.http.do_stream":
			req, _ := http.NewRequest(http.MethodPost, p["url"].(string), strings.NewReader(string(p["body"].([]byte))))
			res, err := server.Client().Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			raw, _ := io.ReadAll(res.Body)
			next++
			id := fmt.Sprintf("s-%d", next)
			bodies[id] = raw
			*out.(*upstreamStream) = upstreamStream{StatusCode: res.StatusCode, Headers: res.Header, StreamID: id}
		case "host.http.stream_read":
			*out.(*streamChunk) = streamChunk{Payload: bodies[stringValue(p["stream_id"])], Done: true}
		case "host.http.stream_close":
			closes++
		case "host.stream.emit":
			emitted = append(emitted, p["payload"].([]byte)...)
		case "host.stream.close":
			done <- p
		}
		return nil
	})
	src := map[string]any{"model": DefaultModelID, "input": "Apply patch", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(src), Stream: true, StreamID: "client-429", StorageJSON: jsonBytes(map[string]any{"access_token": "fixture", "account_id": "fixture"})}
	if _, err := svc.Handle("executor.execute_stream", jsonBytes(req)); err != nil {
		t.Fatal(err)
	}
	var closed map[string]any
	select {
	case closed = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not close")
	}
	if closed["error"] == nil {
		t.Fatal("credential-level failure on retry must close the stream with an error")
	}
	hostMu.Lock()
	wire, gotCloses := string(emitted), closes
	hostMu.Unlock()
	if strings.Contains(wire, "response.failed") || strings.Contains(wire, "response.completed") {
		t.Fatal("retry failure was disguised as a terminal response")
	}
	if gotCloses != 2 {
		t.Fatalf("upstream streams closed %d times, want 2", gotCloses)
	}
	// 往返已结束（lifecycle done 已调用）：插件停止无需等待。
	started := time.Now()
	if _, err := svc.Handle("plugin.shutdown", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("finished round trip still held the plugin lifecycle")
	}
}

// ws：第二条连接上游报限流时同样以带 error 的流关闭，不输出终态。
func TestRetryCredentialFailureAfterStreamOpenWS(t *testing.T) {
	bad, _ := relayFixture(t.Name()+"-bad", true)
	var mu sync.Mutex
	conns := 0
	mock := &mockBPS{}
	mock.respond = func(ctx context.Context, conn *websocket.Conn) {
		mu.Lock()
		conns++
		attempt := conns
		mu.Unlock()
		if attempt == 2 {
			_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]any{"code": "rate_limit_exceeded"}}})
		} else {
			_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.completed", "response": map[string]any{"id": "r", "status": "completed", "output": []any{bad}}})
		}
		_, _, _ = conn.Read(ctx)
	}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()
	svc := newWSService(t, server)
	var hostMu sync.Mutex
	var emitted []byte
	done := make(chan map[string]any, 1)
	svc.SetHost(func(method string, payload any, out any) error {
		p := payload.(map[string]any)
		hostMu.Lock()
		defer hostMu.Unlock()
		switch method {
		case "host.stream.emit":
			emitted = append(emitted, p["payload"].([]byte)...)
		case "host.stream.close":
			done <- p
		}
		return nil
	})
	src := map[string]any{"model": DefaultModelID, "input": "Apply patch", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(src), Stream: true, StreamID: "client-ws-429", StorageJSON: jsonBytes(map[string]any{"access_token": "fixture", "account_id": "fixture"})}
	if _, err := svc.Handle("executor.execute_stream", jsonBytes(req)); err != nil {
		t.Fatal(err)
	}
	var closed map[string]any
	select {
	case closed = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not close")
	}
	if closed["error"] == nil {
		t.Fatal("rate limit on ws retry must close the stream with an error")
	}
	hostMu.Lock()
	wire := string(emitted)
	hostMu.Unlock()
	if strings.Contains(wire, "response.failed") || strings.Contains(wire, "response.completed") {
		t.Fatal("ws retry failure was disguised as a terminal response")
	}
	mu.Lock()
	defer mu.Unlock()
	if conns != 2 {
		t.Fatalf("ws connections=%d want 2", conns)
	}
}
