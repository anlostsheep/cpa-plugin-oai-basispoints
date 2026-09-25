package basispoints

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 高 1：等待响应头阶段也能被超时 / shutdown 取消 ----

// headerBlockingHost：do_stream 一直阻塞（上游不返回响应头），直到 host.http.cancel 取消其 operation。
type headerBlockingHost struct {
	mu          sync.Mutex
	opIDSent    string
	cancelled   chan string
	cancelOnce  sync.Once
	doStreamErr string
}

func newHeaderBlockingHost(errText string) *headerBlockingHost {
	return &headerBlockingHost{cancelled: make(chan string, 1), doStreamErr: errText}
}

func (h *headerBlockingHost) call(method string, payload any, out any) error {
	p, _ := payload.(map[string]any)
	switch method {
	case "host.http.operation_open":
		if p["host_callback_id"] != "cb-1" {
			return errors.New("operation_open without callback id")
		}
		out.(*hostOperationOpenResult).OperationID = "op-42"
	case "host.http.do_stream":
		h.mu.Lock()
		h.opIDSent, _ = p["operation_id"].(string)
		h.mu.Unlock()
		op := <-h.cancelled
		return errors.New(h.doStreamErr + " (" + op + ")")
	case "host.http.cancel":
		op, _ := p["operation_id"].(string)
		h.cancelOnce.Do(func() { h.cancelled <- op })
	}
	return nil
}

func headerPhaseRequest() []byte {
	return jsonBytes(ExecutorRequest{
		Model:          DefaultModelID,
		Payload:        jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi", "stream": true}),
		Stream:         true,
		StreamID:       "hdr",
		HostCallbackID: "cb-1",
		StorageJSON:    jsonBytes(map[string]any{"access_token": "tok-raw-SECRET-9", "account_id": "acct"}),
	})
}

func TestHTTPHeaderWaitIsCancelledOnTimeout(t *testing.T) {
	h := newHeaderBlockingHost("context canceled")
	svc := NewService()
	svc.cfg.TimeoutSeconds = 1
	svc.SetHost(h.call)
	start := time.Now()
	_, err := svc.execute(headerPhaseRequest(), true)
	if !isKind(err, "upstream_timeout") {
		t.Fatalf("header wait should end with upstream_timeout, got %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("timeout did not cancel the blocked do_stream")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.opIDSent != "op-42" {
		t.Fatalf("do_stream must carry the opened operation_id, got %q", h.opIDSent)
	}
}

func TestHTTPHeaderWaitIsCancelledOnShutdown(t *testing.T) {
	h := newHeaderBlockingHost("context canceled")
	svc := NewService()
	svc.SetHost(h.call)
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.execute(headerPhaseRequest(), true)
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond) // do_stream 已阻塞在等待响应头
	start := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= shutdownWait {
		t.Fatal("shutdown did not cancel the header wait")
	}
	select {
	case err := <-errCh:
		if !isKind(err, "plugin_stopped") {
			t.Fatalf("want plugin_stopped, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("execute never returned after shutdown")
	}
}

// ---- 高 3：HTTP 错误路径按当前令牌精确脱敏 ----

func TestHTTPTransportErrorRedactsRawToken(t *testing.T) {
	h := newHeaderBlockingHost("dial failed for token tok-raw-SECRET-9")
	svc := NewService()
	svc.SetHost(func(method string, payload any, out any) error {
		if method == "host.http.do_stream" {
			return errors.New("proxy said: tok-raw-SECRET-9 rejected")
		}
		return h.call(method, payload, out)
	})
	_, err := svc.execute(headerPhaseRequest(), true)
	if err == nil || strings.Contains(err.Error(), "tok-raw-SECRET-9") {
		t.Fatalf("raw token leaked or no error: %v", err)
	}
}

// ---- 高 2：生命周期按代隔离 ----

func TestLifecycleGenerationsAreIsolated(t *testing.T) {
	old := shutdownWait
	shutdownWait = 300 * time.Millisecond
	defer func() { shutdownWait = old }()

	svc := NewService()
	ctx1, done1, err := svc.beginStream()
	if err != nil {
		t.Fatal(err)
	}
	defer done1() // 模拟一个迟迟不退出的旧往返
	svc.shutdown()
	if !stoppedByShutdown(ctx1) {
		t.Fatal("gen-1 turn must observe plugin shutdown as its cancel cause")
	}
	if _, err := svc.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: \"\"\n")})); err != nil {
		t.Fatal(err)
	}
	ctx2, done2, err := svc.beginStream()
	if err != nil {
		t.Fatal(err)
	}
	if ctx2.Err() != nil || stoppedByShutdown(ctx2) {
		t.Fatal("gen-2 must start with a live context")
	}
	// 旧往返在新一代建立后仍能看出自己是被 shutdown 取消的（不会误判为客户端断开）。
	if !stoppedByShutdown(ctx1) {
		t.Fatal("rebuilding the lifecycle must not change gen-1's cancel cause")
	}
	done2()
	// 第二次 shutdown 只等待 gen-2（已无在途往返），不被 gen-1 残留的往返拖住。
	start := time.Now()
	svc.shutdown()
	if elapsed := time.Since(start); elapsed >= shutdownWait {
		t.Fatalf("shutdown of gen-2 waited on gen-1's leftover turn (%v)", elapsed)
	}
}

func TestConfigureWaitsForInFlightShutdown(t *testing.T) {
	old := shutdownWait
	shutdownWait = 300 * time.Millisecond
	defer func() { shutdownWait = old }()

	svc := NewService()
	_, done1, _ := svc.beginStream()
	defer done1()
	var shutdownReturned atomic.Bool
	go func() {
		svc.shutdown() // 会等满 shutdownWait（gen-1 往返未退出）
		shutdownReturned.Store(true)
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := svc.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: \"\"\n")})); err != nil {
		t.Fatal(err)
	}
	if !shutdownReturned.Load() {
		t.Fatal("configure rebuilt the lifecycle while shutdown was still draining")
	}
}

// ---- 中 2：response 自身的状态码 ----

func TestClassifyUsesResponseStatusCode(t *testing.T) {
	err := classifyUpstreamFailure(map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "status_code": float64(429)}}, "response.failed")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 429 || isRequestScoped(err) {
		t.Fatalf("response.status_code=429 must map to a credential-level 429, got %#v", err)
	}
}

// ---- 中 3：镜像中的 null ----

func TestSettingsMirrorNullHandling(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")
	bad := `{"timeout_seconds": null}`
	if err := os.WriteFile(path, []byte(bad), 0600); err != nil {
		t.Fatal(err)
	}
	svc := NewService()
	if _, err := svc.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: " + dir + "\n")})); !isKind(err, "invalid_config") {
		t.Fatalf("null scalar in mirror must be invalid_config, got %v", err)
	}
	if raw, _ := os.ReadFile(path); string(raw) != bad {
		t.Fatalf("mirror was overwritten: %q", raw)
	}
	// 插件自己写出的 null（nil 指针 / nil 切片）必须可被重新读取。
	ok := `{"heartbeat_seconds": null, "dedicated_auth_files": null, "model_mappings": null}`
	if err := os.WriteFile(path, []byte(ok), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewService().Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: " + dir + "\n")})); err != nil {
		t.Fatalf("nullable fields must be accepted: %v", err)
	}
}
