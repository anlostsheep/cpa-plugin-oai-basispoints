package basispoints

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sessionHost 记录 emit/close，可配置 emit 失败以模拟客户端断开。
type sessionHost struct {
	mu        sync.Mutex
	emitted   []byte
	emits     int
	closeErr  any
	closes    int
	failEmits bool
	closed    chan struct{}
}

func newSessionHost() *sessionHost { return &sessionHost{closed: make(chan struct{}, 1)} }

func (h *sessionHost) call(method string, payload any, out any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch method {
	case "host.stream.emit":
		if h.failEmits {
			return errors.New("client gone")
		}
		h.emits++
		h.emitted = append(h.emitted, payload.(map[string]any)["payload"].([]byte)...)
	case "host.stream.close":
		h.closes++
		h.closeErr = payload.(map[string]any)["error"]
		select {
		case h.closed <- struct{}{}:
		default:
		}
	}
	return nil
}

func (h *sessionHost) events(t *testing.T) []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return clientStreamEvents(t, append([]byte(nil), h.emitted...))
}

// partialEvents 解析未以 [DONE] 结束的流（带 error 关闭的路径），仍校验序号连续。
func (h *sessionHost) partialEvents(t *testing.T) []map[string]any {
	t.Helper()
	h.mu.Lock()
	raw := string(h.emitted)
	h.mu.Unlock()
	var events []map[string]any
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event["sequence_number"] != float64(len(events)) {
			t.Fatalf("out of order sequence: %#v", event)
		}
		events = append(events, event)
	}
	return events
}

func eventTypes(events []map[string]any) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, stringValue(e["type"]))
	}
	return out
}

func sessionService(h *sessionHost) *Service {
	svc := NewService()
	svc.SetHost(h.call)
	return svc
}

// A：心跳周期性发出 response.in_progress；最终回放与开场事件序号连续、id 一致、只有一次 created。
func TestStreamSessionHeartbeatAndAlignedFinish(t *testing.T) {
	h := newSessionHost()
	svc := sessionService(h)
	ss := svc.newStreamSession("s", 40*time.Millisecond, nil)
	ss.start()
	time.Sleep(150 * time.Millisecond) // 至少 2~3 次心跳
	ss.finish(map[string]any{"id": "resp_upstream", "status": "completed", "output": []any{messageItem("assistant", "hi")}})

	events := h.events(t) // clientStreamEvents 已校验 sequence_number 连续
	types := eventTypes(events)
	if types[0] != "response.created" || types[1] != "response.in_progress" {
		t.Fatalf("prologue = %v", types[:2])
	}
	heartbeats := 0
	created := 0
	for _, typ := range types {
		switch typ {
		case "response.in_progress":
			heartbeats++
		case "response.created":
			created++
		}
	}
	if created != 1 {
		t.Fatalf("response.created emitted %d times, want exactly 1", created)
	}
	if heartbeats < 3 { // 开场 1 次 + 至少 2 次心跳
		t.Fatalf("heartbeats = %d, want >= 3 (types %v)", heartbeats, types)
	}
	last := events[len(events)-1]
	if last["type"] != "response.completed" {
		t.Fatalf("last event = %v, want response.completed", last["type"])
	}
	firstID := objectValue(events[0]["response"])["id"]
	lastID := objectValue(last["response"])["id"]
	if firstID == nil || firstID != lastID {
		t.Fatalf("response id not aligned: created=%v completed=%v", firstID, lastID)
	}
	if h.closeErr != nil || h.closes != 1 {
		t.Fatalf("close err=%v closes=%d", h.closeErr, h.closes)
	}
}

// 心跳 emit 失败 → 视为客户端断开 → 回调 onDisconnect（用于取消上游 / 发送 WS cancel 帧）。
func TestStreamSessionDisconnectTriggersCancel(t *testing.T) {
	h := newSessionHost()
	svc := sessionService(h)
	var cancelled atomic.Bool
	done := make(chan struct{})
	ss := svc.newStreamSession("s", 30*time.Millisecond, func() {
		if cancelled.CompareAndSwap(false, true) {
			close(done)
		}
	})
	ss.start()
	h.mu.Lock()
	h.failEmits = true
	h.mu.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("onDisconnect was not invoked after emit failure")
	}
	// 断开后 fail 不再 emit，且静默关闭（不让 CPA 冷却凭据）。
	ss.fail(fail(499, "client_disconnected", "gone"))
	if h.closeErr != nil {
		t.Fatalf("disconnect should close cleanly, got %v", h.closeErr)
	}
}

// #6：请求层面错误 → response.failed + 正常关闭；凭据/限流/传输错误 → 带 error 关闭，不发 failed。
func TestStreamSessionFailureClassification(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantFailed bool
	}{
		{"invalid_tool_code", fail(422, "invalid_tool_code", "bad code"), true},
		{"invalid_tool_call", fail(502, "invalid_tool_call", "bad call"), true},
		{"upstream_incomplete", fail(502, "upstream_incomplete", "failed"), true},
		{"server_draining", fail(503, "server_draining", "draining"), true},
		{"auth_401", fail(401, "upstream_error", "unauthorized"), false},
		{"forbidden_403", fail(403, "upstream_error", "forbidden"), false},
		{"rate_limit_429", fail(429, "upstream_error", "slow down"), false},
		{"transport", fail(502, "upstream_transport", "dial failed"), false},
		{"timeout", fail(504, "upstream_timeout", "timed out"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSessionHost()
			svc := sessionService(h)
			ss := svc.newStreamSession("s", time.Hour, nil) // 心跳开启但测试期内不触发
			ss.start()
			ss.fail(tc.err)
			var types []string
			if tc.wantFailed {
				types = eventTypes(h.events(t)) // 正常关闭：必须以 [DONE] 结束
			} else {
				types = eventTypes(h.partialEvents(t)) // 带 error 关闭：无 [DONE]
			}
			hasFailed := strings.Contains(strings.Join(types, ","), "response.failed")
			if hasFailed != tc.wantFailed {
				t.Fatalf("response.failed emitted=%v, want %v (events %v)", hasFailed, tc.wantFailed, types)
			}
			if tc.wantFailed && h.closeErr != nil {
				t.Fatalf("request-scoped error must close cleanly, got %v", h.closeErr)
			}
			if !tc.wantFailed && h.closeErr == nil {
				t.Fatal("credential/transport error must close with error so CPA can cool down / rotate")
			}
			for _, typ := range types {
				if strings.HasPrefix(typ, "response.output_item") || typ == "response.completed" {
					t.Fatalf("failure path replayed output: %v", types)
				}
			}
		})
	}
}

// heartbeat_seconds=0：不提前输出，保持 v0.1.10 的一次性回放（含完整开场事件）。
func TestStreamSessionHeartbeatDisabledKeepsLegacyReplay(t *testing.T) {
	h := newSessionHost()
	svc := sessionService(h)
	ss := svc.newStreamSession("s", 0, nil)
	ss.start()
	if h.emits != 0 {
		t.Fatalf("heartbeat disabled but start emitted %d times", h.emits)
	}
	ss.finish(map[string]any{"id": "resp_upstream", "status": "completed", "output": []any{}})
	types := eventTypes(h.events(t))
	if h.emits != 1 || types[0] != "response.created" || types[len(types)-1] != "response.completed" {
		t.Fatalf("legacy replay changed: emits=%d types=%v", h.emits, types)
	}
	if id := objectValue(h.events(t)[0]["response"])["id"]; id != "resp_upstream" {
		t.Fatalf("legacy replay should keep upstream id, got %v", id)
	}
}

func TestHeartbeatSecondsValidation(t *testing.T) {
	for _, bad := range []int{-1, maxHeartbeatSeconds + 1} {
		cfg := defaultConfig()
		cfg.HeartbeatSeconds = intPtr(bad)
		if err := cfg.normalize(); err == nil {
			t.Fatalf("heartbeat_seconds=%d should be rejected", bad)
		}
	}
	cfg := defaultConfig()
	cfg.HeartbeatSeconds = nil
	if err := cfg.normalize(); err != nil || cfg.heartbeatInterval() != DefaultHeartbeatSeconds*time.Second {
		t.Fatalf("nil heartbeat should default to %ds: %v %v", DefaultHeartbeatSeconds, cfg.heartbeatInterval(), err)
	}
	zero := defaultConfig()
	zero.HeartbeatSeconds = intPtr(0)
	if err := zero.normalize(); err != nil || zero.heartbeatInterval() != 0 {
		t.Fatalf("heartbeat 0 should disable: %v %v", zero.heartbeatInterval(), err)
	}
}
