package basispoints

// stream_tool_mode: buffered（0.1.17.0，思路移植自上游 #21 / ee7e59e，按 fork 的流会话实现）：
// 本轮有可调用工具时，正文与工具等整轮校验通过后再交付；失败的那次不交付，复用最多一次重新
// 生成。开流、心跳、客户端 response id、共享截止时间与失败分类沿用 fork 行为。

import (
	"strings"
	"testing"
	"time"
)

func startBuffered(t *testing.T, h *incHost, heartbeat int, src map[string]any, tweak ...func(*Service)) *Service {
	t.Helper()
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(heartbeat)
	svc.cfg.StreamToolMode = StreamToolModeBuffered
	for _, f := range tweak {
		f(svc)
	}
	svc.SetHost(h.call)
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(src), Stream: true, StreamID: "client-" + t.Name(), StorageJSON: jsonBytes(map[string]any{"access_token": "test-access", "account_id": "test-account"})}
	if _, err := svc.Handle("executor.execute_stream", jsonBytes(req)); err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestBufferedBadThenGoodDeliversOnlySecondAttempt(t *testing.T) {
	bad, _ := relayFixture("call_buf_bad", true)
	good, _ := relayFixture("call_buf_good", false)
	first := newBPStream("resp_buf_1")
	first.reasoning("第一次摘要，不应交付。", 3)
	first.message("第一次的说明，不应交付。", 6)
	first.tool(bad, 3)
	second := newBPStream("resp_buf_2")
	second.reasoning("第二次摘要。", 3)
	second.message("第二次的说明。", 4)
	second.tool(good, 3)
	h := newIncHost(chunks(first.terminal(), 3), chunks(second.terminal(), 3))
	startBuffered(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.nextID != 2 {
		t.Fatalf("attempts=%d want 2", h.nextID)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if text := streamedText(events); text != "第二次的说明。" {
		t.Fatalf("only the validated attempt may be delivered, got %q", text)
	}
	if countType(events, "response.created") != 1 || objectValue(events[0]["response"])["id"] != "resp_buf_1" {
		t.Fatalf("client id must stay frozen at the first upstream id: %v", events[0])
	}
	if final := terminalEvent(t, events); final["id"] != "resp_buf_1" {
		t.Fatalf("terminal id %v", final["id"])
	}
	if strings.Contains(h.snapshot(), "第一次") {
		t.Fatal("failed attempt leaked to the client")
	}
	// buffered 的整轮回放不产生 reasoning_summary_* 增量，摘要只出现在 added/done 的完整 item 里。
	if !strings.Contains(h.snapshot(), "第二次摘要。") || strings.Contains(h.snapshot(), "第一次摘要") {
		t.Fatal("buffered replay must carry only the validated attempt's summary")
	}
	rec := h.waitSummary(t)
	if rec.Delivery != "buffered" || rec.ConfigMode != StreamToolModeBuffered || rec.TextCommitted || rec.SummaryCommitted || rec.Regen != regenSuccess || rec.Attempts != 2 || rec.Exit != "completed" {
		t.Fatalf("summary wrong: %+v", rec)
	}
	if logs := strings.Join(h.logMessages(), "\n"); strings.Contains(logs, "replayed at terminal without incremental delivery") {
		t.Fatalf("active buffering must not be logged as a fallback: %s", logs)
	}
}

func TestBufferedBadTwiceFailsWithoutDeliveringText(t *testing.T) {
	bad1, _ := relayFixture("call_buf_x1", true)
	bad2, _ := relayFixture("call_buf_x2", true)
	first := newBPStream("resp_buf_x1")
	first.message("不应交付一。", 3)
	first.tool(bad1, 2)
	second := newBPStream("resp_buf_x2")
	second.message("不应交付二。", 3)
	second.tool(bad2, 2)
	h := newIncHost(chunks(first.terminal(), 2), chunks(second.terminal(), 2))
	startBuffered(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("request-scoped failure must close normally: %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if streamedText(events) != "" || countType(events, "response.failed") != 1 || countType(events, "response.completed") != 0 {
		t.Fatalf("want response.failed only, got %v", eventTypes(events))
	}
	for _, e := range events {
		if e["type"] == "response.failed" {
			if code := objectValue(objectValue(e["response"])["error"])["code"]; code != "invalid_tool_call" {
				t.Fatalf("failed code %v", code)
			}
		}
	}
	rec := h.waitSummary(t)
	if rec.Regen != regenExhausted || rec.Exit != "failed" || rec.TextCommitted {
		t.Fatalf("summary wrong: %+v", rec)
	}
}

// 暂不交付期间 message 事件与终态不一致：直接失败，不当作工具格式错误重新生成。
func TestBufferedFinalMismatchFailsWithoutRegeneration(t *testing.T) {
	good, _ := relayFixture("call_buf_mm", false)
	s := newBPStream("resp_buf_mm")
	s.message("事件里的正文", 4)
	s.tool(good, 2)
	output := append([]any{}, s.output...)
	msg := cloneObject(objectValue(output[0]))
	msg["content"] = []any{map[string]any{"type": "output_text", "text": "终态里换了正文", "annotations": []any{}}}
	output[0] = msg
	h := newIncHost(chunks(s.terminal(output...), 3))
	startBuffered(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.nextID != 1 {
		t.Fatalf("inconsistent stream must not be regenerated, attempts=%d", h.nextID)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if streamedText(events) != "" || countType(events, "response.failed") != 1 {
		t.Fatalf("want response.failed without text, got %v", eventTypes(events))
	}
	if !strings.Contains(h.snapshot(), "invalid_upstream_stream") {
		t.Fatal("want invalid_upstream_stream")
	}
}

// 缓冲期间心跳照发，response id 不变。
func TestBufferedKeepsHeartbeatAndClientID(t *testing.T) {
	good, _ := relayFixture("call_buf_hb", false)
	s := newBPStream("resp_buf_hb")
	s.message("等很久才完成。", 4)
	s.tool(good, 2)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, 2, 4))
	h.holdAt[1] = 2 // 读到第二个分块前阻塞：开场已经发生，正文与工具还在缓冲
	startBuffered(t, h, 1, relaySource())
	h.waitFor(t, "resp_buf_hb")
	// 开场本身含一个 in_progress 事件；至少再出现一个才是缓冲期间的心跳。
	deadline := time.Now().Add(5 * time.Second)
	for strings.Count(h.snapshot(), "event: response.in_progress") < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if strings.Count(h.snapshot(), "event: response.in_progress") < 2 {
		t.Fatalf("heartbeat must continue while buffering: %s", h.snapshot())
	}
	if strings.Contains(h.snapshot(), "等很久") {
		t.Fatal("text delivered before validation")
	}
	close(h.release)
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.created") != 1 || terminalEvent(t, events)["id"] != "resp_buf_hb" || streamedText(events) != "等很久才完成。" {
		t.Fatalf("buffered replay wrong: %v", eventTypes(events))
	}
}

// 无可调用工具或 tool_choice: none 时仍按增量交付。
func TestBufferedWithoutCallableToolsStreamsIncrementally(t *testing.T) {
	for name, src := range map[string]map[string]any{
		"no_tools":    {"model": DefaultModelID, "input": "hi", "stream": true},
		"choice_none": {"model": DefaultModelID, "input": "hi", "stream": true, "tool_choice": "none", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}},
	} {
		t.Run(name, func(t *testing.T) {
			s := newBPStream("resp_buf_inc_" + name)
			s.message("增量正文", 4)
			blocks := s.terminal()
			h := newIncHost(chunks(blocks, indexOfType(blocks, "response.output_text.delta", 1)+1))
			h.holdAt[1] = 1
			startBuffered(t, h, 5, src)
			h.waitFor(t, "output_text.delta")
			close(h.release)
			h.waitClosed(t)
			if rec := h.waitSummary(t); rec.Delivery != "incremental" || callable(rec) != "false" {
				t.Fatalf("summary wrong: %+v", rec)
			}
		})
	}
}

// 重新生成那次遇到流内 429：带 error 关流交给 CPA（不伪装成请求级失败）。
func TestBufferedSecondAttemptRateLimited(t *testing.T) {
	bad, _ := relayFixture("call_buf_rl", true)
	first := newBPStream("resp_buf_rl1")
	first.tool(bad, 2)
	var b strings.Builder
	created := newBPStream("resp_buf_rl2")
	writeSSE(&b, "response.failed", map[string]any{"type": "response.failed", "response": map[string]any{"id": "resp_buf_rl2", "status": "failed", "error": map[string]any{"code": "rate_limit_exceeded", "message": "slow down"}}, "status": 429})
	second := append(append([]string{}, created.blocks...), b.String())
	h := newIncHost(chunks(first.terminal(), 2), chunks(second, 1))
	startBuffered(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.closeErr == nil {
		t.Fatal("rate limit on the regenerated attempt must close with error")
	}
	rec := h.waitSummary(t)
	// final 是最后一次尝试的校验结果：第二次在校验前就因 429 中止，不能残留第一次的 invalid，
	// 否则 A3 会把上游错误算成工具失败。
	if rec.Regen != regenAborted || rec.Exit != "upstream_error" || rec.Attempts != 2 || rec.First != "invalid:invalid_json" || rec.Final != relayNotRun || rec.ErrorKind == "invalid_tool_call" {
		t.Fatalf("summary wrong: %+v", rec)
	}
}

// 重新生成那次遇到流内 401：同样带 error 关流，final 不残留 invalid。
func TestBufferedSecondAttemptUnauthorized(t *testing.T) {
	bad, _ := relayFixture("call_buf_401", true)
	first := newBPStream("resp_buf_401a")
	first.tool(bad, 2)
	var b strings.Builder
	created := newBPStream("resp_buf_401b")
	writeSSE(&b, "response.failed", map[string]any{"type": "response.failed", "response": map[string]any{"id": "resp_buf_401b", "status": "failed", "error": map[string]any{"code": "token_expired", "message": "expired"}}, "status": 401})
	second := append(append([]string{}, created.blocks...), b.String())
	h := newIncHost(chunks(first.terminal(), 2), chunks(second, 1))
	startBuffered(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.closeErr == nil {
		t.Fatal("401 on the regenerated attempt must close with error")
	}
	rec := h.waitSummary(t)
	if rec.Regen != regenAborted || rec.Exit != "upstream_error" || rec.Final != relayNotRun {
		t.Fatalf("summary wrong: %+v", rec)
	}
}

// 截止时间在重新生成前已耗尽：以超时结束，不再发起第二次往返。
func TestBufferedDeadlineExhaustedBeforeRegeneration(t *testing.T) {
	bad, _ := relayFixture("call_buf_dl", true)
	first := newBPStream("resp_buf_dl")
	first.tool(bad, 2)
	h := newIncHost(chunks(first.terminal(), 2))
	h.emitDelay = map[int]time.Duration{1: 1200 * time.Millisecond} // 开场 emit 拖过 1s 截止时间
	startBuffered(t, h, 5, relaySource(), func(s *Service) { s.cfg.TimeoutSeconds = 1 })
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if h.nextID != 1 || rec.Exit != "timeout" {
		t.Fatalf("want timeout without a second attempt: attempts=%d %+v", h.nextID, rec)
	}
}

// 插件停用：缓冲中的请求以 plugin_stopped 结束。
func TestBufferedShutdownStopsRequest(t *testing.T) {
	good, _ := relayFixture("call_buf_stop", false)
	s := newBPStream("resp_buf_stop")
	s.tool(good, 2)
	h := newIncHost(chunks(s.terminal(), 2))
	h.holdAt[1] = 1
	svc := startBuffered(t, h, 5, relaySource())
	h.waitFor(t, "resp_buf_stop")
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(h.release)
	}()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if rec.Exit != "stopped" {
		t.Fatalf("summary wrong: %+v", rec)
	}
}

func TestStreamToolModeConfig(t *testing.T) {
	for _, mode := range []string{"", "incremental", "BUFFERED", " buffered "} {
		cfg := defaultConfig()
		cfg.StreamToolMode = mode
		if err := cfg.normalize(); err != nil {
			t.Fatalf("%q: %v", mode, err)
		}
		if cfg.StreamToolMode != StreamToolModeIncremental && cfg.StreamToolMode != StreamToolModeBuffered {
			t.Fatalf("%q normalized to %q", mode, cfg.StreamToolMode)
		}
	}
	cfg := defaultConfig()
	cfg.StreamToolMode = "fast"
	if err := cfg.normalize(); !isKind(err, "invalid_config") {
		t.Fatalf("invalid mode must be rejected, got %v", err)
	}
	fields := registration(defaultConfig())["metadata"].(map[string]any)["ConfigFields"].([]map[string]any)
	found := false
	for _, f := range fields {
		if f["Name"] == "stream_tool_mode" {
			found = true
		}
	}
	if !found {
		t.Fatal("stream_tool_mode must be declared in ConfigFields")
	}
}
