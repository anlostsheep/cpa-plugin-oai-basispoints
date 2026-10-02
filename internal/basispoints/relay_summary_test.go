package basispoints

// relay_summary（0.1.17.0）：每次执行恰好一条汇总，字段只含类别与计数，不含内容。

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

// callable 把三态的 tool_callable 转成 "true" / "false" / "unknown"。
func callable(rec relaySummaryRecord) string {
	if rec.ToolCallable == nil {
		return "unknown"
	}
	if *rec.ToolCallable {
		return "true"
	}
	return "false"
}

func (h *incHost) summaries() []relaySummaryRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []relaySummaryRecord
	for _, l := range h.logs {
		if rec, ok := relaySummaryFromLog(stringValue(l["message"])); ok {
			out = append(out, rec)
		}
	}
	return out
}

// waitSummary 等到恰好出现一条汇总（汇总在关流之后写），并确认之后不会再多出一条。
func (h *incHost) waitSummary(t *testing.T) relaySummaryRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := h.summaries(); len(got) > 0 {
			time.Sleep(50 * time.Millisecond)
			if got = h.summaries(); len(got) != 1 {
				t.Fatalf("want exactly one relay_summary, got %d: %+v", len(got), got)
			}
			return got[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("relay_summary not logged")
	return relaySummaryRecord{}
}

func assertNoContent(t *testing.T, h *incHost, forbidden ...string) {
	t.Helper()
	logs := strings.Join(h.logMessages(), "\n")
	for _, f := range forbidden {
		if strings.Contains(logs, f) {
			t.Fatalf("logs must not carry content %q: %s", f, logs)
		}
	}
}

func TestRelaySummaryHTTPStreamToolOK(t *testing.T) {
	good, patch := relayFixture("call_summary_ok", false)
	s := newBPStream("resp_summary_ok")
	s.message("说明。", 3)
	s.tool(good, 3)
	h := newIncHost(chunks(s.terminal(), 4))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if rec.V != 1 || rec.Version != Version || rec.Model != DefaultModelID || rec.Transport != TransportHTTP || !rec.Stream {
		t.Fatalf("identity fields wrong: %+v", rec)
	}
	if rec.ConfigMode != StreamToolModeIncremental || rec.Delivery != "incremental" || callable(rec) != "true" || !rec.TextCommitted {
		t.Fatalf("delivery fields wrong: %+v", rec)
	}
	if rec.Attempts != 1 || rec.First != relayLegacy || rec.Final != relayLegacy || rec.Regen != regenNone || rec.LegacyCalls != 1 {
		t.Fatalf("validation fields wrong: %+v", rec)
	}
	if rec.Exit != "completed" || rec.ErrorKind != "" {
		t.Fatalf("exit wrong: %+v", rec)
	}
	assertNoContent(t, h, "说明", "Begin Patch", patch, "test-access")
}

func TestRelaySummaryHTTPStreamRegenerateSuccess(t *testing.T) {
	bad, _ := relayFixture("call_sum_bad", true)
	good, _ := relayFixture("call_sum_good", false)
	first := newBPStream("resp_sum_1")
	first.tool(bad, 2)
	second := newBPStream("resp_sum_2")
	second.tool(good, 2)
	h := newIncHost(chunks(first.terminal(), 2), chunks(second.terminal(), 2))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if rec.Attempts != 2 || rec.First != "invalid:invalid_json" || rec.Final != relayLegacy || rec.Regen != regenSuccess || rec.Exit != "completed" {
		t.Fatalf("regenerate summary wrong: %+v", rec)
	}
	if rec.TextCommitted {
		t.Fatalf("no text was delivered: %+v", rec)
	}
}

func TestRelaySummaryHTTPStreamRegenerateExhausted(t *testing.T) {
	bad1, _ := relayFixture("call_sum_bad1", true)
	bad2, _ := relayFixture("call_sum_bad2", true)
	first := newBPStream("resp_sum_x1")
	first.tool(bad1, 2)
	second := newBPStream("resp_sum_x2")
	second.tool(bad2, 2)
	h := newIncHost(chunks(first.terminal(), 2), chunks(second.terminal(), 2))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if rec.Attempts != 2 || rec.Final != "invalid:invalid_json" || rec.Regen != regenExhausted || rec.Exit != "failed" || rec.ErrorKind != "invalid_tool_call" || rec.LegacyCalls != 0 {
		t.Fatalf("exhausted summary wrong: %+v", rec)
	}
}

func TestRelaySummaryHTTPStreamInvalidAfterText(t *testing.T) {
	bad, _ := relayFixture("call_sum_after_text", true)
	s := newBPStream("resp_sum_after")
	s.message("先说明。", 4)
	s.tool(bad, 3)
	h := newIncHost(chunks(s.terminal(), 5))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if !rec.TextCommitted || rec.Attempts != 1 || rec.First != "invalid:invalid_json" || rec.Final != rec.First || rec.Regen != regenNone || rec.Exit != "failed" {
		t.Fatalf("after-text summary wrong: %+v", rec)
	}
}

func TestRelaySummaryHTTPStreamNoToolsNoCall(t *testing.T) {
	s := newBPStream("resp_sum_text")
	s.message("纯文本。", 3)
	h := newIncHost(chunks(s.terminal(), 3))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	rec := h.waitSummary(t)
	if callable(rec) != "false" || rec.First != relayNoCall || rec.Final != relayNoCall || rec.Exit != "completed" {
		t.Fatalf("text-only summary wrong: %+v", rec)
	}
}

// 校验通过但交付时客户端已断开：exit 不能记为 completed。
func TestRelaySummaryClientDisconnectIsNotCompleted(t *testing.T) {
	good, _ := relayFixture("call_sum_disc", false)
	s := newBPStream("resp_sum_disc")
	s.tool(good, 2)
	h := newIncHost(chunks(s.terminal(), 2))
	// 开场事件（第 1 次 emit）成功，终态回放（之后的 emit）失败：校验已通过但没能交付。
	h.failAfter = 1
	startIncremental(t, h, 5, relaySource())
	rec := h.waitSummary(t)
	if rec.Exit != "cancelled" || rec.Final != relayLegacy {
		t.Fatalf("disconnect summary wrong: %+v", rec)
	}
}

// 同步窗口内建连失败：同步返回错误，并且恰好一条 connect_error 汇总。
func TestRelaySummaryConnectErrorSync(t *testing.T) {
	var mu sync.Mutex
	var logs []string
	svc := NewService()
	svc.SetHost(func(method string, payload any, out any) error {
		p, _ := payload.(map[string]any)
		switch method {
		case "host.http.do_stream":
			*out.(*upstreamStream) = upstreamStream{StatusCode: 429, Headers: http.Header{"Content-Type": {"application/json"}}, StreamID: "err-1"}
		case "host.http.stream_read":
			*out.(*streamChunk) = streamChunk{Payload: []byte(`{"error":{"message":"rate limited"}}`), Done: true}
		case "host.log":
			mu.Lock()
			logs = append(logs, stringValue(p["message"]))
			mu.Unlock()
		}
		return nil
	})
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(relaySource()), Stream: true, StreamID: "client-connect-err", StorageJSON: jsonBytes(map[string]any{"access_token": "secret-token", "account_id": "acct"})}
	if _, err := svc.Handle("executor.execute_stream", jsonBytes(req)); err == nil {
		t.Fatal("want sync error")
	}
	mu.Lock()
	defer mu.Unlock()
	var recs []relaySummaryRecord
	for _, l := range logs {
		if rec, ok := relaySummaryFromLog(l); ok {
			recs = append(recs, rec)
		}
	}
	if len(recs) != 1 || recs[0].Exit != "connect_error" || recs[0].First != relayNotRun || recs[0].Attempts != 1 || callable(recs[0]) != "true" {
		t.Fatalf("connect error summary wrong: %+v", recs)
	}
	if strings.Contains(strings.Join(logs, "\n"), "secret-token") {
		t.Fatal("token leaked into logs")
	}
}

func nonStreamHost(responses []map[string]any, logs *[]string, mu *sync.Mutex) HostCall {
	attempt := 0
	return func(method string, payload any, out any) error {
		p, _ := payload.(map[string]any)
		switch method {
		case "host.http.do":
			mu.Lock()
			r := responses[min(attempt, len(responses)-1)]
			attempt++
			mu.Unlock()
			*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Headers: http.Header{"Content-Type": {"application/json"}}, Body: jsonBytes(r)}
		case "host.log":
			mu.Lock()
			*logs = append(*logs, stringValue(p["message"]))
			mu.Unlock()
		}
		return nil
	}
}

func TestRelaySummaryNonStream(t *testing.T) {
	bad, _ := relayFixture("call_ns_bad", true)
	good, _ := relayFixture("call_ns_good", false)
	var mu sync.Mutex
	var logs []string
	svc := NewService()
	svc.SetHost(nonStreamHost([]map[string]any{
		{"id": "r1", "status": "completed", "output": []any{bad}},
		{"id": "r2", "status": "completed", "output": []any{good}},
	}, &logs, &mu))
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(relaySource()), StorageJSON: jsonBytes(map[string]any{"access_token": "t", "account_id": "a"})}
	if _, err := svc.Handle("executor.execute", jsonBytes(req)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var recs []relaySummaryRecord
	for _, l := range logs {
		if rec, ok := relaySummaryFromLog(l); ok {
			recs = append(recs, rec)
		}
	}
	if len(recs) != 1 {
		t.Fatalf("want one summary, got %+v", recs)
	}
	rec := recs[0]
	if rec.Stream || rec.Delivery != "non_stream" || rec.Attempts != 2 || rec.First != "invalid:invalid_json" || rec.Final != relayLegacy || rec.Regen != regenSuccess || rec.Exit != "completed" || rec.LegacyCalls != 1 {
		t.Fatalf("non-stream summary wrong: %+v", rec)
	}
}

func TestRelaySummaryNonStreamIncomplete(t *testing.T) {
	var mu sync.Mutex
	var logs []string
	svc := NewService()
	svc.SetHost(nonStreamHost([]map[string]any{
		{"id": "r1", "status": "incomplete", "incomplete_details": map[string]any{"reason": "max_output_tokens"}, "output": []any{messageItem("assistant", "partial")}},
	}, &logs, &mu))
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(map[string]any{"model": DefaultModelID, "input": "hi"}), StorageJSON: jsonBytes(map[string]any{"access_token": "t", "account_id": "a"})}
	if _, err := svc.Handle("executor.execute", jsonBytes(req)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, l := range logs {
		if rec, ok := relaySummaryFromLog(l); ok {
			if rec.Exit != "incomplete" || rec.Final != relayNoCall {
				t.Fatalf("incomplete summary wrong: %+v", rec)
			}
			return
		}
	}
	t.Fatal("no summary")
}

func TestRelaySummaryPrepareError(t *testing.T) {
	var mu sync.Mutex
	var logs []string
	svc := NewService()
	svc.SetHost(nonStreamHost(nil, &logs, &mu))
	// 缺少 access_token：prepareRequest 失败，未发起任何上游请求。
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(relaySource()), StorageJSON: jsonBytes(map[string]any{"account_id": "a"})}
	if _, err := svc.Handle("executor.execute", jsonBytes(req)); err == nil {
		t.Fatal("want error")
	}
	mu.Lock()
	defer mu.Unlock()
	var recs []relaySummaryRecord
	for _, l := range logs {
		if rec, ok := relaySummaryFromLog(l); ok {
			recs = append(recs, rec)
		}
	}
	if len(recs) != 1 || recs[0].Exit != "prepare_error" || recs[0].Attempts != 0 || recs[0].First != relayNotRun || callable(recs[0]) != "true" {
		t.Fatalf("prepare error summary wrong: %+v", recs)
	}
}

func TestRelayReasonCategory(t *testing.T) {
	cases := map[string]string{
		"code invalid_json byte_offset=12":   "invalid_json",
		"outer_arguments trailing_content":   "trailing_content",
		"code not_object_or_json_string":     "not_object",
		"code invalid_json_object":           "not_object",
		"arguments null_object":              "not_object",
		"tool_not_in_catalog":                "tool_not_in_catalog",
		"tool_not_allowed_by_tool_choice":    "tool_not_allowed_by_tool_choice",
		"arguments_schema_mismatch":          "arguments_schema_mismatch",
		"custom_args_not_string":             "custom_args_not_string",
		"duplicate_call_id":                  "duplicate_call_id",
		"missing_call_id":                    "missing_call_id",
		"parallel_tool_calls_disabled":       "parallel_limit",
		"required_tool_choice_not_satisfied": "required_tool_choice_not_satisfied",
		"outer_not_transport":                "outer_not_transport",
		"references_invalid":                 "references_invalid",
		"code_not_string":                    "code_not_string",
		"legacy_envelope_invalid":            "legacy_envelope_invalid",
		"something_new":                      "other",
	}
	for reason, want := range cases {
		if got := relayReasonCategory(relayError(reason)); got != want {
			t.Errorf("%q → %q want %q", reason, got, want)
		}
	}
	if got := relayReasonCategory(errors.New("plain")); got != "other" {
		t.Errorf("plain error → %q", got)
	}
}

func TestExitForError(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{nil, "completed"},
		{stoppedError(), "stopped"},
		{timeoutError(defaultConfig()), "timeout"},
		{errClientDisconnected, "cancelled"},
		{fail(499, "client_disconnected", "gone"), "cancelled"},
		{relayError("tool_not_in_catalog"), "failed"},
		{fail(429, "rate_limited", "slow down"), "upstream_error"},
		{fail(502, "upstream_transport", "reset"), "upstream_error"},
	}
	for _, c := range cases {
		if got := exitForError(c.err); got != c.want {
			t.Errorf("%v → %q want %q", c.err, got, c.want)
		}
	}
}

// ws 传输：整轮回放，汇总的 delivery 为 terminal，且同样恰好一条。
func TestRelaySummaryWS(t *testing.T) {
	good, _ := relayFixture("call_ws_sum", false)
	mock := &mockBPS{respond: func(ctx context.Context, conn *websocket.Conn) {
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_ws_sum", "status": "completed", "output": []any{good}}})
		_, _, _ = conn.Read(ctx)
	}}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()
	svc := newWSService(t, server)
	var mu sync.Mutex
	var logs []string
	closed := make(chan struct{}, 1)
	svc.SetHost(func(method string, payload any, out any) error {
		p, _ := payload.(map[string]any)
		switch method {
		case "host.log":
			mu.Lock()
			logs = append(logs, stringValue(p["message"]))
			mu.Unlock()
		case "host.stream.close":
			closed <- struct{}{}
		}
		return nil
	})
	source := jsonBytes(relaySource())
	if _, err := svc.executeStreamWS(ExecutorRequest{Model: DefaultModelID, StreamID: "ws-sum", OriginalRequest: source}, wsBody(), credential{AccessToken: "tok", AccountID: "a"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("stream was never closed")
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	var recs []relaySummaryRecord
	for _, l := range logs {
		if rec, ok := relaySummaryFromLog(l); ok {
			recs = append(recs, rec)
		}
	}
	if len(recs) != 1 || recs[0].Transport != TransportWS || recs[0].Delivery != "terminal" || recs[0].Final != relayLegacy || recs[0].Exit != "completed" || recs[0].Attempts != 1 {
		t.Fatalf("ws summary wrong: %+v", recs)
	}
}

// 汇总日志（host.log）阻塞时，看门狗仍能强制关流：关流不依赖日志完成；日志放行后汇总恰好一条。
func TestRelaySummaryBlockedLogDoesNotBlockForcedClose(t *testing.T) {
	oldGrace := streamDeadlineGrace
	streamDeadlineGrace = 100 * time.Millisecond
	defer func() { streamDeadlineGrace = oldGrace }()
	good, _ := relayFixture("call_blocked_log", false)
	s := newBPStream("resp_blocked_log")
	s.tool(good, 2)
	inner := newIncHost(chunks(s.terminal(), 2))
	releaseLog := make(chan struct{})
	var closes, logsDone atomic.Int32
	closed := make(chan struct{}, 1)
	call := func(method string, payload any, out any) error {
		p, _ := payload.(map[string]any)
		switch method {
		case "host.log":
			if strings.Contains(stringValue(p["message"]), relaySummaryPrefix) {
				<-releaseLog
				defer logsDone.Add(1)
			}
		case "host.stream.close":
			closes.Add(1)
			select {
			case closed <- struct{}{}:
			default:
			}
		}
		return inner.call(method, payload, out)
	}
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(5)
	svc.cfg.TimeoutSeconds = 1
	svc.SetHost(call)
	if _, err := svc.execute(jsonBytes(ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(relaySource()), Stream: true, StreamID: "blocked-log", StorageJSON: jsonBytes(map[string]any{"access_token": "t", "account_id": "a"})}), true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("forced close was blocked by the stuck summary log")
	}
	close(releaseLog)
	deadline := time.Now().Add(5 * time.Second)
	for logsDone.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if n := closes.Load(); n != 1 {
		t.Fatalf("host.stream.close must be called exactly once, got %d", n)
	}
	got := inner.summaries()
	if len(got) != 1 {
		t.Fatalf("want exactly one summary after the log is released, got %d", len(got))
	}
	// 汇总是收尾快照：钩子开始写时终态已交给宿主流，之后才发生的强关不再改写这条记录。
	if got[0].Exit != "completed" || got[0].Final != relayLegacy {
		t.Fatalf("snapshot summary wrong: %+v", got[0])
	}
	// 往返协程已退出：shutdown 不需要等待。
	begin := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("round trip still running (shutdown waited %v)", elapsed)
	}
}

// 强关赢在正常收尾之前：不调用正常钩子，兜底汇总以强关原因（timeout）为准，不继承旧错误。
func TestRelaySummaryForceBeforeNormalCloseUsesForceReason(t *testing.T) {
	h := newIncHost()
	svc := NewService()
	svc.SetHost(h.call)
	summary := svc.newRelaySummary(ExecutorRequest{Model: DefaultModelID}, TransportHTTP, true)
	session := svc.newStreamSession("force-first", 0, nil)
	hookCalls := 0
	summary.bind(session)
	inner := session.onEnd
	session.onEnd = func(outcome string, err error) { hookCalls++; inner(outcome, err) }
	summary.validated(relayStats{}, relayError("tool_not_in_catalog"))
	session.abortDownstream(timeoutError(defaultConfig()))
	session.fail(relayError("tool_not_in_catalog"))
	summary.finishAfter(session, "", relayError("tool_not_in_catalog"))
	if hookCalls != 0 {
		t.Fatalf("normal hook must not run after a forced close, ran %d times", hookCalls)
	}
	rec := h.waitSummary(t)
	if rec.Exit != "timeout" || rec.ErrorKind != "upstream_timeout" {
		t.Fatalf("forced reason must win: %+v", rec)
	}
	if h.closes != 1 {
		t.Fatalf("closes=%d want 1", h.closes)
	}
}

// response.failed 没能交给宿主流（客户端此时断开）：exit 记为 cancelled，而不是 failed。
func TestRelaySummaryFailedEmitFailureIsCancelled(t *testing.T) {
	bad1, _ := relayFixture("call_fe_1", true)
	bad2, _ := relayFixture("call_fe_2", true)
	first := newBPStream("resp_fe_1")
	first.tool(bad1, 2)
	second := newBPStream("resp_fe_2")
	second.tool(bad2, 2)
	h := newIncHost(chunks(first.terminal(), 2), chunks(second.terminal(), 2))
	h.failAfter = 1 // 开场成功，之后的 response.failed 发送失败
	startIncremental(t, h, 5, relaySource())
	rec := h.waitSummary(t)
	if rec.Exit != "cancelled" || rec.Final != "invalid:invalid_json" || rec.Regen != regenExhausted {
		t.Fatalf("summary wrong: %+v", rec)
	}
}

// 首次校验失败、决定重新生成后截止时间已到：final 保留首次的 invalid，但 exit 是 timeout、
// regen 是 aborted——A3 的主指标要求 exit=failed 且 error_kind=invalid_tool_call，不会把它算进去。
func TestRelaySummaryTimeoutBeforeRegenerationSemantics(t *testing.T) {
	h := newIncHost()
	svc := NewService()
	svc.SetHost(h.call)
	summary := svc.newRelaySummary(ExecutorRequest{Model: DefaultModelID}, TransportHTTP, true)
	summary.attemptStarted()
	summary.validated(relayStats{}, relayError("code invalid_json byte_offset=3"))
	summary.regenerating()
	summary.finish("", timeoutError(defaultConfig()))
	rec := h.waitSummary(t)
	if rec.Final != "invalid:invalid_json" || rec.Exit != "timeout" || rec.Regen != regenAborted || rec.ErrorKind != "upstream_timeout" || rec.Attempts != 1 {
		t.Fatalf("summary wrong: %+v", rec)
	}
	if isUserVisibleToolFailure(rec) {
		t.Fatal("timeout before regeneration must not count as a user-visible tool failure")
	}
}

// isUserVisibleToolFailure 是 README 中 A3 主指标的判定：最终校验失败，且请求以工具错误结束。
func isUserVisibleToolFailure(rec relaySummaryRecord) bool {
	return strings.HasPrefix(rec.Final, "invalid:") && rec.Exit == "failed" && rec.ErrorKind == "invalid_tool_call"
}

func TestUserVisibleToolFailurePredicate(t *testing.T) {
	cases := []struct {
		rec  relaySummaryRecord
		want bool
	}{
		{relaySummaryRecord{Final: "invalid:invalid_json", Exit: "failed", ErrorKind: "invalid_tool_call"}, true},
		{relaySummaryRecord{Final: "invalid:invalid_json", Exit: "timeout", ErrorKind: "upstream_timeout"}, false},
		{relaySummaryRecord{Final: "invalid:invalid_json", Exit: "cancelled", ErrorKind: "invalid_tool_call"}, false},
		{relaySummaryRecord{Final: relayNotRun, Exit: "upstream_error", ErrorKind: "rate_limited"}, false},
		{relaySummaryRecord{Final: relayOK, Exit: "completed"}, false},
	}
	for _, c := range cases {
		if got := isUserVisibleToolFailure(c.rec); got != c.want {
			t.Errorf("%+v → %v want %v", c.rec, got, c.want)
		}
	}
}
