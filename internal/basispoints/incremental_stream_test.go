package basispoints

// 增量交付（第一期，0.1.16.0）测试。上游 SSE 夹具只借用 RK 实测序列的事件类型、顺序与数量
// （网页版场景3 / 0927 抓包、上游探针 01/02/05），内容全部为合成数据。

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 上游 SSE 夹具 ----

type bpStream struct {
	id     string
	blocks []string
	output []any
}

func newBPStream(id string) *bpStream {
	s := &bpStream{id: id}
	meta := map[string]any{"id": id, "object": "response", "status": "in_progress", "model": "gpt-6-sol", "output": []any{}, "error": nil}
	s.add("response.created", map[string]any{"response": meta})
	s.add("response.in_progress", map[string]any{"response": cloneObject(meta)})
	return s
}

func (s *bpStream) add(kind string, v map[string]any) {
	v["type"] = kind
	var b strings.Builder
	writeSSE(&b, kind, v)
	s.blocks = append(s.blocks, b.String())
}

func splitText(text string, n int) []string {
	runes := []rune(text)
	if n > len(runes) {
		n = len(runes)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, string(runes[i*len(runes)/n:(i+1)*len(runes)/n]))
	}
	return out
}

func (s *bpStream) reasoning(summary string, deltas int) {
	s.reasoningParts(fmt.Sprintf("rs_%d", len(s.output)), []string{summary}, deltas)
}

// reasoningParts 生成多 summary 部件的推理条目（每个部件按 deltas 均分）。
func (s *bpStream) reasoningParts(id string, parts []string, deltas int) {
	idx := len(s.output)
	summary := make([]any, 0, len(parts))
	for _, text := range parts {
		summary = append(summary, map[string]any{"type": "summary_text", "text": text})
	}
	item := map[string]any{"type": "reasoning", "id": id, "summary": summary, "encrypted_content": "enc-synthetic"}
	s.add("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "reasoning", "id": id, "summary": []any{}}})
	for partIndex, text := range parts {
		s.add("response.reasoning_summary_part.added", map[string]any{"output_index": idx, "item_id": id, "summary_index": partIndex, "part": map[string]any{"type": "summary_text", "text": ""}})
		for _, d := range splitText(text, deltas) {
			s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": idx, "item_id": id, "summary_index": partIndex, "delta": d})
		}
		s.add("response.reasoning_summary_text.done", map[string]any{"output_index": idx, "item_id": id, "summary_index": partIndex, "text": text})
		s.add("response.reasoning_summary_part.done", map[string]any{"output_index": idx, "item_id": id, "summary_index": partIndex, "part": map[string]any{"type": "summary_text", "text": text}})
	}
	s.add("response.output_item.done", map[string]any{"output_index": idx, "item": item})
	s.output = append(s.output, item)
}

func (s *bpStream) message(text string, deltas int) {
	idx, id := len(s.output), fmt.Sprintf("msg_%d", len(s.output))
	part := map[string]any{"type": "output_text", "text": text, "annotations": []any{}}
	item := map[string]any{"type": "message", "id": id, "role": "assistant", "status": "completed", "content": []any{part}}
	s.add("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "message", "id": id, "role": "assistant", "status": "in_progress", "content": []any{}}})
	s.add("response.content_part.added", map[string]any{"output_index": idx, "content_index": 0, "item_id": id, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
	for _, d := range splitText(text, deltas) {
		s.add("response.output_text.delta", map[string]any{"output_index": idx, "content_index": 0, "item_id": id, "delta": d})
	}
	s.add("response.output_text.done", map[string]any{"output_index": idx, "content_index": 0, "item_id": id, "text": text})
	s.add("response.content_part.done", map[string]any{"output_index": idx, "content_index": 0, "item_id": id, "part": part})
	s.add("response.output_item.done", map[string]any{"output_index": idx, "item": item})
	s.output = append(s.output, item)
}

func (s *bpStream) tool(call map[string]any, deltas int) {
	idx := len(s.output)
	item := cloneObject(call)
	item["id"] = fmt.Sprintf("fc_%d", idx)
	item["status"] = "completed"
	args, _ := item["arguments"].(string)
	added := cloneObject(item)
	added["arguments"], added["status"] = "", "in_progress"
	s.add("response.output_item.added", map[string]any{"output_index": idx, "item": added})
	for _, d := range splitText(args, deltas) {
		s.add("response.function_call_arguments.delta", map[string]any{"output_index": idx, "item_id": item["id"], "delta": d})
	}
	s.add("response.function_call_arguments.done", map[string]any{"output_index": idx, "item_id": item["id"], "arguments": args})
	s.add("response.output_item.done", map[string]any{"output_index": idx, "item": item})
	s.output = append(s.output, item)
}

// terminal 追加终态；override 可替换终态 output（用于构造不一致）。
func (s *bpStream) terminal(override ...any) []string {
	output := s.output
	if len(override) > 0 {
		output = override
	}
	s.add("response.completed", map[string]any{"response": map[string]any{"id": s.id, "object": "response", "status": "completed", "model": "gpt-6-sol", "output": output, "usage": map[string]any{"total_tokens": 42}}})
	return s.blocks
}

// chunks 把事件块在给定位置切开（位置为事件块下标，切点之前为一个分块）。
func chunks(blocks []string, cuts ...int) []string {
	var out []string
	prev := 0
	for _, c := range append(cuts, len(blocks)) {
		out = append(out, strings.Join(blocks[prev:c], ""))
		prev = c
	}
	return out
}

func byteChunks(blocks []string, size int) []string {
	all := strings.Join(blocks, "")
	var out []string
	for len(all) > size {
		out = append(out, all[:size])
		all = all[size:]
	}
	return append(out, all)
}

func indexOfType(blocks []string, kind string, nth int) int {
	seen := 0
	for i, b := range blocks {
		if strings.HasPrefix(b, "event: "+kind+"\n") {
			seen++
			if seen == nth {
				return i
			}
		}
	}
	return -1
}

// ---- 宿主桩：按分块逐次读取，可在指定分块前阻塞 ----

type incHost struct {
	mu             sync.Mutex
	attempts       [][]string
	holdAt         map[int]int // 第几次往返（从 1 起）→ 在读取该分块前阻塞，直到 release 关闭
	release        chan struct{}
	nextID         int
	pos            map[string]int
	streamAtt      map[string]int
	reads          int
	bodies         [][]byte
	emitted        []byte
	failEmits      bool
	failAfter      int // >0：第 failAfter 次之后的 emit 失败（模拟开流后客户端断开）
	blockAfter     int // >0：第 blockAfter 次之后的 emit 阻塞到 host.stream.close（宿主队列满）
	emitCalls      int
	upstreamCloses int
	emitDelay      map[int]time.Duration // 第 n 次 emit 先等待该时长再成功（模拟下游读得慢）
	downClosed     chan struct{}
	closeErr       any
	closes         int
	closed         chan struct{}
	logs           []map[string]any
}

func newIncHost(attempts ...[]string) *incHost {
	return &incHost{attempts: attempts, holdAt: map[int]int{}, release: make(chan struct{}), pos: map[string]int{}, streamAtt: map[string]int{}, closed: make(chan struct{}, 1), downClosed: make(chan struct{})}
}

func (h *incHost) call(method string, payload any, out any) error {
	p, _ := payload.(map[string]any)
	switch method {
	case "host.http.do_stream":
		h.mu.Lock()
		h.nextID++
		id := fmt.Sprintf("up-%d", h.nextID)
		h.pos[id], h.streamAtt[id] = 0, h.nextID
		h.bodies = append(h.bodies, p["body"].([]byte))
		h.mu.Unlock()
		*out.(*upstreamStream) = upstreamStream{StatusCode: 200, Headers: http.Header{"Content-Type": {"text/event-stream"}}, StreamID: id}
	case "host.http.stream_read":
		id := stringValue(p["stream_id"])
		h.mu.Lock()
		att, i := h.streamAtt[id], h.pos[id]
		hold, holding := h.holdAt[att]
		h.mu.Unlock()
		if holding && hold == i {
			<-h.release
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.reads++
		if att < 1 || att > len(h.attempts) {
			return fmt.Errorf("unexpected upstream attempt %d", att)
		}
		chunkList := h.attempts[att-1]
		if i >= len(chunkList) {
			*out.(*streamChunk) = streamChunk{Done: true}
			return nil
		}
		h.pos[id] = i + 1
		*out.(*streamChunk) = streamChunk{Payload: []byte(chunkList[i]), Done: i == len(chunkList)-1}
	case "host.http.stream_close":
		h.mu.Lock()
		h.upstreamCloses++
		h.mu.Unlock()
	case "host.stream.emit":
		h.mu.Lock()
		h.emitCalls++
		n, blockAfter := h.emitCalls, h.blockAfter
		delay := h.emitDelay[n]
		h.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		if blockAfter > 0 && n > blockAfter {
			<-h.downClosed
			return errors.New("stream is not open")
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.failEmits || (h.failAfter > 0 && n > h.failAfter) {
			return errors.New("client gone")
		}
		h.emitted = append(h.emitted, p["payload"].([]byte)...)
	case "host.stream.close":
		h.mu.Lock()
		h.closes++
		if h.closes == 1 {
			close(h.downClosed)
		}
		h.closeErr = p["error"]
		h.mu.Unlock()
		select {
		case h.closed <- struct{}{}:
		default:
		}
	case "host.log":
		h.mu.Lock()
		h.logs = append(h.logs, p)
		h.mu.Unlock()
	}
	return nil
}

func (h *incHost) snapshot() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return string(h.emitted)
}

func (h *incHost) waitFor(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(h.snapshot(), substr) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q; emitted so far:\n%s", substr, h.snapshot())
}

func (h *incHost) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-h.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not close")
	}
}

func relaySource() map[string]any {
	return map[string]any{"model": DefaultModelID, "input": "Apply patch", "stream": true, "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
}

func startIncremental(t *testing.T, h *incHost, heartbeat int, src map[string]any) *Service {
	t.Helper()
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(heartbeat)
	svc.SetHost(h.call)
	req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(src), Stream: true, StreamID: "client-" + t.Name(), StorageJSON: jsonBytes(map[string]any{"access_token": "test-access", "account_id": "test-account"})}
	if _, err := svc.Handle("executor.execute_stream", jsonBytes(req)); err != nil {
		t.Fatal(err)
	}
	return svc
}

func streamedText(events []map[string]any) string {
	var b strings.Builder
	for _, e := range events {
		if e["type"] == "response.output_text.delta" {
			delta, _ := e["delta"].(string) // 不用 stringValue：它会去掉首尾空白
			b.WriteString(delta)
		}
	}
	return b.String()
}

func streamedSummary(events []map[string]any) string {
	var b strings.Builder
	for _, e := range events {
		if e["type"] == "response.reasoning_summary_text.delta" {
			delta, _ := e["delta"].(string)
			b.WriteString(delta)
		}
	}
	return b.String()
}

func countType(events []map[string]any, kind string) int {
	n := 0
	for _, e := range events {
		if e["type"] == kind {
			n++
		}
	}
	return n
}

func (h *incHost) logMessages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, l := range h.logs {
		out = append(out, stringValue(l["level"])+":"+stringValue(l["message"])+":"+string(jsonBytes(l["fields"])))
	}
	return out
}

func terminalEvent(t *testing.T, events []map[string]any) map[string]any {
	t.Helper()
	for _, e := range events {
		if e["type"] == "response.completed" {
			return objectValue(e["response"])
		}
	}
	t.Fatalf("no response.completed in %v", eventTypes(events))
	return nil
}

// ---- A1–A4：HTTP 路径 ----

// 夹具 A（场景3 第1轮）：推理 → 正文 → 工具。摘要与正文在终态前到达（摘要先于正文）；
// 工具只在终态后回放，摘要/正文各交付一次且不重复。
func TestIncrementalTextArrivesBeforeTerminal(t *testing.T) {
	text := "表格已检查，下面把第 3 行的公式改成求和，并保留原有格式。"
	summary := "先查看工作表结构，再决定如何修改公式。"
	call, _ := relayFixture("call_scene3", false)
	s := newBPStream("resp_up_scene3")
	s.reasoning(summary, 94)
	s.message(text, 27)
	s.tool(call, 5)
	blocks := s.terminal()
	cut := indexOfType(blocks, "response.output_text.delta", 3) + 1
	h := newIncHost(chunks(blocks, cut))
	h.holdAt[1] = 1
	startIncremental(t, h, 5, relaySource())

	h.waitFor(t, "response.output_text.delta")
	early := h.snapshot()
	for _, forbidden := range []string{"response.completed", "function_call", "custom_tool_call"} {
		if strings.Contains(early, forbidden) {
			t.Fatalf("%s leaked before terminal:\n%s", forbidden, early)
		}
	}
	if !strings.Contains(early, "response.reasoning_summary_text.delta") {
		t.Fatalf("summary must stream before terminal:\n%s", early)
	}
	close(h.release)
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("normal close expected, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedText(events); got != text {
		t.Fatalf("streamed text mismatch (duplicated or missing):\n got %q\nwant %q", got, text)
	}
	if got := streamedSummary(events); got != summary {
		t.Fatalf("streamed summary mismatch (duplicated or missing):\n got %q\nwant %q", got, summary)
	}
	if n := countType(events, "response.created"); n != 1 {
		t.Fatalf("created events = %d", n)
	}
	created := objectValue(events[0]["response"])
	if _, hasError := created["error"]; events[0]["type"] != "response.created" || created["id"] != "resp_up_scene3" || hasError {
		t.Fatalf("prologue must use upstream id without error field: %#v", events[0])
	}
	firstSummary, firstText := -1, -1
	for i, e := range events {
		switch e["type"] {
		case "response.reasoning_summary_text.delta":
			if firstSummary < 0 {
				firstSummary = i
			}
		case "response.output_text.delta":
			if firstText < 0 {
				firstText = i
			}
		}
	}
	if firstSummary < 0 || firstText < 0 || firstSummary > firstText {
		t.Fatalf("summary must be delivered before text (summary=%d text=%d)", firstSummary, firstText)
	}
	for kind, want := range map[string]int{
		"response.reasoning_summary_part.added": 1,
		"response.reasoning_summary_text.done":  1,
		"response.reasoning_summary_part.done":  1,
	} {
		if n := countType(events, kind); n != want {
			t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
		}
	}
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("success must complete exactly once: %v", eventTypes(events))
	}
	final := terminalEvent(t, events)
	if final["id"] != "resp_up_scene3" {
		t.Fatalf("terminal id %v", final["id"])
	}
	output, _ := final["output"].([]any)
	if len(output) != 3 || objectValue(output[0])["type"] != "reasoning" || objectValue(output[2])["type"] != "custom_tool_call" || objectValue(output[2])["name"] != "apply_patch" {
		t.Fatalf("unexpected final output: %s", jsonBytes(output))
	}
	if n := countType(events, "response.content_part.added"); n != 1 {
		t.Fatalf("message content replayed twice: content_part.added=%d", n)
	}
	for _, e := range events {
		if e["type"] == "response.output_text.delta" && (e["item_id"] != "msg_1" || e["output_index"] != float64(1) || e["content_index"] != float64(0)) {
			t.Fatalf("delta routed to wrong item/part: %v", e)
		}
		if e["type"] == "response.output_item.added" {
			item := objectValue(e["item"])
			if stringValue(item["type"]) == "reasoning" {
				if summaryAny, _ := item["summary"].([]any); len(summaryAny) != 0 {
					t.Fatalf("reasoning added must start with empty summary: %v", e)
				}
			}
		}
		if e["type"] == "response.output_item.done" {
			item := objectValue(e["item"])
			if stringValue(item["type"]) == "reasoning" && item["encrypted_content"] != "enc-synthetic" {
				t.Fatalf("reasoning done must carry upstream ciphertext: %v", e)
			}
		}
	}
}

// 夹具 B（0927 第1轮）：推理 → 工具，无正文。摘要随首个增量交付，工具整轮不 commit，
// 只在终态回放。
func TestIncrementalToolOnlyTurnStreamsSummaryBeforeToolAtTerminal(t *testing.T) {
	summary := "只需要调用一次工具。"
	call, _ := relayFixture("call_tool_only", false)
	s := newBPStream("resp_up_tool")
	s.reasoning(summary, 100)
	s.tool(call, 57)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, len(blocks)-1))
	h.holdAt[1] = 1
	startIncremental(t, h, 5, relaySource())
	h.waitFor(t, "response.reasoning_summary_text.delta")
	early := h.snapshot()
	for _, forbidden := range []string{"function_call", "custom_tool_call", "response.completed"} {
		if strings.Contains(early, forbidden) {
			t.Fatalf("%s leaked before terminal:\n%s", forbidden, early)
		}
	}
	close(h.release)
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	final := terminalEvent(t, events)
	output, _ := final["output"].([]any)
	if final["id"] != "resp_up_tool" || len(output) != 2 || objectValue(output[1])["type"] != "custom_tool_call" {
		t.Fatalf("unexpected final: %s", jsonBytes(final))
	}
	if got := streamedSummary(events); got != summary {
		t.Fatalf("streamed summary mismatch (duplicated or missing):\n got %q\nwant %q", got, summary)
	}
	if n := countType(events, "response.reasoning_summary_text.done"); n != 1 {
		t.Fatalf("reasoning summary text.done count=%d want 1: %v", n, eventTypes(events))
	}
	if n := countType(events, "response.output_item.done"); n != 2 {
		t.Fatalf("item lifecycle must close once per item, got %d: %v", n, eventTypes(events))
	}
}

// 夹具 D（探针 01）：纯正文 275 个 delta，按 7 字节切块，逐块解码后文本完整且不重复。
func TestIncrementalByteSplitLongText(t *testing.T) {
	text := strings.Repeat("逐字节切分的正文，", 60)
	s := newBPStream("resp_up_text")
	s.message(text, 275)
	h := newIncHost(byteChunks(s.terminal(), 7))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedText(events); got != text {
		t.Fatalf("byte-split text mismatch: got %d bytes want %d", len(got), len(text))
	}
	if countType(events, "response.output_text.delta") < 275 {
		t.Fatalf("expected incremental deltas, got %d", countType(events, "response.output_text.delta"))
	}
	if terminalEvent(t, events)["id"] != "resp_up_text" {
		t.Fatal("terminal id mismatch")
	}
}

// 未交付摘要/正文前工具无效 → 重生成一次；空摘要不 commit，客户端 id 固定为首轮开流时的上游 id。
func TestIncrementalRegenerateBeforeSummaryKeepsClientID(t *testing.T) {
	bad, _ := relayFixture("call_bad", true)
	good, _ := relayFixture("call_good", false)
	first := newBPStream("resp_attempt1")
	first.reasoning("", 0)
	first.tool(bad, 4)
	second := newBPStream("resp_attempt2")
	second.message("重新生成后的说明。", 6)
	second.tool(good, 4)
	h := newIncHost(chunks(first.terminal(), 3), chunks(second.terminal(), 5))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.nextID != 2 {
		t.Fatalf("attempts=%d want 2", h.nextID)
	}
	if !strings.Contains(string(h.bodies[1]), transportRetryHint) {
		t.Fatal("retry hint missing on second attempt")
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.created") != 1 || objectValue(events[0]["response"])["id"] != "resp_attempt1" {
		t.Fatalf("client id must be frozen at first open: %v", events[0])
	}
	if final := terminalEvent(t, events); final["id"] != "resp_attempt1" {
		t.Fatalf("terminal id %v", final["id"])
	}
	if streamedText(events) != "重新生成后的说明。" {
		t.Fatalf("text %q", streamedText(events))
	}
	if got := streamedSummary(events); got != "" {
		t.Fatalf("empty summary must not deliver deltas, got %q", got)
	}
	if logs := strings.Join(h.logMessages(), "\n"); !strings.Contains(logs, "info:basispoints: regenerating once after invalid tool call") || !strings.Contains(logs, `"transport":"http"`) {
		t.Fatalf("regenerate counter log missing: %s", logs)
	}
}

// 正文已交付后工具无效 → response.failed，不重生成、不重复正文、正常关闭（不冷却凭据）。
func TestIncrementalInvalidToolAfterTextFailsWithoutRetry(t *testing.T) {
	bad, _ := relayFixture("call_bad_after_text", true)
	s := newBPStream("resp_up_badtool")
	s.message("先说明要做什么。", 8)
	s.tool(bad, 4)
	h := newIncHost(chunks(s.terminal(), 6))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.nextID != 1 {
		t.Fatalf("must not regenerate after text was delivered, attempts=%d", h.nextID)
	}
	if h.closeErr != nil {
		t.Fatalf("request-scoped failure must close normally: %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 0 || countType(events, "response.failed") != 1 {
		t.Fatalf("want response.failed only, got %v", eventTypes(events))
	}
	var failed map[string]any
	for _, e := range events {
		if e["type"] == "response.failed" {
			failed = objectValue(objectValue(e["response"])["error"])
		}
	}
	if failed["code"] != "invalid_tool_call" {
		t.Fatalf("failed code %v", failed["code"])
	}
	if streamedText(events) != "先说明要做什么。" {
		t.Fatalf("text duplicated or missing: %q", streamedText(events))
	}
	logs := strings.Join(h.logMessages(), "\n")
	if !strings.Contains(logs, "warn:basispoints: tool call invalid after output was delivered") || strings.Contains(logs, "regenerating once") {
		t.Fatalf("422-after-delivery counter log wrong: %s", logs)
	}
	if strings.Contains(logs, "先说明") || strings.Contains(logs, "apply_patch") {
		t.Fatalf("logs must not carry content: %s", logs)
	}
}

// 纯摘要轮：摘要增量整轮交付一次、生命周期闭合一次，终态只补 completed。
func TestIncrementalSummaryOnlyTurnStreamsOnce(t *testing.T) {
	summary := "只输出推理摘要。"
	s := newBPStream("resp_up_sum_only")
	s.reasoning(summary, 5)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, len(blocks)-1))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("normal close expected, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedSummary(events); got != summary {
		t.Fatalf("streamed summary mismatch (duplicated or missing):\n got %q\nwant %q", got, summary)
	}
	want := "response.created,response.in_progress,response.output_item.added,response.reasoning_summary_part.added," +
		"response.reasoning_summary_text.delta,response.reasoning_summary_text.delta,response.reasoning_summary_text.delta," +
		"response.reasoning_summary_text.delta,response.reasoning_summary_text.delta," +
		"response.reasoning_summary_text.done,response.reasoning_summary_part.done,response.output_item.done,response.completed"
	if got := strings.Join(eventTypes(events), ","); got != want {
		t.Fatalf("summary-only turn replay:\n got %s\nwant %s", got, want)
	}
	if countType(events, "response.output_text.delta") != 0 {
		t.Fatalf("no message text expected: %v", eventTypes(events))
	}
}

// 多个推理条目、每个条目多个 summary 部件：按原始下标回放，不因部件边界压缩索引。
func TestIncrementalMultiReasoningItemsAndSummaryParts(t *testing.T) {
	summary := "第一部分。第二部分。第二条推理。"
	s := newBPStream("resp_up_multi_sum")
	s.reasoningParts("rs_multi_1", []string{"第一部分。", "第二部分。"}, 2)
	s.reasoningParts("rs_multi_2", []string{"第二条推理。"}, 2)
	blocks := s.terminal()
	h := newIncHost(byteChunks(blocks, 11))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedSummary(events); got != summary {
		t.Fatalf("multi-part summary mismatch:\n got %q\nwant %q", got, summary)
	}
	for kind, want := range map[string]int{
		"response.reasoning_summary_part.added": 3,
		"response.reasoning_summary_text.done":  3,
		"response.reasoning_summary_part.done":  3,
		"response.output_item.done":             2,
		"response.completed":                    1,
	} {
		if n := countType(events, kind); n != want {
			t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
		}
	}
	final := terminalEvent(t, events)
	output, _ := final["output"].([]any)
	if len(output) != 2 || objectValue(output[0])["id"] != "rs_multi_1" || objectValue(output[1])["id"] != "rs_multi_2" {
		t.Fatalf("unexpected final output: %s", jsonBytes(output))
	}
}

// 上游在摘要中间截断（无后续 delta/done 事件）→ 终态只补发后缀并补齐生命周期，各一次。
func TestIncrementalSummarySuffixCompletedAtTerminal(t *testing.T) {
	s := newBPStream("resp_up_suffix")
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": "rs_suffix", "summary": []any{}}})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": "rs_suffix", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
	s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": "rs_suffix", "summary_index": 0, "delta": "前半段"})
	item := map[string]any{"type": "reasoning", "id": "rs_suffix", "summary": []any{map[string]any{"type": "summary_text", "text": "前半段后半段"}}, "encrypted_content": "enc-synthetic"}
	blocks := s.terminal(item)
	h := newIncHost(chunks(blocks, len(blocks)-1))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedSummary(events); got != "前半段后半段" {
		t.Fatalf("suffix summary mismatch (duplicated or missing):\n got %q", got)
	}
	for kind, want := range map[string]int{
		"response.reasoning_summary_text.delta": 2,
		"response.reasoning_summary_text.done":  1,
		"response.reasoning_summary_part.done":  1,
		"response.output_item.done":             1,
		"response.completed":                    1,
	} {
		if n := countType(events, kind); n != want {
			t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
		}
	}
}

// 生产事实（v0.1.18.0 事故）：added=ENC0、item.done=ENC1、completed.output=ENC2，同一 reasoning
// 条目的阶段快照密文值两两不同——不能要求跨阶段字节相等。A2 契约：live 帧不携带密文，终态回放
// 的最终 item.done 恰好一次、携带与 completed.output 同值的密文（ENC2）；摘要增量照常交付一次，正常完成。
func TestIncrementalReasoningStageCiphertextsDeliveredOnceAtTerminal(t *testing.T) {
	summary := "分阶段密文不应跨事件比对的摘要。"
	const id = "rs_stage_cipher"
	s := newBPStream("resp_up_stage_cipher")
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": id, "summary": []any{}, "encrypted_content": "ENC0"}})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
	s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "delta": summary})
	s.add("response.reasoning_summary_text.done", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "text": summary})
	s.add("response.reasoning_summary_part.done", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": summary}})
	s.add("response.output_item.done", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": id, "summary": []any{map[string]any{"type": "summary_text", "text": summary}}, "encrypted_content": "ENC1"}})
	final := map[string]any{"type": "reasoning", "id": id, "summary": []any{map[string]any{"type": "summary_text", "text": summary}}, "encrypted_content": "ENC2"}
	h := newIncHost(chunks(s.terminal(final), 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("normal close expected, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("want completed once, got %v", eventTypes(events))
	}
	if got := streamedSummary(events); got != summary {
		t.Fatalf("summary duplicated or missing: %q", got)
	}
	addedReasoning, doneReasoning := 0, 0
	for _, e := range events {
		switch e["type"] {
		case "response.output_item.added":
			item := objectValue(e["item"])
			if stringValue(item["type"]) != "reasoning" {
				continue
			}
			addedReasoning++
			if _, has := item["encrypted_content"]; has {
				t.Fatalf("live reasoning added must not carry ciphertext: %v", e)
			}
		case "response.output_item.done":
			item := objectValue(e["item"])
			if stringValue(item["type"]) != "reasoning" {
				continue
			}
			doneReasoning++
			if item["encrypted_content"] != "ENC2" {
				t.Fatalf("terminal replay must carry the final ciphertext exactly once: %v", e)
			}
		}
	}
	if addedReasoning != 1 || doneReasoning != 1 {
		t.Fatalf("reasoning lifecycle must be one added + one done, got added=%d done=%d: %v", addedReasoning, doneReasoning, eventTypes(events))
	}
	raw := h.snapshot()
	if strings.Contains(raw, "ENC0") || strings.Contains(raw, "ENC1") {
		t.Fatal("live stage ciphertexts leaked to client")
	}
	finalOutput, _ := terminalEvent(t, events)["output"].([]any)
	if len(finalOutput) != 1 || objectValue(finalOutput[0])["encrypted_content"] != "ENC2" {
		t.Fatalf("completed.output must carry the final ciphertext: %s", jsonBytes(finalOutput))
	}
}

// 同一下标先出现推理再出现消息（或相反）→ invalid_upstream_stream；冲突内容不交付。
func TestIncrementalOutputIndexConflictFails(t *testing.T) {
	run := func(t *testing.T, s *bpStream, forbidden string, wantText, wantSummary string) {
		t.Helper()
		blocks := s.terminal()
		h := newIncHost(chunks(blocks, 5))
		startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
		h.waitClosed(t)
		events := clientStreamEvents(t, []byte(h.snapshot()))
		if countType(events, "response.failed") != 1 || countType(events, "response.completed") != 0 {
			t.Fatalf("want response.failed only, got %v", eventTypes(events))
		}
		if got := streamedText(events); got != wantText {
			t.Fatalf("delivered text %q want %q", got, wantText)
		}
		if got := streamedSummary(events); got != wantSummary {
			t.Fatalf("delivered summary %q want %q", got, wantSummary)
		}
		if strings.Contains(h.snapshot(), forbidden) {
			t.Fatalf("conflicting item %q leaked to client", forbidden)
		}
	}

	t.Run("reasoning_then_message", func(t *testing.T) {
		s := newBPStream("resp_up_conflict_a")
		s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": "rs_conf_a", "summary": []any{}}})
		s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": "rs_conf_a", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
		s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": "rs_conf_a", "summary_index": 0, "delta": "先有推理。"})
		s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "message", "id": "msg_conf_a", "role": "assistant", "status": "in_progress", "content": []any{}}})
		run(t, s, "msg_conf_a", "", "先有推理。")
	})

	t.Run("message_then_reasoning", func(t *testing.T) {
		s := newBPStream("resp_up_conflict_b")
		s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "message", "id": "msg_conf_b", "role": "assistant", "status": "in_progress", "content": []any{}}})
		s.add("response.content_part.added", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_conf_b", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
		s.add("response.output_text.delta", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_conf_b", "delta": "正文。"})
		s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": "rs_conf_b", "summary": []any{}}})
		run(t, s, "rs_conf_b", "正文。", "")
	})
}

// summary 生命周期重复（同一部件再次 added）→ invalid_upstream_stream。
func TestIncrementalDuplicateSummaryLifecycleFails(t *testing.T) {
	s := newBPStream("resp_up_dup")
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": "rs_dup", "summary": []any{}}})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": "rs_dup", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
	s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": "rs_dup", "summary_index": 0, "delta": "重复"})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": "rs_dup", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, 5))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.failed") != 1 || countType(events, "response.completed") != 0 {
		t.Fatalf("want response.failed only, got %v", eventTypes(events))
	}
	if got := streamedSummary(events); got != "重复" {
		t.Fatalf("summary must be delivered once before failure: %q", got)
	}
}

// 摘要已交付后工具无效 → response.failed，不重生成（与正文已交付同样的边界）。
func TestIncrementalToolFailureAfterSummaryFailsWithoutRetry(t *testing.T) {
	bad, _ := relayFixture("call_bad_after_sum", true)
	s := newBPStream("resp_up_bad_after_sum")
	s.reasoning("先摘要再失败。", 4)
	s.tool(bad, 4)
	h := newIncHost(chunks(s.terminal(), 4))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.nextID != 1 {
		t.Fatalf("must not regenerate after summary was delivered, attempts=%d", h.nextID)
	}
	if h.closeErr != nil {
		t.Fatalf("request-scoped failure must close normally: %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 0 || countType(events, "response.failed") != 1 {
		t.Fatalf("want response.failed only, got %v", eventTypes(events))
	}
	var failed map[string]any
	for _, e := range events {
		if e["type"] == "response.failed" {
			failed = objectValue(objectValue(e["response"])["error"])
		}
	}
	if failed["code"] != "invalid_tool_call" {
		t.Fatalf("failed code %v", failed["code"])
	}
	if got := streamedSummary(events); got != "先摘要再失败。" {
		t.Fatalf("summary duplicated or missing: %q", got)
	}
	logs := strings.Join(h.logMessages(), "\n")
	if !strings.Contains(logs, "warn:basispoints: tool call invalid after output was delivered") || strings.Contains(logs, "regenerating once") {
		t.Fatalf("post-delivery counter log wrong: %s", logs)
	}
}

// 摘要已交付后上游流内报 429 → 带 error 关闭（交给 CPA 冷却/换号），不发 response.failed。
func TestIncrementalRateLimitAfterSummaryClosesWithError(t *testing.T) {
	s := newBPStream("resp_up_429_sum")
	s.reasoning("部分摘要。", 4)
	blocks := s.blocks[:indexOfType(s.blocks, "response.reasoning_summary_text.delta", 2)+1]
	var b strings.Builder
	writeSSE(&b, "response.failed", map[string]any{"type": "response.failed", "response": map[string]any{"id": "resp_up_429_sum", "status": "failed", "error": map[string]any{"code": "rate_limit_exceeded", "message": "slow down"}}, "status": 429})
	blocks = append(append([]string{}, blocks...), b.String())
	h := newIncHost(chunks(blocks, len(blocks)-1))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr == nil {
		t.Fatal("credential/rate-limit failure after summary must close with error")
	}
	if strings.Contains(h.snapshot(), "response.failed") {
		t.Fatal("must not emit response.failed for non-request-scoped failure")
	}
	// 带 error 关闭没有 [DONE] 终止符，直接核对原始快照里的部分摘要。
	raw := h.snapshot()
	if n := strings.Count(raw, "event: response.reasoning_summary_text.delta"); n != 2 {
		t.Fatalf("partial summary before 429: %d deltas\n%s", n, raw)
	}
	if strings.Contains(raw, "response.reasoning_summary_text.done") {
		t.Fatal("truncated stream must not close the summary lifecycle")
	}
}

// 摘要交付时 emit 失败（客户端断开）→ 中止读取，不再发起后续 stream_read。
func TestIncrementalSummaryEmitFailureStopsReading(t *testing.T) {
	s := newBPStream("resp_up_sum_gone")
	s.reasoning("客户端断开摘要。", 7)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, 3, 5, 7))
	h.failAfter = 1
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	h.mu.Lock()
	reads, closeErr := h.reads, h.closeErr
	h.mu.Unlock()
	if reads != 2 {
		t.Fatalf("reads=%d after summary emit failure, want 2", reads)
	}
	if closeErr != nil {
		t.Fatalf("client disconnect closes without error, got %v", closeErr)
	}
}

// 上游完全没有推理事件、推理条目只在终态出现 → 终态回放完整摘要生命周期一次，正文不重复。
func TestIncrementalReasoningMissingEventsReplayedAtTerminal(t *testing.T) {
	text := "正文照常增量。"
	msgID := "msg_missing"
	msg := map[string]any{"type": "message", "id": msgID, "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}
	s := newBPStream("resp_up_missing")
	s.add("response.output_item.added", map[string]any{"output_index": 1, "item": map[string]any{"type": "message", "id": msgID, "role": "assistant", "status": "in_progress", "content": []any{}}})
	s.add("response.content_part.added", map[string]any{"output_index": 1, "content_index": 0, "item_id": msgID, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
	s.add("response.output_text.delta", map[string]any{"output_index": 1, "content_index": 0, "item_id": msgID, "delta": text})
	s.add("response.output_text.done", map[string]any{"output_index": 1, "content_index": 0, "item_id": msgID, "text": text})
	s.add("response.content_part.done", map[string]any{"output_index": 1, "content_index": 0, "item_id": msgID, "part": map[string]any{"type": "output_text", "text": text, "annotations": []any{}}})
	s.add("response.output_item.done", map[string]any{"output_index": 1, "item": msg})
	reasoning := map[string]any{"type": "reasoning", "id": "rs_missing", "summary": []any{map[string]any{"type": "summary_text", "text": "只在终态出现的推理。"}}, "encrypted_content": "enc-missing"}
	blocks := s.terminal(reasoning, msg)
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedText(events); got != text {
		t.Fatalf("text duplicated or missing: %q", got)
	}
	if got := streamedSummary(events); got != "只在终态出现的推理。" {
		t.Fatalf("terminal-only reasoning summary: %q", got)
	}
	for kind, want := range map[string]int{
		"response.reasoning_summary_part.added": 1,
		"response.reasoning_summary_text.done":  1,
		"response.reasoning_summary_part.done":  1,
		"response.output_item.done":             2,
		"response.completed":                    1,
	} {
		if n := countType(events, kind); n != want {
			t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
		}
	}
}

// 上游缺少 content_part.added（不满足 commit 前提）：退回终态回放，并记录一次计数日志。
func TestIncrementalNoCommitFallsBackAndLogs(t *testing.T) {
	s := newBPStream("resp_up_nopart")
	s.message("缺少部件事件的正文", 4)
	var blocks []string
	for _, b := range s.terminal() {
		if !strings.HasPrefix(b, "event: response.content_part.added\n") {
			blocks = append(blocks, b)
		}
	}
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if streamedText(events) != "缺少部件事件的正文" || terminalEvent(t, events)["id"] != "resp_up_nopart" {
		t.Fatalf("fallback replay wrong: %v", eventTypes(events))
	}
	if logs := strings.Join(h.logMessages(), "\n"); !strings.Contains(logs, "replayed at terminal without incremental delivery") {
		t.Fatalf("no-commit counter log missing: %s", logs)
	}
}

// 终态正文与已交付不一致 → invalid_upstream_stream（请求级，response.failed）。
func TestIncrementalFinalMismatchFails(t *testing.T) {
	call, _ := relayFixture("call_mismatch_valid", false)
	s := newBPStream("resp_up_mismatch")
	s.message("已经发出的正文", 4)
	s.tool(call, 3)
	changed := cloneObject(objectValue(s.output[0]))
	changed["content"] = []any{map[string]any{"type": "output_text", "text": "完全不同的正文", "annotations": []any{}}}
	blocks := s.terminal(changed, s.output[1])
	h := newIncHost(chunks(blocks, 5))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 0 {
		t.Fatal("mismatched terminal must not complete")
	}
	var code any
	for _, e := range events {
		if e["type"] == "response.failed" {
			code = objectValue(objectValue(e["response"])["error"])["code"]
		}
	}
	if code != "invalid_upstream_stream" && code != "invalid_upstream_response" {
		t.Fatalf("failed code %v (events %v)", code, eventTypes(events))
	}
	// 不一致的终态须在触碰工具身份前被拒绝：同 call_id 的历史只按客户端条目自身冷重建。
	assertNoReplayableFailure(t, "call_mismatch_valid", "apply_patch", "client-owned input")
}

// 上游未给 done 事件、终态正文比已交付更长：Service 路径须补发未交付后缀。
func TestIncrementalTerminalSuffixIsReplayed(t *testing.T) {
	s := newBPStream("resp_up_suffix")
	full := "前半段后半段"
	part := map[string]any{"type": "output_text", "text": full, "annotations": []any{}}
	item := map[string]any{"type": "message", "id": "msg_0", "role": "assistant", "status": "completed", "content": []any{part}}
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "message", "id": "msg_0", "role": "assistant", "status": "in_progress", "content": []any{}}})
	s.add("response.content_part.added", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
	s.add("response.output_text.delta", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "delta": "前半段"})
	s.output = append(s.output, item)
	h := newIncHost(chunks(s.terminal(), 5))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedText(events); got != full {
		t.Fatalf("undelivered suffix not replayed: %q", got)
	}
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("suffix replay must complete exactly once: %v", eventTypes(events))
	}
	if countType(events, "response.output_text.done") != 1 || countType(events, "response.content_part.added") != 1 {
		t.Fatalf("unexpected replay events %v", eventTypes(events))
	}
}

// 开场成功后正文交付失败（客户端断开）：错误必须传回读取循环并停止读取上游。
func TestIncrementalTextEmitFailureStopsReading(t *testing.T) {
	s := newBPStream("resp_up_emitfail")
	s.message("开场之后客户端断开", 5)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, 2, 5, 7, 9))
	h.failAfter = 1 // 第 1 次 emit（以上游 id 开场）成功，之后的正文交付失败
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	h.mu.Lock()
	reads, closeErr := h.reads, h.closeErr
	h.mu.Unlock()
	if reads != 2 {
		t.Fatalf("reads=%d after text emit failure, want 2", reads)
	}
	if closeErr != nil {
		t.Fatalf("client disconnect after delivered text must close without error, got %v", closeErr)
	}
}

// 流末尾（终态之后）有一个未以空行结尾的非法事件：EOF 冲刷后同样被状态机拒绝。
func TestIncrementalTrailingEventWithoutBlankLineIsRejected(t *testing.T) {
	for _, withBlank := range []bool{false, true} {
		t.Run(fmt.Sprintf("blank=%t", withBlank), func(t *testing.T) {
			s := newBPStream("resp_up_trailing")
			s.message("正文", 2)
			blocks := s.terminal()
			var b strings.Builder
			writeSSE(&b, "response.output_text.delta", map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "item_id": "msg_0", "delta": "终态之后"})
			trailing := b.String()
			if !withBlank {
				trailing = strings.TrimSuffix(trailing, "\n\n")
			}
			blocks = append(append([]string{}, blocks...), trailing)
			h := newIncHost(chunks(blocks, 5))
			startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
			h.waitClosed(t)
			events := clientStreamEvents(t, []byte(h.snapshot()))
			if countType(events, "response.completed") != 0 || countType(events, "response.failed") != 1 {
				t.Fatalf("event after terminal must fail the stream: %v", eventTypes(events))
			}
		})
	}
}

// done 事件的 logprobs 与已交付增量冲突：拒绝，不把冲突数据当作完成。
func TestIncrementalConflictingDoneLogprobsRejected(t *testing.T) {
	s := newBPStream("resp_up_logprobs")
	good := []any{map[string]any{"token": "好", "logprob": -0.1}}
	bad := []any{map[string]any{"token": "坏", "logprob": -99}}
	part := map[string]any{"type": "output_text", "text": "好", "annotations": []any{}, "logprobs": good}
	item := map[string]any{"type": "message", "id": "msg_0", "role": "assistant", "status": "completed", "content": []any{part}}
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "message", "id": "msg_0", "role": "assistant", "status": "in_progress", "content": []any{}}})
	s.add("response.content_part.added", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}, "logprobs": []any{}}})
	s.add("response.output_text.delta", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "delta": "好", "logprobs": good})
	s.add("response.output_text.done", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "text": "好", "logprobs": bad})
	s.output = append(s.output, item)
	h := newIncHost(chunks(s.terminal(), 5))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	raw := h.snapshot()
	if strings.Contains(raw, "-99") {
		t.Fatal("conflicting logprobs were delivered to the client")
	}
	events := clientStreamEvents(t, []byte(raw))
	if countType(events, "response.completed") != 0 || countType(events, "response.failed") != 1 {
		t.Fatalf("conflicting done logprobs must fail: %v", eventTypes(events))
	}
}

// 心跳关闭、下游不读（宿主队列满，正文增量 emit 持锁阻塞）：请求截止时间到后带超时错误关闭下游。
func TestIncrementalDeadlineAbortsBlockedDownstream(t *testing.T) {
	oldGrace := streamDeadlineGrace
	streamDeadlineGrace = 100 * time.Millisecond
	defer func() { streamDeadlineGrace = oldGrace }()
	s := newBPStream("resp_up_blocked")
	s.message(strings.Repeat("阻塞", 40), 40)
	blocks := s.terminal()
	cuts := make([]int, 0, len(blocks))
	for i := 1; i < len(blocks); i++ {
		cuts = append(cuts, i)
	}
	h := newIncHost(chunks(blocks, cuts...))
	h.blockAfter = 3
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(0)
	svc.cfg.TimeoutSeconds = 1
	svc.SetHost(h.call)
	start := time.Now()
	if _, err := svc.execute(httpStreamRequest("blocked-downstream"), true); err != nil {
		t.Fatal(err)
	}
	h.waitClosed(t)
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("blocked downstream released only after %v", elapsed)
	}
	time.Sleep(200 * time.Millisecond)
	h.mu.Lock()
	closes, closeErr := h.closes, h.closeErr
	h.mu.Unlock()
	if closes != 1 || !strings.Contains(fmt.Sprint(closeErr), "timed out") {
		t.Fatalf("want exactly one close with timeout error, got closes=%d err=%v", closes, closeErr)
	}
	// 往返已退出：shutdown 无需等待进行中的往返；上游流恰好关闭一次。
	begin := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("round trip still running after forced close (shutdown waited %v)", elapsed)
	}
	h.mu.Lock()
	upstreamCloses := h.upstreamCloses
	h.mu.Unlock()
	if upstreamCloses != 1 {
		t.Fatalf("upstream closed %d times, want 1", upstreamCloses)
	}
	// 截止看门狗强关：汇总记 timeout，不能是 completed，也要记下已交付的正文。
	rec := h.waitSummary(t)
	if rec.Exit != "timeout" || rec.ErrorKind != "upstream_timeout" || !rec.TextCommitted {
		t.Fatalf("forced-close summary wrong: %+v", rec)
	}
}

// 同一块内：正文交付跨过截止时间后，随后的事件又不一致。守卫已记录的超时优先于回调的
// 一致性错误——带 timeout 关闭，不发请求级 response.failed。
func TestIncrementalTimeoutBeatsLaterStreamError(t *testing.T) {
	oldGrace := streamDeadlineGrace
	streamDeadlineGrace = 5 * time.Second
	defer func() { streamDeadlineGrace = oldGrace }()
	s := newBPStream("resp_up_timeout_then_bad")
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "message", "id": "msg_0", "role": "assistant", "status": "in_progress", "content": []any{}}})
	s.add("response.content_part.added", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
	s.add("response.output_text.delta", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "delta": "abc"})
	s.add("response.output_text.done", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_0", "text": "xyz"})
	h := newIncHost(chunks(s.blocks)) // 唯一一块
	h.emitDelay = map[int]time.Duration{2: 1300 * time.Millisecond}
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(5)
	svc.cfg.TimeoutSeconds = 1
	svc.SetHost(h.call)
	if _, err := svc.execute(httpStreamRequest("timeout-then-bad"), true); err != nil {
		t.Fatal(err)
	}
	h.waitClosed(t)
	h.mu.Lock()
	closeErr, raw := h.closeErr, string(h.emitted)
	h.mu.Unlock()
	if !strings.Contains(fmt.Sprint(closeErr), "timed out") || strings.Contains(raw, "response.failed") {
		t.Fatalf("timeout must win over later stream error: closeErr=%v failed=%t", closeErr, strings.Contains(raw, "response.failed"))
	}
}

// commit 后上游流内报 429 → 带 error 关闭（交给 CPA 冷却/换号），不发 response.failed。
func TestIncrementalRateLimitAfterTextClosesWithError(t *testing.T) {
	s := newBPStream("resp_up_429")
	s.message("部分正文", 4)
	blocks := s.blocks[:indexOfType(s.blocks, "response.output_text.delta", 2)+1]
	var b strings.Builder
	writeSSE(&b, "response.failed", map[string]any{"type": "response.failed", "response": map[string]any{"id": "resp_up_429", "status": "failed", "error": map[string]any{"code": "rate_limit_exceeded", "message": "slow down"}}, "status": 429})
	blocks = append(append([]string{}, blocks...), b.String())
	h := newIncHost(chunks(blocks, len(blocks)-1))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr == nil {
		t.Fatal("credential/rate-limit failure after text must close with error")
	}
	if strings.Contains(h.snapshot(), "response.failed") {
		t.Fatal("must not emit response.failed for non-request-scoped failure")
	}
}

// 客户端断开：交付失败即中止读取，不再发起后续 stream_read。
func TestIncrementalClientDisconnectStopsReading(t *testing.T) {
	s := newBPStream("resp_up_gone")
	s.message("客户端很快断开", 7)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, 5, 7, 9))
	h.failEmits = true
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	h.mu.Lock()
	reads, closeErr := h.reads, h.closeErr
	h.mu.Unlock()
	if reads != 1 {
		t.Fatalf("reads=%d after client disconnect, want 1", reads)
	}
	if closeErr != nil {
		t.Fatalf("client disconnect closes without error, got %v", closeErr)
	}
}

// onChunk 返回错误（交付失败/事件不一致）即中止读取，不再发起下一次 stream_read。
func TestReadGuardedIntoStopsOnChunkError(t *testing.T) {
	s := newBPStream("resp_up_chunkerr")
	s.message("三个分块", 3)
	h := newIncHost(chunks(s.terminal(), 3, 5, 7))
	svc := NewService()
	svc.SetHost(h.call)
	var stream upstreamStream
	if err := h.call("host.http.do_stream", map[string]any{"body": []byte("{}")}, &stream); err != nil {
		t.Fatal(err)
	}
	g := svc.newUpstreamGuard("", "", nil, nil, time.Minute, timeoutError(svc.config()))
	defer g.release()
	g.attach(stream.StreamID)
	boom := errors.New("boom")
	_, err := svc.readGuardedInto(stream, g, func([]byte) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("want onChunk error, got %v", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.reads != 1 {
		t.Fatalf("reads=%d after onChunk error, want 1", h.reads)
	}
}

// 空闲心跳先于上游 created 到达 → 全程使用合成 resp_bp_ id，只有一次 created。
func TestIncrementalHeartbeatFirstUsesSyntheticID(t *testing.T) {
	s := newBPStream("resp_up_late")
	s.message("心跳之后才开始输出", 5)
	h := newIncHost(chunks(s.terminal(), 4))
	h.holdAt[1] = 0
	startIncremental(t, h, 1, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitFor(t, "resp_bp_")
	close(h.release)
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	id := stringValue(objectValue(events[0]["response"])["id"])
	if !strings.HasPrefix(id, "resp_bp_") || countType(events, "response.created") != 1 {
		t.Fatalf("want single synthetic created, got %v %v", id, eventTypes(events))
	}
	if final := terminalEvent(t, events); final["id"] != id {
		t.Fatalf("terminal id %v != client id %v", final["id"], id)
	}
	if streamedText(events) != "心跳之后才开始输出" {
		t.Fatalf("text %q", streamedText(events))
	}
}

// 心跳关闭（heartbeat_seconds=0）：不提前开流，但正文仍按增量交付。
func TestIncrementalWithoutHeartbeatStillStreamsText(t *testing.T) {
	s := newBPStream("resp_up_nohb")
	s.message("无心跳也能增量输出", 6)
	blocks := s.terminal()
	cut := indexOfType(blocks, "response.output_text.delta", 2) + 1
	h := newIncHost(chunks(blocks, cut))
	h.holdAt[1] = 1
	startIncremental(t, h, 0, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitFor(t, "response.output_text.delta")
	close(h.release)
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if objectValue(events[0]["response"])["id"] != "resp_up_nohb" || streamedText(events) != "无心跳也能增量输出" {
		t.Fatalf("unexpected events %v", eventTypes(events))
	}
}

// ---- 会话与回放单元 ----

// 心跳只在空闲时发送：持续输出期间不插入 in_progress，空闲后恢复。
func TestStreamSessionHeartbeatOnlyWhenIdle(t *testing.T) {
	h := newSessionHost()
	svc := sessionService(h)
	ss := svc.newStreamSession("s", 60*time.Millisecond, nil)
	ss.start()
	stop := time.After(300 * time.Millisecond)
busy:
	for i := 0; ; i++ {
		select {
		case <-stop:
			break busy
		case <-time.After(10 * time.Millisecond):
			frame := map[string]any{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "item_id": "m", "delta": "x"}
			if err := ss.deliver(nil, []map[string]any{frame}); err != nil {
				t.Fatal(err)
			}
		}
	}
	h.mu.Lock()
	busyHeartbeats := strings.Count(string(h.emitted), "event: response.in_progress") - 1 // 减去开场
	h.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	ss.finish(map[string]any{"id": "r", "status": "completed", "output": []any{}})
	h.mu.Lock()
	total := strings.Count(string(h.emitted), "event: response.in_progress") - 1
	h.mu.Unlock()
	if busyHeartbeats != 0 {
		t.Fatalf("heartbeat interleaved with active output: %d", busyHeartbeats)
	}
	if total-busyHeartbeats < 2 {
		t.Fatalf("idle heartbeats not sent: %d", total-busyHeartbeats)
	}
}

// 缓冲回放（ws / 未 commit）按上游 #12 逐段给出 message 内容，added 不带全量 content。
func TestSyntheticEventsReplayMessageContentParts(t *testing.T) {
	msg := map[string]any{"type": "message", "id": "msg_x", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": "hello", "annotations": []any{}}}}
	events := syntheticEvents(map[string]any{"id": "r", "status": "completed", "output": []any{msg}}, true)
	var types []string
	for _, e := range events {
		types = append(types, e.name)
		if e.name == "response.output_item.added" {
			if content, _ := objectValue(e.value["item"])["content"].([]any); len(content) != 0 {
				t.Fatalf("added must not carry full content: %v", e.value)
			}
		}
	}
	want := "response.created,response.in_progress,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed"
	if strings.Join(types, ",") != want {
		t.Fatalf("replay events:\n got %s\nwant %s", strings.Join(types, ","), want)
	}
}

// keepFinal：commit 后终态只补发未交付的正文后缀，并跳过已交付的开场/部件事件。
func TestKeepFinalReplaysOnlyUndeliveredSuffix(t *testing.T) {
	var forwarded []map[string]any
	d := newStreamDelivery(nil, func(meta map[string]any, frames []map[string]any) error {
		forwarded = append(forwarded, frames...)
		return nil
	})
	s := newBPStream("resp_k")
	s.message("前半段后半段", 2)
	// 只喂到第一个 delta（commit，交付「前半段」）
	upto := indexOfType(s.blocks, "response.output_text.delta", 1)
	dec := newSSEDecoder()
	if err := dec.feed([]byte(strings.Join(s.blocks[:upto+1], "")), d.consume); err != nil {
		t.Fatal(err)
	}
	if !d.committed || streamedText(forwarded) != "前半段" {
		t.Fatalf("commit state %v text %q", d.committed, streamedText(forwarded))
	}
	final := map[string]any{"id": "resp_k", "status": "completed", "output": s.output}
	if err := d.validateFinal(final); err != nil {
		t.Fatal(err)
	}
	events := syntheticEvents(final, false)
	var kept []string
	var suffix string
	for i := range events {
		if d.keepFinal(&events[i]) {
			kept = append(kept, events[i].name)
			if events[i].name == "response.output_text.delta" {
				suffix, _ = events[i].value["delta"].(string)
			}
		}
	}
	if suffix != "后半段" {
		t.Fatalf("suffix %q", suffix)
	}
	want := "response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed"
	if strings.Join(kept, ",") != want {
		t.Fatalf("kept %s", strings.Join(kept, ","))
	}
}

// keepFinal：摘要 commit 后终态只补发未交付的摘要后缀，并补齐未交付的生命周期事件。
func TestKeepFinalReplaysUndeliveredSummarySuffix(t *testing.T) {
	var forwarded []map[string]any
	d := newStreamDelivery(nil, func(meta map[string]any, frames []map[string]any) error {
		forwarded = append(forwarded, frames...)
		return nil
	})
	s := newBPStream("resp_k_sum")
	s.reasoning("前半段后半段", 2)
	// 只喂到第一个 delta（commit，交付「前半段」）
	upto := indexOfType(s.blocks, "response.reasoning_summary_text.delta", 1)
	dec := newSSEDecoder()
	if err := dec.feed([]byte(strings.Join(s.blocks[:upto+1], "")), d.consume); err != nil {
		t.Fatal(err)
	}
	if !d.committed || streamedSummary(forwarded) != "前半段" {
		t.Fatalf("commit state %v summary %q", d.committed, streamedSummary(forwarded))
	}
	final := map[string]any{"id": "resp_k_sum", "status": "completed", "output": s.output}
	if err := d.validateFinal(final); err != nil {
		t.Fatal(err)
	}
	events := syntheticEventsWithReasoningSummary(final, false)
	var kept []string
	var suffix string
	for i := range events {
		if d.keepFinal(&events[i]) {
			kept = append(kept, events[i].name)
			if events[i].name == "response.reasoning_summary_text.delta" {
				suffix, _ = events[i].value["delta"].(string)
			}
		}
	}
	if suffix != "后半段" {
		t.Fatalf("suffix %q", suffix)
	}
	want := "response.reasoning_summary_text.delta,response.reasoning_summary_text.done,response.reasoning_summary_part.done,response.output_item.done,response.completed"
	if strings.Join(kept, ",") != want {
		t.Fatalf("kept %s", strings.Join(kept, ","))
	}
}

// 空摘要不 commit；直到首个非空消息正文增量才整批交付（含此前未交付的推理生命周期）。
func TestStreamDeliveryEmptySummaryDoesNotCommitUntilText(t *testing.T) {
	var forwarded []map[string]any
	d := newStreamDelivery(nil, func(meta map[string]any, frames []map[string]any) error {
		forwarded = append(forwarded, frames...)
		return nil
	})
	s := newBPStream("resp_empty_sum")
	s.reasoning("", 0)
	s.message("正文。", 1)
	firstDelta := indexOfType(s.blocks, "response.output_text.delta", 1)
	dec := newSSEDecoder()
	if err := dec.feed([]byte(strings.Join(s.blocks[:firstDelta], "")), d.consume); err != nil {
		t.Fatal(err)
	}
	if d.committed || len(forwarded) != 0 {
		t.Fatalf("empty summary must not commit: committed=%v forwarded=%d", d.committed, len(forwarded))
	}
	if err := dec.feed([]byte(s.blocks[firstDelta]), d.consume); err != nil {
		t.Fatal(err)
	}
	if !d.committed || len(forwarded) != 7 {
		// 8 个缓冲事件里 reasoning item.done 只校验不转发（密文延迟终态），批内交付 7 帧。
		t.Fatalf("message text must commit with 7 pending frames: committed=%v frames=%d", d.committed, len(forwarded))
	}
	if got := streamedSummary(forwarded); got != "" {
		t.Fatalf("empty summary delivered deltas: %q", got)
	}
	if streamedText(forwarded) != "正文。" {
		t.Fatalf("text %q", streamedText(forwarded))
	}
}

// reasoning 摘要增量只由专用生成器产生：默认回放保持冻结的 legacy 形状（added 携带完整 summary）。
func TestReasoningSummaryEventsOnlyInDedicatedGenerator(t *testing.T) {
	item := map[string]any{"type": "reasoning", "id": "rs_gen", "summary": []any{map[string]any{"type": "summary_text", "text": "摘要正文。"}}, "encrypted_content": "enc-gen"}
	response := map[string]any{"id": "r", "status": "completed", "output": []any{item}}
	var legacy []string
	for _, e := range syntheticEvents(response, false) {
		legacy = append(legacy, e.name)
		if strings.HasPrefix(e.name, "response.reasoning_summary_") {
			t.Fatalf("legacy replay must not emit %s", e.name)
		}
		if e.name == "response.output_item.added" {
			if summaryAny, _ := objectValue(e.value["item"])["summary"].([]any); len(summaryAny) != 1 {
				t.Fatalf("legacy added must carry full summary: %v", e.value)
			}
			if objectValue(e.value["item"])["encrypted_content"] != "enc-gen" {
				t.Fatalf("legacy added must stay unchanged (ciphertext kept): %v", e.value)
			}
		}
	}
	if want := "response.output_item.added,response.output_item.done,response.completed"; strings.Join(legacy, ",") != want {
		t.Fatalf("legacy sequence %s", strings.Join(legacy, ","))
	}
	var dedicated []string
	for _, e := range syntheticEventsWithReasoningSummary(response, false) {
		dedicated = append(dedicated, e.name)
		switch e.name {
		case "response.output_item.added":
			if summaryAny, _ := objectValue(e.value["item"])["summary"].([]any); len(summaryAny) != 0 {
				t.Fatalf("dedicated added must start with empty summary: %v", e.value)
			}
			if _, has := objectValue(e.value["item"])["encrypted_content"]; has {
				t.Fatalf("dedicated added must not carry ciphertext: %v", e.value)
			}
		case "response.output_item.done":
			if objectValue(e.value["item"])["encrypted_content"] != "enc-gen" {
				t.Fatalf("dedicated done must carry full item: %v", e.value)
			}
		}
	}
	want := "response.output_item.added,response.reasoning_summary_part.added,response.reasoning_summary_text.delta,response.reasoning_summary_text.done,response.reasoning_summary_part.done,response.output_item.done,response.completed"
	if strings.Join(dedicated, ",") != want {
		t.Fatalf("dedicated sequence:\n got %s\nwant %s", strings.Join(dedicated, ","), want)
	}
}

// 下游不读（宿主队列满、增量 emit 阻塞在会话锁内）时 shutdown 仍有界。
func TestIncrementalShutdownBoundedWhenDeliveryStalls(t *testing.T) {
	oldWait, oldGrace := shutdownWait, shutdownForceGrace
	shutdownWait, shutdownForceGrace = 3*time.Second, 200*time.Millisecond
	defer func() { shutdownWait, shutdownForceGrace = oldWait, oldGrace }()

	s := newBPStream("resp_up_stall")
	s.message("下游停止读取", 6)
	blocks := s.terminal()
	inner := newIncHost(chunks(blocks, indexOfType(blocks, "response.output_text.delta", 1)+1))
	var stall atomic.Bool
	downClosed := make(chan struct{})
	var once sync.Once
	var closes atomic.Int32
	call := func(method string, payload any, out any) error {
		switch method {
		case "host.stream.emit":
			if stall.Load() {
				<-downClosed
				return errors.New("stream is not open")
			}
		case "host.stream.close":
			closes.Add(1)
			once.Do(func() { close(downClosed) })
			return nil
		}
		return inner.call(method, payload, out)
	}
	inner.holdAt[1] = 1
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(5)
	svc.SetHost(call)
	if _, err := svc.execute(httpStreamRequest("stall-incremental"), true); err != nil {
		t.Fatal(err)
	}
	inner.waitFor(t, "response.output_text.delta")
	stall.Store(true)
	close(inner.release) // 后续增量 emit 阻塞
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if _, err := svc.Handle("plugin.shutdown", nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= shutdownWait {
		t.Fatalf("shutdown waited the full %v: stalled incremental emit never released", shutdownWait)
	}
	if n := closes.Load(); n != 1 {
		t.Fatalf("host.stream.close must be called exactly once, got %d", n)
	}
	// shutdown 看门狗强关：汇总以强关原因为准（stopped），并记下已交付的正文。
	rec := inner.waitSummary(t)
	if rec.Exit != "stopped" || rec.ErrorKind != "plugin_stopped" || !rec.TextCommitted {
		t.Fatalf("forced-close summary wrong: %+v", rec)
	}
}

// 最后一块（Done）交付时下游读得慢、跨过请求截止时间（但未到截止看门狗）：守卫已记录的超时
// 不能被忽略——以带 error 的超时关闭，不发 completed。
func TestIncrementalLastChunkCrossingDeadlineKeepsTimeout(t *testing.T) {
	oldGrace := streamDeadlineGrace
	streamDeadlineGrace = 5 * time.Second
	defer func() { streamDeadlineGrace = oldGrace }()
	s := newBPStream("resp_up_lastchunk")
	s.message("最后一块跨过截止时间", 5)
	h := newIncHost(chunks(s.terminal())) // 唯一一块，Done=true
	h.emitDelay = map[int]time.Duration{2: 1300 * time.Millisecond}
	svc := NewService()
	svc.cfg.HeartbeatSeconds = intPtr(5)
	svc.cfg.TimeoutSeconds = 1
	svc.SetHost(h.call)
	if _, err := svc.execute(httpStreamRequest("last-chunk-deadline"), true); err != nil {
		t.Fatal(err)
	}
	h.waitClosed(t)
	h.mu.Lock()
	closeErr, raw := h.closeErr, string(h.emitted)
	h.mu.Unlock()
	if !strings.Contains(fmt.Sprint(closeErr), "timed out") || strings.Contains(raw, "response.completed") {
		t.Fatalf("recorded timeout was ignored: closeErr=%v completed=%t", closeErr, strings.Contains(raw, "response.completed"))
	}
}

// 截止看门狗触发时插件已停止：按 shutdown 静默强关（不带 error），不报成超时。
func TestStreamSessionDeadlineAfterShutdownClosesSilently(t *testing.T) {
	oldGrace, oldForce := streamDeadlineGrace, shutdownForceGrace
	streamDeadlineGrace, shutdownForceGrace = 10*time.Millisecond, time.Hour
	defer func() { streamDeadlineGrace, shutdownForceGrace = oldGrace, oldForce }()
	h := newSessionHost()
	ss := sessionService(h).newStreamSession("s", 0, nil)
	defer ss.markEnded() // 结束 bindLifecycle 的看门狗
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errPluginStopped)
	ss.bindLifecycle(ctx)
	ss.bindDeadline(time.Now(), timeoutError(defaultConfig()))
	select {
	case <-h.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("deadline watchdog did not close")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closes != 1 || h.closeErr != nil {
		t.Fatalf("shutdown must win over timeout: closes=%d err=%v", h.closes, h.closeErr)
	}
}

// 强关进行中（已赢得关闭权、宿主 close 尚未返回）时正常收尾：不另行以 nil 关闭、不发 completed，
// 关闭原因保持为超时。
func TestStreamSessionAbortOwnsCloseReason(t *testing.T) {
	entered, releaseClose := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var closes int
	var closeErr any
	var emitted []byte
	host := func(method string, payload any, out any) error {
		p := payload.(map[string]any)
		switch method {
		case "host.stream.close":
			mu.Lock()
			closes++
			first := closes == 1
			closeErr = p["error"]
			mu.Unlock()
			if first {
				close(entered)
				<-releaseClose
			}
		case "host.stream.emit":
			mu.Lock()
			emitted = append(emitted, p["payload"].([]byte)...)
			mu.Unlock()
		}
		return nil
	}
	svc := NewService()
	svc.SetHost(host)
	ss := svc.newStreamSession("s", 0, nil)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseClose) }) }
	defer release() // 失败时也释放阻塞中的 close
	wait := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	aborted := make(chan struct{})
	go func() { ss.abortDownstream(timeoutError(defaultConfig())); close(aborted) }()
	wait(entered, "abort close to start")
	finished := make(chan struct{})
	go func() {
		ss.finish(map[string]any{"id": "r", "status": "completed", "output": []any{}})
		close(finished)
	}()
	time.Sleep(50 * time.Millisecond)
	release()
	wait(aborted, "abort to finish")
	wait(finished, "finish to return")
	mu.Lock()
	defer mu.Unlock()
	if closes != 1 || !strings.Contains(fmt.Sprint(closeErr), "timed out") || strings.Contains(string(emitted), "response.completed") {
		t.Fatalf("closes=%d err=%v completed=%t", closes, closeErr, strings.Contains(string(emitted), "response.completed"))
	}
}

// ---- A6 一致性：密文随终态交付（最终 item.done 与 completed.output 同值）、开场预填字段规范化 ----

// 生产 as-is 形态（无摘要增量）：reasoning 在首个 message 增量前完成（added/done/终态的阶段
// 密文值可不同）。live reasoning item.done 只校验不转发，不阻断缓冲批提交——message 增量照常
// 流式交付（v0.1.18.0 曾因密文比对失败静默丢失），终态回放给出带 ENC2 的完整条目（与 completed.output 同值）。
func TestIncrementalReasoningDoneConsumedThenMessageDeltasCommit(t *testing.T) {
	text := "The smallest positive integer is 423."
	const id = "rs_no_summary"
	s := newBPStream("resp_up_no_summary")
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": id, "content": []any{}, "summary": []any{}, "encrypted_content": "ENC0"}})
	s.add("response.output_item.done", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": id, "content": []any{}, "summary": []any{}, "encrypted_content": "ENC1"}})
	s.add("response.output_item.added", map[string]any{"output_index": 1, "item": map[string]any{"type": "message", "id": "msg_no_summary", "status": "in_progress", "content": []any{}, "role": "assistant"}})
	s.add("response.content_part.added", map[string]any{"output_index": 1, "content_index": 0, "item_id": "msg_no_summary", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
	for _, d := range splitText(text, 4) {
		s.add("response.output_text.delta", map[string]any{"output_index": 1, "content_index": 0, "item_id": "msg_no_summary", "delta": d})
	}
	msgPart := map[string]any{"type": "output_text", "text": text, "annotations": []any{}}
	s.add("response.output_text.done", map[string]any{"output_index": 1, "content_index": 0, "item_id": "msg_no_summary", "text": text})
	s.add("response.content_part.done", map[string]any{"output_index": 1, "content_index": 0, "item_id": "msg_no_summary", "part": msgPart})
	s.add("response.output_item.done", map[string]any{"output_index": 1, "item": map[string]any{"type": "message", "id": "msg_no_summary", "status": "completed", "content": []any{msgPart}, "role": "assistant"}})
	finalReasoning := map[string]any{"type": "reasoning", "id": id, "content": []any{}, "summary": []any{}, "encrypted_content": "ENC2"}
	finalMessage := map[string]any{"type": "message", "id": "msg_no_summary", "status": "completed", "content": []any{msgPart}, "role": "assistant"}
	h := newIncHost(chunks(s.terminal(finalReasoning, finalMessage), 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("normal close expected, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		for _, e := range events {
			if e["type"] == "response.failed" {
				t.Fatalf("unexpected failure: %s", jsonBytes(e))
			}
		}
		t.Fatalf("want completed once, got %v", eventTypes(events))
	}
	if got := streamedText(events); got != text {
		t.Fatalf("message deltas must be delivered live exactly once: %q", got)
	}
	if countType(events, "response.reasoning_summary_text.delta") != 0 {
		t.Fatalf("no summary increments in this shape: %v", eventTypes(events))
	}
	doneReasoning := 0
	for _, e := range events {
		if e["type"] == "response.output_item.done" && stringValue(objectValue(e["item"])["type"]) == "reasoning" {
			doneReasoning++
			if objectValue(e["item"])["encrypted_content"] != "ENC2" {
				t.Fatalf("reasoning done must carry the final ciphertext: %v", e)
			}
		}
	}
	if doneReasoning != 1 {
		t.Fatalf("reasoning done count=%d want 1: %v", doneReasoning, eventTypes(events))
	}
	raw := h.snapshot()
	if strings.Contains(raw, "ENC0") || strings.Contains(raw, "ENC1") {
		t.Fatal("live stage ciphertexts leaked to client")
	}
}

// 终态条目缺失/空字符串密文时照原样交付：绝不从 added/done 的瞬时值合成或改写密文。
func TestIncrementalReasoningFinalCipherMissingStaysAsIs(t *testing.T) {
	variants := []struct {
		name string
		set  func(item map[string]any)
		want func(item map[string]any) bool
	}{
		{"missing", func(map[string]any) {}, func(item map[string]any) bool {
			_, has := item["encrypted_content"]
			return !has
		}},
		{"empty", func(item map[string]any) { item["encrypted_content"] = "" }, func(item map[string]any) bool {
			value, has := item["encrypted_content"]
			return has && value == ""
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			summary := "终态缺密文时照原样交付的摘要。"
			id := "rs_final_cipher_" + variant.name
			s := newBPStream("resp_up_final_cipher_" + variant.name)
			s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": id, "summary": []any{}, "encrypted_content": "ENC0"}})
			s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
			s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "delta": summary})
			s.add("response.reasoning_summary_text.done", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "text": summary})
			s.add("response.reasoning_summary_part.done", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": summary}})
			final := map[string]any{"type": "reasoning", "id": id, "summary": []any{map[string]any{"type": "summary_text", "text": summary}}}
			variant.set(final)
			s.add("response.output_item.done", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": id, "summary": []any{map[string]any{"type": "summary_text", "text": summary}}, "encrypted_content": "ENC1"}})
			blocks := s.terminal(final)
			cut := indexOfType(blocks, "response.reasoning_summary_text.delta", 1) + 1
			h := newIncHost(chunks(blocks, cut))
			startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
			h.waitClosed(t)
			if h.closeErr != nil {
				t.Fatalf("normal close expected, got %v", h.closeErr)
			}
			events := clientStreamEvents(t, []byte(h.snapshot()))
			if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
				t.Fatalf("want completed once, got %v", eventTypes(events))
			}
			if got := streamedSummary(events); got != summary {
				t.Fatalf("summary duplicated or missing: %q", got)
			}
			doneSeen := false
			for _, e := range events {
				if e["type"] != "response.output_item.done" || stringValue(objectValue(e["item"])["type"]) != "reasoning" {
					continue
				}
				doneSeen = true
				if !variant.want(objectValue(e["item"])) {
					t.Fatalf("terminal done must carry the final item as-is: %v", e)
				}
			}
			if !doneSeen {
				t.Fatalf("missing terminal reasoning done: %v", eventTypes(events))
			}
			finalOutput, _ := terminalEvent(t, events)["output"].([]any)
			if len(finalOutput) != 1 || !variant.want(objectValue(finalOutput[0])) {
				t.Fatalf("completed.output must be unchanged: %s", jsonBytes(finalOutput))
			}
			if raw := h.snapshot(); strings.Contains(raw, "ENC0") || strings.Contains(raw, "ENC1") {
				t.Fatal("stage ciphertexts must not be synthesized into any frame")
			}
		})
	}
}

// live added 帧无论是否携带密文（含缺失/null/空变体）都统一剥离；密文由终态条目的最终
// item.done 交付（恰好一次，与 completed.output 同值），三种变体都正常完成一次。
func TestIncrementalAddedEmptyCiphertextCompletedAtDoneOrFinal(t *testing.T) {
	variants := []struct {
		name string
		set  func(item map[string]any)
	}{
		{"missing", func(map[string]any) {}},
		{"null", func(item map[string]any) { item["encrypted_content"] = nil }},
		{"empty", func(item map[string]any) { item["encrypted_content"] = "" }},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			summary := "补全的摘要。"
			id := "rs_enc_" + variant.name
			s := newBPStream("resp_up_enc_" + variant.name)
			addedItem := map[string]any{"type": "reasoning", "id": id, "summary": []any{}}
			variant.set(addedItem)
			s.add("response.output_item.added", map[string]any{"output_index": 0, "item": addedItem})
			s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
			s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "delta": summary})
			s.add("response.reasoning_summary_text.done", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "text": summary})
			s.add("response.reasoning_summary_part.done", map[string]any{"output_index": 0, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": summary}})
			item := map[string]any{"type": "reasoning", "id": id, "summary": []any{map[string]any{"type": "summary_text", "text": summary}}, "encrypted_content": "enc-complete"}
			s.add("response.output_item.done", map[string]any{"output_index": 0, "item": item})
			blocks := s.terminal(item)
			cut := indexOfType(blocks, "response.reasoning_summary_text.delta", 1) + 1
			h := newIncHost(chunks(blocks, cut))
			startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
			h.waitClosed(t)
			if h.closeErr != nil {
				t.Fatalf("normal close expected, got %v", h.closeErr)
			}
			events := clientStreamEvents(t, []byte(h.snapshot()))
			if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
				t.Fatalf("want completed once, got %v", eventTypes(events))
			}
			if got := streamedSummary(events); got != summary {
				t.Fatalf("summary duplicated or missing: %q", got)
			}
			doneSeen := false
			for _, e := range events {
				if e["type"] == "response.output_item.done" {
					if item := objectValue(e["item"]); stringValue(item["type"]) == "reasoning" && item["encrypted_content"] == "enc-complete" {
						doneSeen = true
					}
				}
			}
			if !doneSeen {
				t.Fatalf("done must supply the completed ciphertext: %v", eventTypes(events))
			}
			for kind, want := range map[string]int{
				"response.reasoning_summary_part.added": 1,
				"response.reasoning_summary_text.done":  1,
				"response.reasoning_summary_part.done":  1,
				"response.output_item.done":             1,
				"response.completed":                    1,
			} {
				if n := countType(events, kind); n != want {
					t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
				}
			}
		})
	}
}

// HTTP 已提交增量：added/part.added 的预填摘要文本被规范化为空开场（身份保留、密文剥离），
// 正文只经 delta 累计一次，生命周期恰好一次。
func TestIncrementalReasoningPrefilledOpeningFieldsNormalized(t *testing.T) {
	summary := "第一段第二段"
	s := newBPStream("resp_up_prefill")
	s.add("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"type": "reasoning", "id": "rs_prefill", "summary": []any{map[string]any{"type": "summary_text", "text": "预填摘要不得外发"}}, "encrypted_content": "enc-prefill"}})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 0, "item_id": "rs_prefill", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": "预填部件不得外发"}})
	s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": "rs_prefill", "summary_index": 0, "delta": "第一段"})
	s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 0, "item_id": "rs_prefill", "summary_index": 0, "delta": "第二段"})
	s.add("response.reasoning_summary_text.done", map[string]any{"output_index": 0, "item_id": "rs_prefill", "summary_index": 0, "text": summary})
	s.add("response.reasoning_summary_part.done", map[string]any{"output_index": 0, "item_id": "rs_prefill", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": summary}})
	item := map[string]any{"type": "reasoning", "id": "rs_prefill", "summary": []any{map[string]any{"type": "summary_text", "text": summary}}, "encrypted_content": "enc-prefill"}
	s.add("response.output_item.done", map[string]any{"output_index": 0, "item": item})
	blocks := s.terminal(item)
	cut := indexOfType(blocks, "response.reasoning_summary_text.delta", 1) + 1
	h := newIncHost(chunks(blocks, cut))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("normal close expected, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedSummary(events); got != summary {
		t.Fatalf("summary duplicated or missing (prefill leaked into display?):\n got %q\nwant %q", got, summary)
	}
	var addedSeen, partSeen bool
	for _, e := range events {
		switch e["type"] {
		case "response.output_item.added":
			opened := objectValue(e["item"])
			if stringValue(opened["type"]) != "reasoning" {
				continue
			}
			addedSeen = true
			if summaryAny, _ := opened["summary"].([]any); len(summaryAny) != 0 {
				t.Fatalf("reasoning added must start with empty summary: %v", e)
			}
			if opened["id"] != "rs_prefill" {
				t.Fatalf("identity must be preserved in opening: %v", e)
			}
			if _, has := opened["encrypted_content"]; has {
				t.Fatalf("live opening must not carry ciphertext: %v", e)
			}
		case "response.reasoning_summary_part.added":
			partSeen = true
			part := objectValue(e["part"])
			if stringValue(part["type"]) != "summary_text" || part["text"] != "" {
				t.Fatalf("summary part must open with empty text: %v", e)
			}
		}
	}
	if !addedSeen || !partSeen {
		t.Fatalf("opening events missing: %v", eventTypes(events))
	}
	raw := h.snapshot()
	if strings.Contains(raw, "预填摘要不得外发") || strings.Contains(raw, "预填部件不得外发") {
		t.Fatal("prefilled opening text leaked to client")
	}
	for kind, want := range map[string]int{
		"response.output_item.added":            1,
		"response.reasoning_summary_part.added": 1,
		"response.reasoning_summary_text.done":  1,
		"response.reasoning_summary_part.done":  1,
		"response.output_item.done":             1,
		"response.completed":                    1,
	} {
		if n := countType(events, kind); n != want {
			t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
		}
	}
}

// 正文先 commit、随后才出现带预填的 reasoning 开场：归一化同样生效，摘要只交付一次。
func TestIncrementalMessageCommitThenReasoningPrefilledNormalized(t *testing.T) {
	summary := "迟到的摘要。"
	text := "先到的正文。"
	s := newBPStream("resp_up_text_then_reason")
	s.message(text, 2)
	s.add("response.output_item.added", map[string]any{"output_index": 1, "item": map[string]any{"type": "reasoning", "id": "rs_late", "summary": []any{map[string]any{"type": "summary_text", "text": "迟到预填摘要"}}}})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": 1, "item_id": "rs_late", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": "迟到预填部件"}})
	s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": 1, "item_id": "rs_late", "summary_index": 0, "delta": summary})
	s.add("response.reasoning_summary_text.done", map[string]any{"output_index": 1, "item_id": "rs_late", "summary_index": 0, "text": summary})
	s.add("response.reasoning_summary_part.done", map[string]any{"output_index": 1, "item_id": "rs_late", "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": summary}})
	item := map[string]any{"type": "reasoning", "id": "rs_late", "summary": []any{map[string]any{"type": "summary_text", "text": summary}}}
	s.add("response.output_item.done", map[string]any{"output_index": 1, "item": item})
	s.output = append(s.output, item)
	blocks := s.terminal()
	cut := indexOfType(blocks, "response.output_text.delta", 1) + 1
	h := newIncHost(chunks(blocks, cut))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("normal close expected, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if got := streamedText(events); got != text {
		t.Fatalf("text duplicated or missing: %q", got)
	}
	if got := streamedSummary(events); got != summary {
		t.Fatalf("late reasoning summary duplicated or missing: %q", got)
	}
	var addedSeen, partSeen bool
	for _, e := range events {
		switch e["type"] {
		case "response.output_item.added":
			opened := objectValue(e["item"])
			if stringValue(opened["type"]) != "reasoning" {
				continue
			}
			addedSeen = true
			if summaryAny, _ := opened["summary"].([]any); len(summaryAny) != 0 {
				t.Fatalf("late reasoning added must start with empty summary: %v", e)
			}
		case "response.reasoning_summary_part.added":
			partSeen = true
			part := objectValue(e["part"])
			if stringValue(part["type"]) != "summary_text" || part["text"] != "" {
				t.Fatalf("late summary part must open with empty text: %v", e)
			}
		}
	}
	if !addedSeen || !partSeen {
		t.Fatalf("late reasoning opening events missing: %v", eventTypes(events))
	}
	raw := h.snapshot()
	if strings.Contains(raw, "迟到预填摘要") || strings.Contains(raw, "迟到预填部件") {
		t.Fatal("prefilled late opening text leaked to client")
	}
	for kind, want := range map[string]int{
		"response.output_item.added":            2,
		"response.output_item.done":             2,
		"response.reasoning_summary_part.added": 1,
		"response.completed":                    1,
	} {
		if n := countType(events, kind); n != want {
			t.Fatalf("%s count=%d want %d: %v", kind, n, want, eventTypes(events))
		}
	}
}

// ---- owner 复核修订的常驻回归：摘要增量提交前提缺失的安全回退与按非空内容记交付 ----

// 缺 response.reasoning_summary_part.added（上游违反开启顺序）：未提交路径不交付摘要增量、
// 不报流内错误，整轮退回终态完整回放（legacy 形状的 reasoning item 携带完整摘要），
// 汇总的两个交付口径保持 false 且以 completed 结束。
func TestIncrementalMissingSummaryPartFallsBackToTerminalReplay(t *testing.T) {
	s := newBPStream("resp_missing_part")
	s.reasoning("summary fallback", 1)
	var blocks []string
	for _, b := range s.terminal() {
		if strings.HasPrefix(b, "event: response.reasoning_summary_part.added\n") {
			continue
		}
		blocks = append(blocks, b)
	}
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	if h.closeErr != nil {
		t.Fatalf("fallback must close normally, got %v", h.closeErr)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("missing precommit part must fall back to terminal replay, got %v", eventTypes(events))
	}
	if countType(events, "response.reasoning_summary_text.delta") != 0 || streamedSummary(events) != "" {
		t.Fatal("uncommitted fallback must not deliver summary deltas")
	}
	if !strings.Contains(h.snapshot(), "summary fallback") {
		t.Fatal("terminal replay must carry the full summary")
	}
	rec := h.waitSummary(t)
	if rec.SummaryCommitted || rec.TextCommitted || rec.Delivery != "incremental" || rec.Exit != "completed" {
		t.Fatalf("fallback summary wrong: %+v", rec)
	}
}

// 缺 reasoning 的 response.output_item.added：同样的未提交安全回退，delta 的后到事件不能
// 补齐缺失的开场。
func TestIncrementalMissingReasoningItemFallsBackToTerminalReplay(t *testing.T) {
	s := newBPStream("resp_missing_item")
	s.reasoning("hidden item summary", 1)
	var blocks []string
	for _, b := range s.terminal() {
		if strings.HasPrefix(b, "event: response.output_item.added\n") && strings.Contains(b, `"reasoning"`) {
			continue
		}
		blocks = append(blocks, b)
	}
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("missing reasoning item must fall back to terminal replay, got %v", eventTypes(events))
	}
	if countType(events, "response.reasoning_summary_text.delta") != 0 || !strings.Contains(h.snapshot(), "hidden item summary") {
		t.Fatal("terminal replay must carry the full summary without deltas")
	}
	rec := h.waitSummary(t)
	if rec.SummaryCommitted || rec.TextCommitted || rec.Exit != "completed" {
		t.Fatalf("fallback summary wrong: %+v", rec)
	}
}

// 缺 part.added 的摘要之后跟随正文与工具的混合回合：整轮（摘要、正文、工具）经终态完整
// 回放交付，正文恰好一次、后部工具照常转换，提交前的冲突不触发重新生成。
func TestIncrementalMissingSummaryPartMixedTurnReplaysAtTerminal(t *testing.T) {
	good, _ := relayFixture("call_missing_part_mixed", false)
	s := newBPStream("resp_missing_part_mixed")
	s.reasoning("mixed summary", 1)
	s.message("混合回合正文。", 3)
	s.tool(good, 2)
	var blocks []string
	for _, b := range s.terminal() {
		if strings.HasPrefix(b, "event: response.reasoning_summary_part.added\n") {
			continue
		}
		blocks = append(blocks, b)
	}
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, relaySource())
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("mixed turn must complete: %v", eventTypes(events))
	}
	if got := streamedText(events); got != "混合回合正文。" || countType(events, "response.output_text.delta") != 1 {
		t.Fatalf("text must be replayed exactly once at terminal: %q", got)
	}
	if countType(events, "response.reasoning_summary_text.delta") != 0 || streamedSummary(events) != "" {
		t.Fatal("uncommitted fallback must not deliver summary deltas")
	}
	if !strings.Contains(h.snapshot(), "mixed summary") || !strings.Contains(h.snapshot(), "Begin Patch") {
		t.Fatal("terminal replay must carry the full summary and the client tool call")
	}
	rec := h.waitSummary(t)
	if rec.TextCommitted || rec.SummaryCommitted || rec.Exit != "completed" || rec.First != relayLegacy || rec.Final != relayLegacy {
		t.Fatalf("mixed fallback summary wrong: %+v", rec)
	}
}

// buffered 边界：暂不交付期间摘要缺 part.added 属于流内协议冲突，整轮直接失败、不重新生成、
// 不对外交付任何增量（与终态与缓冲批不一致的处理一致）。
func TestBufferedMissingSummaryPartFailsWithoutRegeneration(t *testing.T) {
	s := newBPStream("resp_buf_missing_part")
	s.reasoning("strict summary", 1)
	var blocks []string
	for _, b := range s.terminal() {
		if strings.HasPrefix(b, "event: response.reasoning_summary_part.added\n") {
			continue
		}
		blocks = append(blocks, b)
	}
	h := newIncHost(chunks(blocks, 3))
	startBuffered(t, h, 5, relaySource())
	h.waitClosed(t)
	if h.nextID != 1 {
		t.Fatalf("buffered stream conflict must not be regenerated, attempts=%d", h.nextID)
	}
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if streamedText(events) != "" || countType(events, "response.failed") != 1 || countType(events, "response.completed") != 0 {
		t.Fatalf("want response.failed only, got %v", eventTypes(events))
	}
	if !strings.Contains(h.snapshot(), "invalid_upstream_stream") || streamedSummary(events) != "" {
		t.Fatal("buffered conflict must surface as invalid_upstream_stream without summary deltas")
	}
	rec := h.waitSummary(t)
	if rec.Exit != "failed" || rec.TextCommitted || rec.SummaryCommitted {
		t.Fatalf("buffered failure summary wrong: %+v", rec)
	}
}

// 空摘要增量不构成摘要交付：注入 0 长度 summary delta 后正文回合提交，summary_committed
// 必须保持 false、text_committed 为 true；空 delta 事件本身仍合法交付。
func TestIncrementalEmptySummaryDeltaDoesNotMarkSummaryCommitted(t *testing.T) {
	s := newBPStream("resp_empty_summary_delta")
	s.reasoning("", 0)
	s.message("text", 1)
	var blocks []string
	for _, b := range s.terminal() {
		blocks = append(blocks, b)
		if strings.HasPrefix(b, "event: response.reasoning_summary_part.added\n") {
			blocks = append(blocks, "event: response.reasoning_summary_text.delta\ndata: "+string(jsonBytes(map[string]any{"type": "response.reasoning_summary_text.delta", "output_index": 0, "summary_index": 0, "item_id": "rs_0", "delta": ""}))+"\n\n")
		}
	}
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("stream invalid: %v", eventTypes(events))
	}
	if streamedText(events) != "text" {
		t.Fatalf("text %q", streamedText(events))
	}
	rec := h.waitSummary(t)
	if rec.SummaryCommitted || !rec.TextCommitted {
		t.Fatalf("empty summary delta must not mark delivery: %+v", rec)
	}
}

// 反向组合：非空摘要增量 + 0 长度正文增量只记摘要交付，text_committed 保持 false。
func TestIncrementalEmptyTextDeltaDoesNotMarkTextCommitted(t *testing.T) {
	s := newBPStream("resp_empty_text_delta")
	s.reasoning("摘要内容。", 1)
	s.message("", 0)
	var blocks []string
	for _, b := range s.terminal() {
		blocks = append(blocks, b)
		if strings.HasPrefix(b, "event: response.content_part.added\n") && strings.Contains(b, `"output_text"`) {
			blocks = append(blocks, "event: response.output_text.delta\ndata: "+string(jsonBytes(map[string]any{"type": "response.output_text.delta", "output_index": 1, "content_index": 0, "item_id": "msg_1", "delta": ""}))+"\n\n")
		}
	}
	h := newIncHost(chunks(blocks, 4))
	startIncremental(t, h, 5, map[string]any{"model": DefaultModelID, "input": "hi", "stream": true})
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	if countType(events, "response.completed") != 1 || countType(events, "response.failed") != 0 {
		t.Fatalf("stream invalid: %v", eventTypes(events))
	}
	if streamedText(events) != "" || streamedSummary(events) != "摘要内容。" {
		t.Fatalf("delivery content wrong: text=%q summary=%q", streamedText(events), streamedSummary(events))
	}
	rec := h.waitSummary(t)
	if rec.TextCommitted || !rec.SummaryCommitted {
		t.Fatalf("empty text delta must not mark delivery: %+v", rec)
	}
}
