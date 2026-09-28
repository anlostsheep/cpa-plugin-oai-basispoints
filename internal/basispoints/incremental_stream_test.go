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
	idx, id := len(s.output), fmt.Sprintf("rs_%d", len(s.output))
	item := map[string]any{"type": "reasoning", "id": id, "summary": []any{map[string]any{"type": "summary_text", "text": summary}}, "encrypted_content": "enc-synthetic"}
	s.add("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "reasoning", "id": id, "summary": []any{}}})
	s.add("response.reasoning_summary_part.added", map[string]any{"output_index": idx, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": ""}})
	for _, d := range splitText(summary, deltas) {
		s.add("response.reasoning_summary_text.delta", map[string]any{"output_index": idx, "item_id": id, "summary_index": 0, "delta": d})
	}
	s.add("response.reasoning_summary_text.done", map[string]any{"output_index": idx, "item_id": id, "summary_index": 0, "text": summary})
	s.add("response.reasoning_summary_part.done", map[string]any{"output_index": idx, "item_id": id, "summary_index": 0, "part": map[string]any{"type": "summary_text", "text": summary}})
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

// 夹具 A（场景3 第1轮）：推理 → 正文 → 工具。正文在终态前到达；推理、工具只在终态后回放。
func TestIncrementalTextArrivesBeforeTerminal(t *testing.T) {
	text := "表格已检查，下面把第 3 行的公式改成求和，并保留原有格式。"
	call, _ := relayFixture("call_scene3", false)
	s := newBPStream("resp_up_scene3")
	s.reasoning("先查看工作表结构，再决定如何修改公式。", 94)
	s.message(text, 27)
	s.tool(call, 5)
	blocks := s.terminal()
	cut := indexOfType(blocks, "response.output_text.delta", 3) + 1
	h := newIncHost(chunks(blocks, cut))
	h.holdAt[1] = 1
	startIncremental(t, h, 5, relaySource())

	h.waitFor(t, "response.output_text.delta")
	early := h.snapshot()
	for _, forbidden := range []string{"response.completed", "reasoning", "function_call", "custom_tool_call"} {
		if strings.Contains(early, forbidden) {
			t.Fatalf("%s leaked before terminal:\n%s", forbidden, early)
		}
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
	if n := countType(events, "response.created"); n != 1 {
		t.Fatalf("created events = %d", n)
	}
	created := objectValue(events[0]["response"])
	if _, hasError := created["error"]; events[0]["type"] != "response.created" || created["id"] != "resp_up_scene3" || hasError {
		t.Fatalf("prologue must use upstream id without error field: %#v", events[0])
	}
	for _, e := range events {
		if strings.HasPrefix(stringValue(e["type"]), "response.reasoning") {
			t.Fatalf("reasoning event streamed: %v", e["type"])
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
	}
}

// 夹具 B（0927 第1轮）：推理 → 工具，无正文。整轮不 commit，工具在终态回放。
func TestIncrementalToolOnlyTurnReplaysAtTerminal(t *testing.T) {
	call, _ := relayFixture("call_tool_only", false)
	s := newBPStream("resp_up_tool")
	s.reasoning("只需要调用一次工具。", 100)
	s.tool(call, 57)
	blocks := s.terminal()
	h := newIncHost(chunks(blocks, len(blocks)-1))
	h.holdAt[1] = 1
	startIncremental(t, h, 5, relaySource())
	h.waitFor(t, "response.in_progress")
	if early := h.snapshot(); strings.Contains(early, "output_item") {
		t.Fatalf("tool-only turn must not stream items before terminal:\n%s", early)
	}
	close(h.release)
	h.waitClosed(t)
	events := clientStreamEvents(t, []byte(h.snapshot()))
	final := terminalEvent(t, events)
	output, _ := final["output"].([]any)
	if final["id"] != "resp_up_tool" || len(output) != 2 || objectValue(output[1])["type"] != "custom_tool_call" {
		t.Fatalf("unexpected final: %s", jsonBytes(final))
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

// 未交付正文前工具无效 → 重生成一次；客户端 id 固定为首轮开流时的上游 id。
func TestIncrementalRegenerateBeforeTextKeepsClientID(t *testing.T) {
	bad, _ := relayFixture("call_bad", true)
	good, _ := relayFixture("call_good", false)
	first := newBPStream("resp_attempt1")
	first.reasoning("尝试调用工具。", 10)
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
	if !strings.Contains(logs, "warn:basispoints: tool call invalid after text was delivered") || strings.Contains(logs, "regenerating once") {
		t.Fatalf("422-after-text counter log wrong: %s", logs)
	}
	if strings.Contains(logs, "先说明") || strings.Contains(logs, "apply_patch") {
		t.Fatalf("logs must not carry content: %s", logs)
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
	if rememberedNativeCall("call_mismatch_valid") != nil {
		t.Fatal("inconsistent terminal must be rejected before tool identities are cached")
	}
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
