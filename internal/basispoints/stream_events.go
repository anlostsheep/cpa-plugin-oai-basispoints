package basispoints

import (
	"bytes"
	"encoding/json"
	"strings"
)

// 增量交付状态机：只提前交付普通消息正文；工具调用（名称与参数）、推理与推理摘要等一律等待
// 完整终态，由 transformResponseBody 整批转换校验后随终态回放。
//
// 移植自上游 JaxsonWang/cpa-plugin-oai-basispoints v0.1.18（11df6f8，stream_events.go，MIT），
// 按 fork 的结构改造：
//   - 不自行写流：commit 与之后的消息事件经 forward 回调交给 streamSession，由会话统一
//     分配 sequence_number、决定客户端 response id、处理断开与停止；
//   - 流内失败事件交给 classifyUpstreamFailure，保留 401/403/429 以便 CPA 冷却或换号；
//   - 不发 error 事件收尾：失败统一由 streamSession.fail 处理（请求级 → response.failed，
//     凭据/传输类 → 带 error 关流）；
//   - finish 改为对 syntheticEvents 的过滤（keepFinal），只补发未交付的部分。

type streamedPart struct {
	kind     string
	text     strings.Builder
	logs     []any
	textDone bool
	done     bool
}

type streamedMessage struct {
	id    string
	parts map[int]*streamedPart
	done  bool
}

type streamDelivery struct {
	// onMeta 在首次得知上游 response id 时调用（可为 nil），供会话以上游 id 提前开流。
	onMeta func(meta map[string]any)
	// forward 交付消息事件；commit 时带上 meta（会话尚未开流则据此开场），之后 meta 为 nil。
	forward func(meta map[string]any, frames []map[string]any) error

	committed     bool
	meta          map[string]any
	pending       []map[string]any
	knownMessages map[int]string
	knownParts    map[[2]int]bool
	messages      map[int]*streamedMessage
	terminal      bool
	sentinel      bool
	events        int // 已解码的上游事件数；用于识别「有 SSE 事件却始终未 commit」
}

func newStreamDelivery(onMeta func(map[string]any), forward func(map[string]any, []map[string]any) error) *streamDelivery {
	return &streamDelivery{onMeta: onMeta, forward: forward, knownMessages: map[int]string{}, knownParts: map[[2]int]bool{}, messages: map[int]*streamedMessage{}}
}

func streamEventError() error {
	return fail(502, "invalid_upstream_stream", "Basis Points stream contains inconsistent message events")
}

// streamIndex 读取上游 SSE 中的序号（parseRelayObject 以 json.Number 解码）。
func streamIndex(value any) (int, error) {
	n, ok := value.(json.Number)
	if !ok {
		return 0, streamEventError()
	}
	i, err := n.Int64()
	if err != nil || i < 0 || i > 1<<30 {
		return 0, streamEventError()
	}
	return int(i), nil
}

// eventIndex 读取 syntheticEvents 生成的序号（Go int）或上游解码的 json.Number。
func eventIndex(value any) (int, bool) {
	switch n := value.(type) {
	case int:
		return n, n >= 0
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil && i >= 0 && i <= 1<<30
	}
	return 0, false
}

func (d *streamDelivery) consume(event, data string) error {
	d.events++
	if strings.TrimSpace(data) == "[DONE]" {
		d.sentinel = true
		return nil
	}
	value, reason := parseRelayObject(data)
	if reason != "" {
		return fail(502, "invalid_upstream_response", "Basis Points returned invalid SSE JSON")
	}
	if d.terminal || d.sentinel {
		return streamEventError()
	}
	kind := stringValue(value["type"])
	if kind == "" {
		kind = event
		value["type"] = kind
	}
	if isUpstreamFailureEvent(kind) {
		return classifyUpstreamFailure(value, kind)
	}
	switch kind {
	case "response.completed", "response.incomplete":
		d.terminal = true
		return nil
	case "response.created", "response.in_progress":
		meta := objectValue(value["response"])
		if d.meta != nil && meta["id"] != d.meta["id"] {
			return streamEventError()
		}
		if d.meta == nil && stringValue(meta["id"]) != "" {
			d.meta = cloneObject(meta)
			if d.onMeta != nil {
				d.onMeta(cloneObject(d.meta))
			}
		}
		return nil
	case "response.output_item.added", "response.output_item.done":
		item := objectValue(value["item"])
		if stringValue(item["type"]) != "message" {
			return nil
		}
		index, err := streamIndex(value["output_index"])
		if err != nil {
			return err
		}
		if kind == "response.output_item.added" {
			d.knownMessages[index] = stringValue(item["id"])
		}
	case "response.content_part.added", "response.content_part.done", "response.output_text.delta", "response.output_text.done":
		index, err := streamIndex(value["output_index"])
		if err != nil {
			return err
		}
		part, err := streamIndex(value["content_index"])
		if err != nil {
			return err
		}
		if kind == "response.content_part.added" {
			d.knownParts[[2]int{index, part}] = stringValue(objectValue(value["part"])["type"]) == "output_text"
		}
	default:
		// reasoning / reasoning_summary、工具参数增量及其他 item 由完整终态保留，不作为正文提前暴露。
		return nil
	}
	if d.committed {
		frame, err := d.applyMessageEvent(value)
		if err != nil {
			return err
		}
		return d.forward(nil, []map[string]any{frame})
	}
	d.pending = append(d.pending, value)
	delta, _ := value["delta"].(string)
	if kind != "response.output_text.delta" || delta == "" || d.meta == nil {
		return nil
	}
	index, _ := streamIndex(value["output_index"])
	part, _ := streamIndex(value["content_index"])
	if d.knownMessages[index] == "" || d.knownMessages[index] != stringValue(value["item_id"]) || !d.knownParts[[2]int{index, part}] {
		return nil
	}
	frames := make([]map[string]any, 0, len(d.pending))
	for _, item := range d.pending {
		frame, err := d.applyMessageEvent(item)
		if err != nil {
			return err
		}
		frames = append(frames, frame)
	}
	d.pending = nil
	d.committed = true
	return d.forward(cloneObject(d.meta), frames)
}

func (d *streamDelivery) applyMessageEvent(value map[string]any) (map[string]any, error) {
	index, err := streamIndex(value["output_index"])
	if err != nil {
		return nil, err
	}
	kind := stringValue(value["type"])
	frame := cloneObject(value)
	m := d.messages[index]
	if kind == "response.output_item.added" {
		item := objectValue(value["item"])
		id := stringValue(item["id"])
		if m != nil || id == "" {
			return nil, streamEventError()
		}
		d.messages[index] = &streamedMessage{id: id, parts: map[int]*streamedPart{}}
		added := cloneObject(item)
		added["status"], added["content"] = "in_progress", []any{}
		frame["item"] = added
		return frame, nil
	}
	if m == nil || m.done {
		return nil, streamEventError()
	}
	if kind == "response.output_item.done" {
		item := objectValue(value["item"])
		content, _ := item["content"].([]any)
		if item["id"] != m.id || len(content) != len(m.parts) {
			return nil, streamEventError()
		}
		for i, part := range m.parts {
			final := objectValue(content[i])
			if !part.done || (part.kind == "output_text" && (final["text"] != part.text.String() || !logprobsMatch(final["logprobs"], part.logs))) {
				return nil, streamEventError()
			}
		}
		m.done = true
		return frame, nil
	}
	if value["item_id"] != m.id {
		return nil, streamEventError()
	}
	partIndex, err := streamIndex(value["content_index"])
	if err != nil {
		return nil, err
	}
	part := m.parts[partIndex]
	if kind == "response.content_part.added" {
		if part != nil || partIndex != len(m.parts) || (partIndex > 0 && !m.parts[partIndex-1].done) {
			return nil, streamEventError()
		}
		added := cloneObject(objectValue(value["part"]))
		part = &streamedPart{kind: stringValue(added["type"])}
		m.parts[partIndex] = part
		if part.kind == "output_text" {
			added["text"] = ""
			if _, exists := added["logprobs"]; exists {
				added["logprobs"] = []any{}
			}
		}
		frame["part"] = added
		return frame, nil
	}
	if part == nil || part.done {
		return nil, streamEventError()
	}
	switch kind {
	case "response.output_text.delta":
		text, ok := value["delta"].(string)
		if !ok || part.kind != "output_text" || part.textDone {
			return nil, streamEventError()
		}
		part.text.WriteString(text)
		logs, _ := value["logprobs"].([]any)
		part.logs = append(part.logs, logs...)
		if logs == nil {
			frame["logprobs"] = []any{}
		}
	case "response.output_text.done":
		if part.kind != "output_text" || part.textDone || value["text"] != part.text.String() || !logprobsMatch(value["logprobs"], part.logs) {
			return nil, streamEventError()
		}
		part.textDone = true
	case "response.content_part.done":
		completed := objectValue(value["part"])
		if stringValue(completed["type"]) != part.kind || (part.kind == "output_text" && (!part.textDone || completed["text"] != part.text.String() || !logprobsMatch(completed["logprobs"], part.logs))) {
			return nil, streamEventError()
		}
		part.done = true
	}
	return frame, nil
}

// logprobsMatch 判断事件/终态里的 logprobs 与已累计交付的是否一致；缺失、null 与空数组
// 都视为「没有 logprobs」，其他类型视为不一致。
func logprobsMatch(value any, logs []any) bool {
	if value == nil {
		// 事件未携带 logprobs：没有向客户端交付冲突数据，允许。
		return true
	}
	got, ok := value.([]any)
	if !ok {
		return false
	}
	if len(got) == 0 && len(logs) == 0 {
		return true
	}
	return bytes.Equal(jsonBytes(got), jsonBytes(logs))
}

// validateFinal 核对上游原始终态与已交付正文一致（在 transformResponseBody 之前调用，
// 避免工具整批校验把不一致的终态写入历史身份缓存）。未 commit 时无需核对。
func (d *streamDelivery) validateFinal(response map[string]any) error {
	if !d.committed {
		return nil
	}
	if response["id"] != d.meta["id"] {
		return streamEventError()
	}
	output, _ := response["output"].([]any)
	for index, message := range d.messages {
		if index >= len(output) {
			return streamEventError()
		}
		item := objectValue(output[index])
		if item["type"] != "message" || item["id"] != message.id {
			return streamEventError()
		}
		content, _ := item["content"].([]any)
		if message.done && len(content) != len(message.parts) {
			return streamEventError()
		}
		for i, part := range message.parts {
			if i >= len(content) {
				return streamEventError()
			}
			finalPart := objectValue(content[i])
			if finalPart["type"] != part.kind {
				return streamEventError()
			}
			if part.kind == "output_text" {
				text, ok := finalPart["text"].(string)
				if !ok || !strings.HasPrefix(text, part.text.String()) || (part.textDone && text != part.text.String()) {
					return streamEventError()
				}
				logs, _ := finalPart["logprobs"].([]any)
				if len(part.logs) > len(logs) || !bytes.Equal(jsonBytes(part.logs), jsonBytes(logs[:len(part.logs)])) {
					if len(part.logs) > 0 {
						return streamEventError()
					}
				}
				// done 已交付给客户端时，终态 logprobs 不能再多出或不同。
				if part.textDone && !logprobsMatch(finalPart["logprobs"], part.logs) {
					return streamEventError()
				}
			}
		}
	}
	return nil
}

// keepFinal 过滤终态回放（syntheticEvents(…, false)）：跳过已交付的消息事件，正文只补发
// 未交付的后缀；工具、推理 item 与终止事件照常保留。会就地改写被截短的 delta。
func (d *streamDelivery) keepFinal(ev *sseEvent) bool {
	kind := ev.name
	if kind == "response.created" || kind == "response.in_progress" {
		return false
	}
	index, ok := eventIndex(ev.value["output_index"])
	message := d.messages[index]
	if !ok || message == nil {
		return true
	}
	if kind == "response.output_item.added" || (kind == "response.output_item.done" && message.done) {
		return false
	}
	partIndex, ok := eventIndex(ev.value["content_index"])
	part := message.parts[partIndex]
	if !ok || part == nil {
		return true
	}
	switch kind {
	case "response.content_part.added":
		return false
	case "response.output_text.delta":
		full, _ := ev.value["delta"].(string)
		sent := part.text.Len()
		if len(full) <= sent {
			return false
		}
		ev.value["delta"] = full[sent:]
		if logs, ok := ev.value["logprobs"].([]any); ok && len(logs) >= len(part.logs) {
			ev.value["logprobs"] = logs[len(part.logs):]
		}
	case "response.output_text.done":
		return !part.textDone
	case "response.content_part.done":
		return !part.done
	}
	return true
}
