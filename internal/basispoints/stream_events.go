package basispoints

import (
	"bytes"
	"encoding/json"
	"strings"
)

// 增量交付状态机：提前交付普通消息正文与推理摘要（reasoning_summary_*）；工具调用（名称与
// 参数）与其他 item 一律等待完整终态，由 transformResponseBody 整批转换校验后随终态回放。
//
// 移植自上游 JaxsonWang/cpa-plugin-oai-basispoints（v0.1.18/11df6f8 的消息部分与
// v0.2.9/2464fb9 的推理摘要部分，MIT），按 fork 的结构改造：
//   - 不自行写流：commit 与之后的消息/摘要事件经 forward 回调交给 streamSession，由会话统一
//     分配 sequence_number、决定客户端 response id、处理断开与停止；
//   - 流内失败事件交给 classifyUpstreamFailure，保留 401/403/429 以便 CPA 冷却或换号；
//   - 不发 error 事件收尾：失败统一由 streamSession.fail 处理（请求级 → response.failed，
//     凭据/传输类 → 带 error 关流）；
//   - finish 改为对专用终态回放生成器（syntheticEventsWithReasoningSummary）的过滤
//     （keepFinal），只补发未交付的正文/摘要部分；WS 与未提交 HTTP 仍用 legacy 形状。

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

// streamedReasoning 独立记录推理条目的交付进度，不登记为普通消息。只核对身份与摘要文本
// （前缀/精确）；密文不跨事件比对——脱敏捕获只证明同一 reasoning 条目的阶段快照
// （added/done/completed）密文值可不同，不能要求跨阶段字节相等（v0.1.18.0 生产事故根因）。
// 密文随终态交付：live added 帧剥离密文，live item.done 只校验不转发；终态回放的最终
// item.done 恰好一次，其密文与 completed/incomplete.output 同值。
type streamedReasoning struct {
	id        string
	summaries map[int]*streamedPart
	// upstreamDone 记录上游已发来合法的 output_item.done（只校验并做 doneSummary 快照，
	// 不转发；重复的 done 被拒绝）。终态回放的最终 item.done 恒保留：恰好一次，
	// 携带与 completed/incomplete.output 同值的密文。
	upstreamDone bool
	doneSummary  []any
}

type streamDelivery struct {
	// onMeta 在首次得知上游 response id 时调用（可为 nil），供会话以上游 id 提前开流。
	onMeta func(meta map[string]any)
	// forward 交付消息/摘要事件；commit 时带上 meta（会话尚未开流则据此开场），之后 meta 为 nil。
	forward func(meta map[string]any, frames []map[string]any) error

	// bufferUntilValidated（stream_tool_mode: buffered 且本轮有可调用工具）：消息/摘要事件照常
	// 经状态机校验，但不 commit、不向客户端交付；终态整批校验通过后由会话完整回放。
	// response.created 的 onMeta 照常触发，开流与心跳不受影响。
	bufferUntilValidated bool

	committed     bool
	meta          map[string]any
	pending       []map[string]any
	knownMessages map[int]string
	knownParts    map[[2]int]bool
	// knownReasonings / knownSummaryParts 是摘要增量的提交前提：item.added 与 part.added 已按序
	// 出现且 id 匹配（与 message 的 knownMessages / knownParts 同级）；缺前提时不提交，终态回放。
	knownReasonings   map[int]string
	knownSummaryParts map[[2]int]bool
	messages          map[int]*streamedMessage
	reasonings        map[int]*streamedReasoning
	// pendingBroken 记住缓冲批试运行失败（缺开启前提、乱序、重复、id/内容冲突）：未提交路径
	// 一律退回终态完整回放，且同一批次不会在后续增量到来时变得可提交，跳过重复试运行。
	pendingBroken bool
	// deliveredText / deliveredSummary 记录已交给宿主流的内容种类（供 relay_summary 区分
	// 正文口径与摘要口径；只有非空增量算交付，buffered 从不 forward，两者保持 false）。
	deliveredText    bool
	deliveredSummary bool
	terminal         bool
	sentinel         bool
	events           int // 已解码的上游事件数；用于识别「有 SSE 事件却始终未 commit」
}

func newStreamDelivery(onMeta func(map[string]any), forward func(map[string]any, []map[string]any) error) *streamDelivery {
	return &streamDelivery{
		onMeta: onMeta, forward: forward,
		knownMessages: map[int]string{}, knownParts: map[[2]int]bool{},
		knownReasonings: map[int]string{}, knownSummaryParts: map[[2]int]bool{},
		messages: map[int]*streamedMessage{}, reasonings: map[int]*streamedReasoning{},
	}
}

func streamEventError() error {
	return fail(502, "invalid_upstream_stream", "Basis Points stream contains inconsistent output events")
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
		itemType := stringValue(item["type"])
		if itemType != "message" && itemType != "reasoning" {
			return nil
		}
		index, err := streamIndex(value["output_index"])
		if err != nil {
			return err
		}
		if kind == "response.output_item.added" && itemType == "message" {
			d.knownMessages[index] = stringValue(item["id"])
		}
		if kind == "response.output_item.added" && itemType == "reasoning" {
			d.knownReasonings[index] = stringValue(item["id"])
		}
	case "response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done":
		index, err := streamIndex(value["output_index"])
		if err != nil {
			return err
		}
		part, err := streamIndex(value["summary_index"])
		if err != nil {
			return err
		}
		if kind == "response.reasoning_summary_part.added" {
			d.knownSummaryParts[[2]int{index, part}] = true
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
		// 工具参数增量及其他 item 由完整终态保留，不作为正文或摘要提前暴露。
		return nil
	}
	if d.bufferUntilValidated {
		// 复用同一状态机核验缓冲事件，不提交、不交付；不一致直接失败（不当作工具格式错误重新生成）。
		_, err := d.applyEvent(value)
		return err
	}
	if d.committed {
		frame, err := d.applyEvent(value)
		if err != nil {
			return err
		}
		// nil 表示事件已消费但不转发（推理 item.done 只校验，密文延迟到终态回放交付）。
		if frame == nil {
			return nil
		}
		d.noteDelivered([]map[string]any{frame})
		return d.forward(nil, []map[string]any{frame})
	}
	d.pending = append(d.pending, value)
	delta, _ := value["delta"].(string)
	if (kind != "response.output_text.delta" && kind != "response.reasoning_summary_text.delta") || delta == "" || d.meta == nil {
		return nil
	}
	if kind == "response.output_text.delta" {
		index, _ := streamIndex(value["output_index"])
		part, _ := streamIndex(value["content_index"])
		if d.knownMessages[index] == "" || d.knownMessages[index] != stringValue(value["item_id"]) || !d.knownParts[[2]int{index, part}] {
			return nil
		}
	} else {
		index, _ := streamIndex(value["output_index"])
		part, _ := streamIndex(value["summary_index"])
		if d.knownReasonings[index] == "" || d.knownReasonings[index] != stringValue(value["item_id"]) || !d.knownSummaryParts[[2]int{index, part}] {
			return nil
		}
	}
	if d.pendingBroken {
		return nil
	}
	frames, ok := d.replayPending()
	if !ok {
		d.pendingBroken = true
		return nil
	}
	d.committed = true
	d.noteDelivered(frames)
	return d.forward(cloneObject(d.meta), frames)
}

// replayPending 在独立状态机上整批试运行缓冲事件：全部按序一致应用时才提交，并接管试运行
// 的状态与帧；任一事件冲突（缺开启前提、乱序、重复、id/内容不一致）时不提交、不交付，由终态
// 完整回放。提交前还没交付任何内容，缓冲批内的冲突一律安全退回回放；提交后到达的同类冲突
// 仍由 applyEvent 直接拒绝。试运行失败后同一缓冲序列不可能再满足提交前提（事件顺序与状态
// 推进都是确定性的），调用方用 pendingBroken 记住并跳过后续重复试运行。
func (d *streamDelivery) replayPending() ([]map[string]any, bool) {
	probe := &streamDelivery{
		knownMessages: d.knownMessages, knownParts: d.knownParts,
		knownReasonings: d.knownReasonings, knownSummaryParts: d.knownSummaryParts,
		messages: map[int]*streamedMessage{}, reasonings: map[int]*streamedReasoning{},
	}
	frames := make([]map[string]any, 0, len(d.pending))
	for _, item := range d.pending {
		frame, err := probe.applyEvent(item)
		if err != nil {
			return nil, false
		}
		// 已消费但不转发的事件（推理 item.done）不进入交付批次；本批的提交触发者必为
		// 非空 delta，frames 至少含该帧。
		if frame != nil {
			frames = append(frames, frame)
		}
	}
	d.messages, d.reasonings = probe.messages, probe.reasonings
	d.pending = nil
	return frames, true
}

// noteDelivered 记录本批交付事件包含的内容种类（正文 output_text 与推理摘要
// reasoning_summary_text 的非空增量），供 relay_summary 区分两种交付口径；空 delta
// 不代表交付了任何内容，buffered 从不调用 forward，两个口径都保持 false。
func (d *streamDelivery) noteDelivered(frames []map[string]any) {
	for _, frame := range frames {
		if delta, _ := frame["delta"].(string); delta == "" {
			continue
		}
		switch stringValue(frame["type"]) {
		case "response.output_text.delta":
			d.deliveredText = true
		case "response.reasoning_summary_text.delta":
			d.deliveredSummary = true
		}
	}
}

// applyEvent 按事件类型分派：reasoning 条目与摘要事件走独立状态，其余走消息状态。
func (d *streamDelivery) applyEvent(value map[string]any) (map[string]any, error) {
	kind := stringValue(value["type"])
	if strings.HasPrefix(kind, "response.reasoning_summary_") ||
		((kind == "response.output_item.added" || kind == "response.output_item.done") && stringValue(objectValue(value["item"])["type"]) == "reasoning") {
		return d.applyReasoningEvent(value)
	}
	return d.applyMessageEvent(value)
}

// applyReasoningEvent 校验并登记推理条目事件；开场（added）统一清空预填摘要字段并剥离密文
// （正文只经 delta 累计，密文随终态回放交付、与终态 output 同值），其余字段（身份等）保持原样。
// item.done 由 validateItem 校验并登记 upstreamDone 快照后返回 nil 帧（不转发）。
// 使用独立状态记录交付进度，不登记为普通消息（与消息同 index 冲突即报错）。
func (d *streamDelivery) applyReasoningEvent(value map[string]any) (map[string]any, error) {
	index, err := streamIndex(value["output_index"])
	if err != nil {
		return nil, err
	}
	kind := stringValue(value["type"])
	r := d.reasonings[index]
	if kind == "response.output_item.added" {
		item := objectValue(value["item"])
		id := stringValue(item["id"])
		if r != nil || d.messages[index] != nil || id == "" {
			return nil, streamEventError()
		}
		d.reasonings[index] = &streamedReasoning{id: id, summaries: map[int]*streamedPart{}}
		frame := cloneObject(value)
		added := cloneObject(objectValue(frame["item"]))
		added["summary"] = []any{}
		// 上游 added 携带的是阶段快照瞬时密文（与 done/终态的值可不同）：live 帧不携带密文。
		delete(added, "encrypted_content")
		frame["item"] = added
		return frame, nil
	}
	if r == nil || r.upstreamDone {
		return nil, streamEventError()
	}
	if kind == "response.output_item.done" {
		if err := r.validateItem(objectValue(value["item"])); err != nil {
			return nil, err
		}
		for _, part := range r.summaries {
			if !part.done {
				return nil, streamEventError()
			}
		}
		r.upstreamDone = true
		r.doneSummary, _ = objectValue(value["item"])["summary"].([]any)
		return nil, nil
	}
	if value["item_id"] != r.id {
		return nil, streamEventError()
	}
	partIndex, err := streamIndex(value["summary_index"])
	if err != nil {
		return nil, err
	}
	part := r.summaries[partIndex]
	if kind == "response.reasoning_summary_part.added" {
		opening := objectValue(value["part"])
		if part != nil || partIndex != len(r.summaries) || (partIndex > 0 && !r.summaries[partIndex-1].done) || stringValue(opening["type"]) != "summary_text" {
			return nil, streamEventError()
		}
		r.summaries[partIndex] = &streamedPart{kind: "summary_text"}
		frame := cloneObject(value)
		added := cloneObject(opening)
		added["text"] = ""
		frame["part"] = added
		return frame, nil
	}
	if part == nil || part.done {
		return nil, streamEventError()
	}
	switch kind {
	case "response.reasoning_summary_text.delta":
		text, ok := value["delta"].(string)
		if !ok || part.textDone {
			return nil, streamEventError()
		}
		part.text.WriteString(text)
	case "response.reasoning_summary_text.done":
		if part.textDone || value["text"] != part.text.String() {
			return nil, streamEventError()
		}
		part.textDone = true
	case "response.reasoning_summary_part.done":
		completed := objectValue(value["part"])
		if !part.textDone || stringValue(completed["type"]) != "summary_text" || completed["text"] != part.text.String() {
			return nil, streamEventError()
		}
		part.done = true
	}
	return value, nil
}

// validateItem 核对条目与已登记的推理状态：身份一致；上游 done 快照（若有）的摘要与当前
// 条目完全一致；每个已开启 part 的文本是条目最终文本的前缀（textDone 时精确相等）。
// 密文不参与任何跨事件比对：阶段快照（added/done/completed）密文值可不同，缺失时也可能不再给出。
func (r *streamedReasoning) validateItem(item map[string]any) error {
	if stringValue(item["type"]) != "reasoning" || stringValue(item["id"]) != r.id {
		return streamEventError()
	}
	summaries, _ := item["summary"].([]any)
	if r.upstreamDone && !bytes.Equal(jsonBytes(summaries), jsonBytes(r.doneSummary)) {
		return streamEventError()
	}
	for i, part := range r.summaries {
		if i >= len(summaries) {
			return streamEventError()
		}
		finalPart := objectValue(summaries[i])
		text, ok := finalPart["text"].(string)
		if stringValue(finalPart["type"]) != "summary_text" || !ok || !strings.HasPrefix(text, part.text.String()) || (part.textDone && text != part.text.String()) {
			return streamEventError()
		}
	}
	return nil
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
		if m != nil || d.reasonings[index] != nil || id == "" {
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

// validateFinal 核对上游原始终态与已交付（或 buffered 下已缓冲校验）的正文/摘要一致（在
// transformResponseBody 之前调用，最先暴露与已交付内容不一致的终态，失败态不会被当作工具
// 格式错误重新生成）。增量模式未 commit 时无需核对；buffered 时即使未交付也要核对，meta
// 可为空（整体 JSON 响应或只有终态事件）。
func (d *streamDelivery) validateFinal(response map[string]any) error {
	if !d.committed && !d.bufferUntilValidated {
		return nil
	}
	if d.meta != nil && response["id"] != d.meta["id"] {
		return streamEventError()
	}
	output, _ := response["output"].([]any)
	for index, reasoning := range d.reasonings {
		if index >= len(output) {
			return streamEventError()
		}
		if err := reasoning.validateItem(objectValue(output[index])); err != nil {
			return err
		}
	}
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

// keepFinal 过滤专用终态回放（syntheticEventsWithReasoningSummary(…, false)）：跳过已交付的
// 消息/摘要事件，正文与摘要只补发未交付的后缀；工具及其他 item 与终止事件照常保留。
// 会就地改写被截短的 delta。
func (d *streamDelivery) keepFinal(ev *sseEvent) bool {
	kind := ev.name
	if kind == "response.created" || kind == "response.in_progress" {
		return false
	}
	index, ok := eventIndex(ev.value["output_index"])
	if !ok {
		return true
	}
	if reasoning := d.reasonings[index]; reasoning != nil {
		// added 已 live 交付（剥离密文）；item.done 从未 live 交付（只校验），终态回放的
		// done 必须保留——最终 item.done 恰好一次，其密文与 completed/incomplete.output 同值。
		if kind == "response.output_item.added" {
			return false
		}
		if partIndex, ok := eventIndex(ev.value["summary_index"]); ok {
			if part := reasoning.summaries[partIndex]; part != nil {
				switch kind {
				case "response.reasoning_summary_part.added":
					return false
				case "response.reasoning_summary_text.delta":
					full, _ := ev.value["delta"].(string)
					sent := part.text.Len()
					if len(full) <= sent {
						return false
					}
					ev.value["delta"] = full[sent:]
				case "response.reasoning_summary_text.done":
					return !part.textDone
				case "response.reasoning_summary_part.done":
					return !part.done
				}
			}
		}
		return true
	}
	message := d.messages[index]
	if message == nil {
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
