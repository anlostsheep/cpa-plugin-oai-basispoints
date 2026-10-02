package basispoints

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

// relay_summary：每次宿主执行在收尾时写一条汇总日志，供现网按请求统计工具中继的
// 首次 / 最终校验结果、重新生成结局与交付方式。
//
// CPA 7.3.20 的日志 formatter 只输出白名单字段，插件经 host.log 传入的 fields 会被丢弃，
// 所以汇总以固定格式的 JSON 写进 message 文本（"basispoints: relay_summary {...}"）。
// 字段只含类别、计数与版本信息，不含请求或响应内容、工具参数、凭据。
//
// 口径：
//   - 一条记录对应一次宿主执行（CPA 的日志前缀提供 request_id）。同一 request_id 有多条
//     时，请求级结局取最后一条；尝试次数与重复执行数按全部记录统计。
//   - first 是本次执行内第一次有效的工具校验结果，final 是最后一次；没有走到工具校验
//     （例如建连失败）时为 not_run。
//   - legacy_calls 只在整批校验通过后累计；失败批次不计。

const relaySummaryPrefix = "basispoints: relay_summary "

// 校验结果取值。
const (
	relayNotRun = "not_run"
	relayNoCall = "no_call"
	relayOK     = "ok"
	relayLegacy = "legacy"
)

// 重新生成结局。
const (
	regenNone      = "none"
	regenSuccess   = "success"
	regenExhausted = "exhausted"
	regenAborted   = "aborted"
)

// relayReasons 是失败原因白名单；按顺序匹配诊断文本中的标识符，未命中记为 other。
var relayReasons = []string{
	"required_tool_choice_not_satisfied",
	"tool_not_allowed_by_tool_choice",
	"tool_not_in_catalog",
	"arguments_schema_mismatch",
	"custom_args_not_string",
	"duplicate_call_id",
	"missing_call_id",
	"outer_not_transport",
	"references_invalid",
	"code_not_string",
	"legacy_envelope_invalid",
	"trailing_content",
	"invalid_json",
}

// relayStats 是一次整批工具转换的计数（仅在整批成功时有意义）。
type relayStats struct {
	Calls  int
	Legacy int
}

type relaySummaryRecord struct {
	V             int    `json:"v"`
	Version       string `json:"version"`
	Model         string `json:"model"`
	Transport     string `json:"transport"`
	Stream        bool   `json:"stream"`
	ConfigMode    string `json:"config_mode"`
	Delivery      string `json:"delivery"`
	ToolCallable  *bool  `json:"tool_callable"` // null：无法解析请求、不能判断
	Attempts      int    `json:"attempts_started"`
	TextCommitted bool   `json:"text_committed"`
	First         string `json:"first"`
	Final         string `json:"final"`
	Regen         string `json:"regen"`
	LegacyCalls   int    `json:"legacy_calls"`
	Exit          string `json:"exit"`
	ErrorKind     string `json:"error_kind"`
}

// relaySummary 在一次执行内累计状态，并在收尾时恰好写一次日志。
type relaySummary struct {
	s       *Service
	request ExecutorRequest
	mu      sync.Mutex
	once    sync.Once
	rec     relaySummaryRecord
	regen   bool
	checks  int    // 已完成的工具校验次数
	status  string // 最终交付的终态 status（incomplete 时 exit 记为 incomplete）
	preset  string // 收尾方预设的 exit（如 connect_error），关流钩子按它记录
}

// setExit 预设 exit，供关流前的钩子使用（钩子只拿得到错误，拿不到调用方的分类）。
func (r *relaySummary) setExit(exit string) {
	r.mu.Lock()
	r.preset = exit
	r.mu.Unlock()
}

// bind 让会话在正常收尾的 host.stream.close 之前写汇总，保证日志带上宿主 request_id。
// 汇总是收尾时的快照：钩子开始写之前若已被强关，以强关原因为准；钩子已开始写之后才发生
// 的强关（例如 host.log 阻塞期间截止看门狗关流）不再反映到这条记录里。
func (r *relaySummary) bind(session *streamSession) {
	session.onEnd = func(outcome string, err error) {
		if outcome == finishCompleted {
			err = nil
		}
		r.finishAfter(session, outcome, err)
	}
}

// finishAfter 在往返协程收尾时兜底写汇总：会话被看门狗强制关流时（不调用 onEnd），以强关
// 原因为准（timeout / stopped），不继承此前设置的收尾结果或旧错误。正常收尾时汇总通常已由
// onEnd 写出，这里只是兜底（once 保证只写一次）。
func (r *relaySummary) finishAfter(session *streamSession, exit string, err error) {
	if reason := session.forcedBy(); reason != nil {
		r.finish(exitForError(reason), reason)
		return
	}
	r.finish(exit, err)
}

func (s *Service) newRelaySummary(request ExecutorRequest, transport string, stream bool) *relaySummary {
	cfg := s.config()
	r := &relaySummary{s: s, request: request, rec: relaySummaryRecord{
		V:          1,
		Version:    Version,
		Model:      request.Model,
		Transport:  transport,
		Stream:     stream,
		ConfigMode: cfg.StreamToolMode,
		First:      relayNotRun,
		Final:      relayNotRun,
		Regen:      regenNone,
	}}
	switch {
	case !stream:
		r.rec.Delivery = "non_stream"
	case transport == TransportWS:
		r.rec.Delivery = "terminal"
	default:
		r.rec.Delivery = "incremental"
	}
	return r
}

func (r *relaySummary) setToolCallable(callable bool) {
	r.mu.Lock()
	r.rec.ToolCallable = &callable
	r.mu.Unlock()
}

// setToolCallableFrom 按客户端原始请求判断本轮是否有可调用工具；解析失败时保持未知（null）。
func (r *relaySummary) setToolCallableFrom(request ExecutorRequest) {
	if source, err := requestSource(request); err == nil {
		r.setToolCallable(len(callableClientToolSpecs(source)) > 0)
	}
}

func (r *relaySummary) setDelivery(delivery string) {
	r.mu.Lock()
	r.rec.Delivery = delivery
	r.mu.Unlock()
}

// attemptStarted 在每次发起上游往返（首次与重新生成）时调用。final 表示最后一次尝试的校验
// 结果：新尝试开始时重置为 not_run，避免重新生成后因上游错误中止时仍残留上一次的 invalid。
func (r *relaySummary) attemptStarted() {
	r.mu.Lock()
	if r.rec.Attempts > 0 {
		r.rec.Final = relayNotRun
	}
	r.rec.Attempts++
	r.mu.Unlock()
}

func (r *relaySummary) markTextCommitted() {
	r.mu.Lock()
	r.rec.TextCommitted = true
	r.mu.Unlock()
}

// validated 记录一次工具整批校验的结果。
func (r *relaySummary) validated(stats relayStats, err error) {
	result := relayResult(stats, err)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks++
	if r.rec.First == relayNotRun {
		r.rec.First = result
	}
	r.rec.Final = result
	if err == nil {
		r.rec.LegacyCalls += stats.Legacy
	}
}

// setTerminalStatus 记录将要交付的终态 status。
func (r *relaySummary) setTerminalStatus(status string) {
	r.mu.Lock()
	r.status = status
	r.mu.Unlock()
}

// regenerating 在决定重新生成时调用。
func (r *relaySummary) regenerating() {
	r.mu.Lock()
	r.regen = true
	r.mu.Unlock()
}

// finish 写出汇总；exit 为空时按 err 推断。只生效一次。
func (r *relaySummary) finish(exit string, err error) {
	r.once.Do(func() {
		r.mu.Lock()
		rec := r.rec
		if exit == "" {
			exit = r.preset
		}
		if exit == "" {
			exit = exitForError(err)
		}
		if exit == "completed" && r.status == "incomplete" {
			exit = "incomplete"
		}
		rec.Exit = exit
		if err != nil {
			rec.ErrorKind = errorKind(err)
		}
		if r.regen {
			switch {
			case r.checks >= 2 && (rec.Final == relayOK || rec.Final == relayNoCall || rec.Final == relayLegacy):
				// 重新生成后的校验通过；之后的交付失败另由 exit 反映。
				rec.Regen = regenSuccess
			case strings.HasPrefix(rec.Final, "invalid:") && r.checks >= 2:
				// 重新生成后的那一次校验仍然失败。
				rec.Regen = regenExhausted
			default:
				rec.Regen = regenAborted
			}
		}
		r.mu.Unlock()
		r.s.logEvent(r.request, "info", relaySummaryPrefix+string(jsonBytes(rec)), nil)
	})
}

func relayResult(stats relayStats, err error) string {
	if err != nil {
		if isKind(err, "invalid_tool_call") {
			return "invalid:" + relayReasonCategory(err)
		}
		return "invalid:other"
	}
	switch {
	case stats.Calls == 0:
		return relayNoCall
	case stats.Legacy > 0:
		return relayLegacy
	default:
		return relayOK
	}
}

// relayReasonCategory 把中转诊断映射到白名单类别，不带任何偏移或内容。
func relayReasonCategory(err error) string {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return "other"
	}
	message := apiErr.Message
	// 先匹配完整别名：invalid_json_object 等含有 invalid_json 子串，不能被它提前吞掉。
	for _, alias := range []string{"not_object_or_json_string", "invalid_json_object", "null_object"} {
		if strings.Contains(message, alias) {
			return "not_object"
		}
	}
	for _, reason := range relayReasons {
		if strings.Contains(message, reason) {
			return reason
		}
	}
	if strings.Contains(message, "parallel_tool_calls_disabled") {
		return "parallel_limit"
	}
	return "other"
}

// exitForError 把执行结束时的错误映射为 exit 类别。
func exitForError(err error) string {
	if err == nil {
		return "completed"
	}
	if errors.Is(err, errClientDisconnected) || isKind(err, "client_disconnected") {
		return "cancelled"
	}
	switch errorKind(err) {
	case "plugin_stopped":
		return "stopped"
	case "upstream_timeout":
		return "timeout"
	}
	if isRequestScoped(err) {
		return "failed"
	}
	return "upstream_error"
}

// relaySummaryFromLog 解析一行日志中的汇总（供测试与聚合使用）。
func relaySummaryFromLog(line string) (relaySummaryRecord, bool) {
	index := strings.Index(line, relaySummaryPrefix)
	if index < 0 {
		return relaySummaryRecord{}, false
	}
	var rec relaySummaryRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(line[index+len(relaySummaryPrefix):])), &rec); err != nil {
		return relaySummaryRecord{}, false
	}
	return rec, true
}
