package basispoints

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
)

// 终态解析移植自原仓库 v0.1.10（#8）：按正文实际格式解码 JSON 或 SSE，保留完整终态对象，
// 不把失败或不完整响应伪装成 completed；拒绝空正文、HTML、非法 JSON、多个终态及状态不一致。
// 与原仓库的差别：上游报告的失败（response.failed / response.cancelled / error）仍交给
// classifyUpstreamFailure，保留凭据失效与限流的 401/403/429，让 CPA 换号或冷却。

// parseResponse 解析一次上游往返的完整正文；失败时只附带内容类型类别与字节数，不回显正文。
func parseResponse(raw []byte, headers http.Header) (map[string]any, error) {
	response, err := parseFinalStreamResponse(raw)
	if err == nil {
		return response, nil
	}
	if !isKind(err, "invalid_upstream_response") {
		return nil, err
	}
	kind := "other"
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(headers.Get("Content-Type"), ";")[0]))
	switch contentType {
	case "application/json", "text/event-stream", "text/html":
		kind = contentType
	case "":
		kind = "missing"
	}
	return nil, fail(502, "invalid_upstream_response", fmt.Sprintf("%s (content_type=%s; bytes=%d)", err.Error(), kind, len(raw)))
}

// terminalResponse 校验终态对象：completed / incomplete 原样保留；failed / cancelled 按帧内
// 状态与错误码分类。
func terminalResponse(response map[string]any) (map[string]any, error) {
	if response == nil {
		return nil, fail(502, "invalid_upstream_response", "Basis Points returned no response object")
	}
	switch status := stringValue(response["status"]); status {
	case "failed", "cancelled":
		return nil, classifyUpstreamFailure(map[string]any{"response": response}, "response."+status)
	case "completed", "incomplete":
	default:
		return nil, fail(502, "invalid_upstream_response", "Basis Points response has no valid terminal status")
	}
	if _, ok := response["output"].([]any); !ok {
		return nil, fail(502, "invalid_upstream_response", "Basis Points returned no output array")
	}
	return response, nil
}

// parseFinalStreamResponse 按实际正文解码：以 { 开头按单个 JSON 终态对象，否则按 SSE。
// 不重发请求来猜测上游协议。
func parseFinalStreamResponse(raw []byte) (map[string]any, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fail(502, "invalid_upstream_response", "Basis Points returned an empty response")
	}
	if trimmed[0] == '{' {
		object, reason := parseRelayObject(string(trimmed))
		if reason != "" {
			return nil, fail(502, "invalid_upstream_response", "Basis Points returned invalid JSON")
		}
		return terminalResponse(object)
	}
	decoder := newSSEDecoder()
	var terminal map[string]any
	emit := func(event, data string) error {
		if strings.TrimSpace(data) == "[DONE]" {
			return nil
		}
		object, reason := parseRelayObject(data)
		if reason != "" {
			return fail(502, "invalid_upstream_response", "Basis Points returned invalid SSE JSON")
		}
		kind := stringValue(object["type"])
		if kind == "" {
			kind = event
		}
		switch {
		case isUpstreamFailureEvent(kind):
			return classifyUpstreamFailure(object, kind)
		case kind == "response.completed" || kind == "response.incomplete":
			response, err := terminalResponse(objectValue(object["response"]))
			if err != nil {
				return err
			}
			if status := stringValue(response["status"]); status != strings.TrimPrefix(kind, "response.") {
				return fail(502, "invalid_upstream_response", "Basis Points stream terminal status mismatch")
			}
			if terminal != nil {
				return fail(502, "invalid_upstream_response", "Basis Points returned multiple terminal responses")
			}
			terminal = response
		}
		return nil
	}
	if err := decoder.feed(raw, emit); err != nil {
		return nil, err
	}
	// EOF 时处理最后一条没有空行终止的事件，但仍严格校验其中 JSON。
	if err := decoder.feed([]byte("\n\n"), emit); err != nil {
		return nil, err
	}
	if terminal == nil {
		return nil, fail(502, "invalid_upstream_response", "Basis Points stream ended without a terminal response")
	}
	return terminal, nil
}
