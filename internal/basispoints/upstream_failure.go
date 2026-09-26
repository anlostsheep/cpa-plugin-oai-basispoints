package basispoints

import (
	"fmt"
	"strings"
)

// classifyUpstreamFailure 把上游在流内报告的失败（response.failed / response.incomplete /
// error 帧或 SSE 事件）映射为 APIError。
//
// 流已建立之后上游仍可能在帧里报告凭据失效或限流（帧内 status 或 error.code）。这类错误
// 必须保留 401/403/429 状态，让 streamSession 带 error 关闭、交给 CPA 冷却或换号；
// 只有真正属于「本次请求」的失败（模型半途失败、输出不完整、请求参数问题）才归为
// upstream_incomplete，由会话发 response.failed 后正常关闭。
func classifyUpstreamFailure(frame map[string]any, typeName string) error {
	status := 0
	var codes []string
	collect := func(object map[string]any, withType bool) {
		if object == nil {
			return
		}
		for _, key := range []string{"status", "status_code", "http_status"} {
			if n := int(numberValue(object[key])); n >= 400 && n <= 599 && status == 0 {
				status = n
			}
		}
		keys := []string{"code", "reason"}
		if withType {
			keys = append(keys, "type")
		}
		for _, key := range keys {
			if value := strings.ToLower(stringValue(object[key])); value != "" {
				codes = append(codes, value)
			}
		}
	}
	collect(frame, false)
	collect(objectValue(frame["error"]), true)
	if response := objectValue(frame["response"]); response != nil {
		// response 自身也可能携带 status_code / http_status（其 status 字段是字符串，
		// numberValue 对字符串返回 0，不会误判）。
		collect(response, false)
		collect(objectValue(response["error"]), true)
		collect(objectValue(response["incomplete_details"]), false)
	}

	code := firstSafeCode(codes)
	message := "Basis Points response did not complete (" + typeName
	if code != "" {
		message += ": " + code
	}
	message += ")"

	switch {
	case status == 401 || status == 402 || status == 403 || status == 404 || status == 429:
		return fail(status, "upstream_error", fmt.Sprintf("%s [HTTP %d]", message, status))
	case codesMatch(codes, "rate_limit", "too_many_requests", "usage_limit", "quota"):
		return fail(429, "upstream_error", message)
	case codesMatch(codes, "account_deactivated", "forbidden", "permission", "unsupported_country", "unsupported_region"):
		return fail(403, "upstream_error", message)
	case codesMatch(codes, "invalid_api_key", "unauthorized", "unauthenticated", "authentication", "invalid_token", "token_expired", "expired_token", "token_invalidated", "token_revoked"):
		return fail(401, "upstream_error", message)
	case status >= 400:
		return fail(status, "upstream_incomplete", message)
	}
	return fail(502, "upstream_incomplete", message)
}

func codesMatch(codes []string, needles ...string) bool {
	for _, code := range codes {
		for _, needle := range needles {
			if strings.Contains(code, needle) {
				return true
			}
		}
	}
	return false
}

// firstSafeCode 只回传短小的标识符形态的错误码，避免把上游任意文本（可能含敏感内容）
// 带进错误串。
func firstSafeCode(codes []string) string {
	for _, code := range codes {
		if len(code) == 0 || len(code) > 64 {
			continue
		}
		ok := true
		for _, r := range code {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
				ok = false
				break
			}
		}
		if ok {
			return code
		}
	}
	return ""
}

// isUpstreamFailureEvent 判断事件类型是否表示上游本轮失败。response.incomplete 是合法终态
// （如达到 max_output_tokens），原样交给客户端，不属于失败。
func isUpstreamFailureEvent(typeName string) bool {
	return typeName == "response.failed" || typeName == "response.cancelled" || typeName == "error"
}
