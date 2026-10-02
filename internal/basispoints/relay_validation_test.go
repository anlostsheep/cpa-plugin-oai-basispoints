package basispoints

// 移植自上游 JaxsonWang/cpa-plugin-oai-basispoints a5f698d（relay_validation_test.go，MIT），按 fork 的结构适配。

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestRelayRejectsMalformedJSONWithoutRepair(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		outer   bool
	}{
		{"raw_newline", "{\"cmd\":\"PRIVATE\nvalue\"}", false},
		{"raw_tab", "{\"cmd\":\"PRIVATE\tvalue\"}", false},
		{"markdown_fence", "```json\n{\"cmd\":\"PRIVATE\"}\n```", false},
		{"fence_trailing_content", "```json\n{\"cmd\":\"PRIVATE\"}\n```\n{\"second\":true}", false},
		{"outer_fence_trailing_content", "", true},
		{"trailing_comma", `{"cmd":"PRIVATE",}`, false},
		// fork 差异：function 参数恰好多套一层完整 JSON 字符串时，允许一次有界的纯解析
		// （合同 v6 约束），见 TestRelayDoubleEncodedFunctionPayloadBoundedDecode。
		{"missing_array_value", `{"targets":[,]}`, false},
		{"missing_object_member", `{,}`, false},
		{"missing_quote", `{"cmd":"PRIVATE "quote""}`, false},
		{"truncated_object", `{"cmd":`, false},
		{"array", `[]`, false},
		{"null", `null`, false},
		{"two_objects", `{} {}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := namespaceTestSource("function", "invoke", "tools")
			good := rawRelayNative(t.Name()+"-good", "tools.invoke", `{"cmd":"valid"}`, []any{"tools.invoke"})
			bad := rawRelayNative(t.Name()+"-bad", "tools.invoke", tc.payload, []any{"tools.invoke"})
			if tc.outer {
				bad = rawRelayNative(t.Name()+"-bad", "tools.invoke", `{"cmd":"PRIVATE"}`, []any{"tools.invoke"})
				bad["arguments"] = "```json\n" + bad["arguments"].(string) + "\n```\n{\"second\":true}"
			}
			original := map[string]any{"status": "completed", "output": []any{good, bad}}
			before := string(jsonBytes(original))
			body, response, changed, err := transformResponseBody(jsonBytes(original), source)
			var api *APIError
			if !errors.As(err, &api) || api.Status != 422 || api.Kind != "invalid_tool_call" || body != nil || response != nil || changed {
				t.Fatalf("malformed batch was not rejected atomically: changed=%t err=%v", changed, err)
			}
			if strings.Contains(api.Message, "PRIVATE") {
				t.Fatal("diagnostic leaked tool input")
			}
			for _, native := range []map[string]any{good, bad} {
				if rememberedNativeCall(stringValue(native["call_id"])) != nil {
					t.Fatal("failed batch was partially cached")
				}
			}
			if string(jsonBytes(original)) != before {
				t.Fatal("original response was mutated")
			}
		})
	}
}

func TestRelayCustomPayloadDoesNotRequireJSON(t *testing.T) {
	for _, payload := range []string{"```json\n{,}\n```\ntrailing text", `{"targets":[,]}`, "  PRIVATE\n\tvalue\r\n"} {
		t.Run(payload, func(t *testing.T) {
			source := namespaceTestSource("custom", "invoke", "tools")
			native := rawRelayNative(t.Name(), "tools.invoke", payload, []any{"tools.invoke"})
			call, err := extractNativeClientToolCall(native, clientToolSpecs(source))
			if err != nil || call["type"] != "custom_tool_call" || call["input"] != payload {
				t.Fatalf("custom raw input was rejected or changed: %v", err)
			}
		})
	}
}

// fork 保留的有界容错：function 载荷整体是一个多套了一层的 JSON 字符串时只剥一层；
// 套两层、或剥一层后仍不是单个对象时照常拒绝；custom 载荷从不解码。
func TestRelayDoubleEncodedFunctionPayloadBoundedDecode(t *testing.T) {
	source := namespaceTestSource("function", "invoke", "tools")
	once := rawRelayNative("de-once", "tools.invoke", `"{\"cmd\":\"ok\"}"`, []any{"tools.invoke"})
	_, response, changed, err := transformResponseBody(jsonBytes(map[string]any{"status": "completed", "output": []any{once}}), source)
	if err != nil || !changed || objectValue(response["output"].([]any)[0])["arguments"] != `{"cmd":"ok"}` {
		t.Fatalf("one extra layer should be decoded once: changed=%t err=%v", changed, err)
	}
	inner, _ := json.Marshal(`{"cmd":"PRIVATE"}`)
	twice, _ := json.Marshal(string(inner))
	for name, payload := range map[string]string{"twice": string(twice), "not_object": `"[1,2]"`} {
		bad := rawRelayNative("de-"+name, "tools.invoke", payload, []any{"tools.invoke"})
		_, _, _, err := transformResponseBody(jsonBytes(map[string]any{"status": "completed", "output": []any{bad}}), source)
		if !isKind(err, "invalid_tool_call") || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("%s: want invalid_tool_call without content, got %v", name, err)
		}
	}
	custom := namespaceTestSource("custom", "invoke", "tools")
	raw := `"{\"cmd\":\"keep\"}"`
	native := rawRelayNative("de-custom", "tools.invoke", raw, []any{"tools.invoke"})
	_, response, _, err = transformResponseBody(jsonBytes(map[string]any{"status": "completed", "output": []any{native}}), custom)
	if err != nil || objectValue(response["output"].([]any)[0])["input"] != raw {
		t.Fatalf("custom payload must be kept verbatim: %v", err)
	}
}
