package basispoints

// 0.1.17.0 过渡期兼容：references 缺失或恰好为 [] 且 code 是合法旧 {tool,args} 封装时按旧路由
// 接受并计数；references 为其他任何值严格按新规则，不回退；不恢复旧解析器的其他宽容。

import (
	"strings"
	"testing"
)

func legacyCode(tool string, args any) string {
	return string(jsonBytes(map[string]any{"tool": tool, "args": args}))
}

func transitionNative(id string, arguments map[string]any) map[string]any {
	return map[string]any{"type": "function_call", "name": transportName, "id": "fc_" + id, "call_id": "call_" + id, "arguments": string(jsonBytes(arguments))}
}

func TestTransitionLegacyAcceptedWhenReferencesAbsentOrEmpty(t *testing.T) {
	patch := "*** Begin Patch\n*** Add File: a.txt\n+\"q\" \\ \t end  \n*** End Patch"
	for name, extra := range map[string]map[string]any{
		"absent": {},
		"empty":  {"references": []any{}},
	} {
		t.Run(name, func(t *testing.T) {
			arguments := map[string]any{"code": legacyCode("functions.apply_patch", patch)}
			for k, v := range extra {
				arguments[k] = v
			}
			source := namespaceTestSource("custom", "apply_patch", "functions")
			_, response, changed, stats, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{transitionNative(t.Name(), arguments)}}), source)
			if err != nil || !changed {
				t.Fatalf("legacy envelope should be accepted: %v", err)
			}
			call := objectValue(response["output"].([]any)[0])
			if call["type"] != "custom_tool_call" || call["name"] != "apply_patch" || call["input"] != patch {
				t.Fatalf("legacy call restored wrongly: %#v", call)
			}
			if stats.Calls != 1 || stats.Legacy != 1 {
				t.Fatalf("legacy must be counted: %+v", stats)
			}
		})
	}
}

func TestTransitionEmptyReferencesRequireLegacyEnvelope(t *testing.T) {
	source := namespaceTestSource("custom", "apply_patch", "")
	for name, code := range map[string]string{
		"raw_patch":        "*** Begin Patch\n*** End Patch",
		"extra_key":        string(jsonBytes(map[string]any{"tool": "apply_patch", "args": "x", "note": "y"})),
		"missing_args":     string(jsonBytes(map[string]any{"tool": "apply_patch"})),
		"nested_transport": legacyCode(transportName, "x"),
	} {
		t.Run(name, func(t *testing.T) {
			native := transitionNative(t.Name(), map[string]any{"references": []any{}, "code": code})
			if _, _, _, _, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{native}}), source); !isKind(err, "invalid_tool_call") {
				t.Fatalf("want invalid_tool_call, got %v", err)
			}
		})
	}
}

func TestTransitionLegacyObjectCodeRejected(t *testing.T) {
	source := namespaceTestSource("custom", "apply_patch", "")
	native := transitionNative("objcode", map[string]any{"code": map[string]any{"tool": "apply_patch", "args": "x"}})
	_, _, _, _, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{native}}), source)
	if !isKind(err, "invalid_tool_call") || relayReasonCategory(err) != "code_not_string" {
		t.Fatalf("object-typed code must be rejected as code_not_string, got %v", err)
	}
}

func TestTransitionInvalidReferencesDoNotFallBack(t *testing.T) {
	source := namespaceTestSource("custom", "apply_patch", "")
	code := legacyCode("apply_patch", "x") // 即使 code 是合法旧封装也不回退
	for name, references := range map[string]any{
		"null":           nil,
		"string":         "apply_patch",
		"two":            []any{"apply_patch", "apply_patch"},
		"non_string":     []any{42},
		"blank":          []any{"  "},
		"transport_name": []any{transportName},
		"padded":         []any{" apply_patch"},
	} {
		t.Run(name, func(t *testing.T) {
			native := transitionNative(t.Name(), map[string]any{"references": references, "code": code})
			_, _, _, _, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{native}}), source)
			if !isKind(err, "invalid_tool_call") || relayReasonCategory(err) != "references_invalid" {
				t.Fatalf("want references_invalid, got %v", err)
			}
		})
	}
}

// references 合法时 code 一律按所选工具的载荷处理，即使它看起来像旧封装，也不改走内层工具。
func TestTransitionValidReferencesTreatLegacyLookingCodeAsPayload(t *testing.T) {
	inner := legacyCode("other_tool", "rm -rf /")
	custom := namespaceTestSource("custom", "apply_patch", "")
	native := transitionNative("asis-custom", map[string]any{"references": []any{"apply_patch"}, "code": inner})
	_, response, _, stats, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{native}}), custom)
	if err != nil {
		t.Fatal(err)
	}
	call := objectValue(response["output"].([]any)[0])
	if call["name"] != "apply_patch" || call["input"] != inner || stats.Legacy != 0 {
		t.Fatalf("custom payload must be kept verbatim for the referenced tool: %#v %+v", call, stats)
	}
	function := namespaceTestSource("function", "invoke", "")
	native = transitionNative("asis-function", map[string]any{"references": []any{"invoke"}, "code": inner})
	_, response, _, stats, err = transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{native}}), function)
	if err != nil {
		t.Fatal(err)
	}
	call = objectValue(response["output"].([]any)[0])
	if call["name"] != "invoke" || !strings.Contains(stringValue(call["arguments"]), `"tool":"other_tool"`) || stats.Legacy != 0 {
		t.Fatalf("function payload must stay with the referenced tool: %#v %+v", call, stats)
	}
}

// 整批失败时，已解析出的旧封装调用不计入。
func TestTransitionLegacyNotCountedWhenBatchFails(t *testing.T) {
	source := namespaceTestSource("custom", "apply_patch", "")
	legacy := transitionNative("batch-legacy", map[string]any{"code": legacyCode("apply_patch", "ok")})
	bad := transitionNative("batch-bad", map[string]any{"references": []any{"missing_tool"}, "code": "x"})
	_, _, _, stats, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{legacy, bad}}), source)
	if !isKind(err, "invalid_tool_call") || stats.Legacy != 0 || stats.Calls != 0 {
		t.Fatalf("failed batch must not count legacy calls: %+v %v", stats, err)
	}
	// 失败批次不留下可借用的状态：同 call_id 的历史只按客户端条目自身冷重建。
	assertNoReplayableFailure(t, "call_batch-legacy", "apply_patch", "ok")
}

// 历史客户端条目不计入 legacy_calls，并按自身冷重建为 references + 纯载荷。
func TestTransitionHistoryEmptyReferencesNotCounted(t *testing.T) {
	source := namespaceTestSource("custom", "apply_patch", "")
	old := transitionNative("hist-old", map[string]any{"references": []any{}, "code": legacyCode("apply_patch", "old")})
	if _, _, _, stats, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{old}}), source); err != nil || stats.Legacy != 1 {
		t.Fatalf("setup: %v %+v", err, stats)
	}
	history := []any{
		map[string]any{"type": "custom_tool_call", "call_id": "call_hist-old", "name": "apply_patch", "input": "old"},
		map[string]any{"type": "custom_tool_call_output", "call_id": "call_hist-old", "output": "done"},
	}
	replay, err := translateInputItems(history)
	if err != nil {
		t.Fatal(err)
	}
	if route, code := rebuiltRoute(t, objectValue(replay[0])); route != "apply_patch" || code != "old" {
		t.Fatalf("history must cold rebuild from the client entry: %#v", replay[0])
	}
	fresh := transitionNative("hist-new", map[string]any{"references": []any{"apply_patch"}, "code": "new"})
	_, _, _, stats, err := transformResponseBodyStats(jsonBytes(map[string]any{"output": []any{fresh}}), source)
	if err != nil || stats.Legacy != 0 || stats.Calls != 1 {
		t.Fatalf("history must not affect legacy count of new output: %+v %v", stats, err)
	}
}

// 新提示不再教 references=[]；重新生成提示要求新格式。
func TestPromptsTeachReferencesNotEmptyArray(t *testing.T) {
	source := map[string]any{"tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}, map[string]any{"type": "function", "name": "exec_command", "parameters": map[string]any{"type": "object", "properties": map[string]any{"cmd": map[string]any{"type": "string"}}, "required": []any{"cmd"}}}}}
	for _, text := range []string{clientToolProtocolInstructions(source), clientToolProtocolReminder(source), transportRetryHint} {
		if strings.Contains(text, "references=[]") || strings.Contains(text, `"references":[]`) {
			t.Fatalf("prompt still teaches empty references: %s", text)
		}
	}
	if !strings.Contains(transportRetryHint, "references") || strings.Contains(transportRetryHint, "catalog-tool object") {
		t.Fatal("retry hint must ask for the references format")
	}
}
