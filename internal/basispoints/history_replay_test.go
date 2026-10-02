package basispoints

// 移植自上游 JaxsonWang/cpa-plugin-oai-basispoints a5f698d（history_replay_test.go，MIT），按 fork 的结构适配。

import (
	"reflect"
	"strings"
	"testing"
)

func TestCustomToolCallItemID(t *testing.T) {
	for _, withID := range []bool{true, false} {
		native := namespaceTestNative("custom_id", "functions.exec", "  text('ok');\r\n")
		wantID := "ctc_custom_id"
		if !withID {
			delete(native, "id")
			wantID = "ctc_call_custom_id"
		}
		before := string(jsonBytes(native))
		call, err := extractNativeClientToolCall(native, clientToolSpecs(namespaceTestSource("custom", "exec", "functions")))
		if err != nil {
			t.Fatal(err)
		}
		if call["id"] != wantID || call["call_id"] != native["call_id"] || call["type"] != "custom_tool_call" {
			t.Errorf("withID=%t: identity = %#v, want id=%s and unchanged call_id", withID, call, wantID)
		}
		if string(jsonBytes(native)) != before {
			t.Fatal("native call was mutated")
		}
	}
}

// 历史回放不依赖任何进程内暖状态：每条客户端历史都按条目自身冷重建，相同 call_id 的
// 不同工具/载荷/凭据不会借用先前请求的数据（旧实现按 call_id 命中全局暖缓存）。
func TestHistoryReplayWithoutWarmState(t *testing.T) {
	source := namespaceTestSource("custom", "exec", "functions")
	native := namespaceTestNative(t.Name(), "functions.exec", "  text('done');\r\n")
	_, response, _, err := transformResponseBody(jsonBytes(map[string]any{"status": "completed", "output": []any{native}}), source)
	if err != nil {
		t.Fatal(err)
	}
	call := objectValue(response["output"].([]any)[0])
	// 同 call_id 但载荷不同的历史（例如另一进程/另一凭据的同名调用）必须重建为自身载荷。
	reuse := cloneObject(call)
	reuse["input"] = "  text('other');\r\n"
	result := map[string]any{"type": "custom_tool_call_output", "call_id": reuse["call_id"], "output": "already executed"}
	replay, err := translateInputItems([]any{reuse, result})
	if err != nil {
		t.Fatal(err)
	}
	replayedCall, replayedOutput := objectValue(replay[0]), objectValue(replay[1])
	route, code := rebuiltRoute(t, replayedCall)
	if route != "functions.exec" || code != reuse["input"] {
		t.Fatalf("rebuild borrowed earlier request data: route=%q code=%q", route, code)
	}
	if replayedCall["call_id"] != call["call_id"] || replayedCall["id"] != functionItemID(stringValue(call["call_id"])) {
		t.Fatalf("rebuild lost the call identity: %#v", replayedCall)
	}
	if replayedOutput["type"] != "function_call_output" || replayedOutput["call_id"] != call["call_id"] || replayedOutput["output"] != result["output"] {
		t.Fatalf("rebuild lost result pairing: %#v", replay)
	}
	for _, key := range []string{"name", "namespace"} {
		if _, present := replayedOutput[key]; present {
			t.Fatalf("client %s leaked into output", key)
		}
	}
}

// 客户端对象参数从原始 JSON 解码时保留 json.Number：剥离条目元数据是顶层浅拷贝，不得经过
// JSON 往返，超出 float64 精度的大整数必须逐字进入重建的中转载荷；无元数据路径同样保真，
// 且 prepareResponsesBody 不修改客户端 source。
func TestHistoryObjectArgumentsPrecisionPreserved(t *testing.T) {
	const raw = `{"model":"gpt-6-astra-basispoints","input":[{"type":"function_call","name":"old","call_id":"call_precision","arguments":{"id":9007199254740993,"nested":{"id":18446744073709551615}},"internal_chat_message_metadata_passthrough":{"x":true}}]}`
	for _, tc := range []struct {
		name     string
		metadata bool
	}{
		{"with_metadata", true},
		{"without_metadata", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, reason := parseRelayObject(raw)
			if reason != "" {
				t.Fatal(reason)
			}
			if !tc.metadata {
				delete(objectValue(source["input"].([]any)[0]), "internal_chat_message_metadata_passthrough")
			}
			before := string(jsonBytes(source))
			body, err := prepareResponsesBody(source, defaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			if after := string(jsonBytes(source)); after != before {
				t.Fatal("client source was mutated")
			}
			found := false
			for _, value := range body["input"].([]any) {
				item := objectValue(value)
				if item["type"] != "function_call" || item["name"] != transportName {
					continue
				}
				found = true
				if _, present := item["internal_chat_message_metadata_passthrough"]; present {
					t.Fatal("metadata leaked into the upstream body")
				}
				_, code := rebuiltRoute(t, item)
				if !strings.Contains(code, "9007199254740993") || !strings.Contains(code, "18446744073709551615") {
					t.Fatalf("object history integers changed: %s", code)
				}
			}
			if !found {
				t.Fatal("historical call missing from translated body")
			}
		})
	}
}

// 直接提交的 native 历史（传输名条目）按原文保留：剥离元数据是顶层浅拷贝，字符串载荷逐字
// 不变，对象载荷同样保留解码时的 json.Number 大整数；元数据不进入上游 body，source 不被修改。
func TestHistoryNativeItemPreservedWithMetadata(t *testing.T) {
	argsText := string(jsonBytes(map[string]any{"summary": "s", "code": `{"id":9007199254740993}`, "references": []any{"exec"}}))
	raw := `{"model":"gpt-6-astra-basispoints","input":[` +
		`{"type":"function_call","id":"fc_native_str","call_id":"call_native_str","name":"run_officejs","arguments":` + string(jsonBytes(argsText)) + `,"status":"completed","internal_chat_message_metadata_passthrough":{"x":true}},` +
		`{"type":"function_call","id":"fc_native_obj","call_id":"call_native_obj","name":"run_officejs","arguments":{"id":9007199254740993,"nested":{"id":18446744073709551615}},"status":"completed","internal_chat_message_metadata_passthrough":{"x":true}}` +
		`]}`
	source, reason := parseRelayObject(raw)
	if reason != "" {
		t.Fatal(reason)
	}
	before := string(jsonBytes(source))
	body, err := prepareResponsesBody(source, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if after := string(jsonBytes(source)); after != before {
		t.Fatal("client source was mutated")
	}
	seen := map[string]map[string]any{}
	for _, value := range body["input"].([]any) {
		item := objectValue(value)
		if item["type"] == "function_call" && isTransportName(stringValue(item["name"])) {
			seen[stringValue(item["call_id"])] = item
		}
	}
	if len(seen) != 2 {
		t.Fatalf("native history items missing from translated body: %d", len(seen))
	}
	strItem := seen["call_native_str"]
	if strItem["arguments"] != argsText || strItem["id"] != "fc_native_str" || strItem["status"] != "completed" {
		t.Fatalf("native string history changed: %#v", strItem)
	}
	objText := string(jsonBytes(seen["call_native_obj"]["arguments"]))
	if !strings.Contains(objText, "9007199254740993") || !strings.Contains(objText, "18446744073709551615") {
		t.Fatalf("native object payload integers changed: %s", objText)
	}
	for _, item := range seen {
		if _, present := item["internal_chat_message_metadata_passthrough"]; present {
			t.Fatal("metadata leaked into the upstream body")
		}
	}
}

func TestHistoryReplayWithoutCurrentToolCatalog(t *testing.T) {
	for _, kind := range []string{"function", "custom"} {
		for _, namespace := range []string{"", "functions"} {
			for _, catalog := range []string{"absent", "empty", "unrelated"} {
				t.Run(kind+"/"+namespace+"/"+catalog, func(t *testing.T) {
					callID := "call_" + t.Name()
					call := map[string]any{"type": "function_call", "id": "fc_old", "call_id": callID, "name": "exec", "arguments": `{"cmd":"printf OK","id":9007199254740993}`}
					outputType, field := "function_call_output", "arguments"
					if kind == "custom" {
						// Old plugin versions saved custom calls with an fc_ item ID.
						call["type"], call["input"] = "custom_tool_call", "  text(\"already executed\");\r\n\t"
						delete(call, "arguments")
						outputType, field = "custom_tool_call_output", "input"
					}
					key := "exec"
					if namespace != "" {
						call["namespace"] = namespace
						key = namespace + ".exec"
					}
					output := []any{map[string]any{"type": "input_text", "text": "  already executed\n"}}
					result := map[string]any{"type": outputType, "id": "ctco_old", "call_id": callID, "name": "exec", "namespace": namespace, "output": output}
					source := map[string]any{"model": DefaultModelID, "input": []any{call, result, messageItem("user", "Summarize this conversation.")}}
					if catalog == "empty" {
						source["tools"] = []any{}
					} else if catalog == "unrelated" {
						source["tools"] = namespaceTestSource("function", "other", "")["tools"]
					}
					// 该 call_id 从未被本进程转换过：冷重建完全来自历史条目自身。
					before := string(jsonBytes(source))
					body, err := prepareResponsesBody(source, defaultConfig())
					if err != nil {
						t.Fatal(err)
					}
					items := body["input"].([]any)
					replayedCall := objectValue(items[len(items)-3])
					envelope, err := transportEnvelope(replayedCall)
					if err != nil {
						t.Fatalf("history was not restored to native transport: %#v: %v", replayedCall, err)
					}
					if envelope.tool != key || envelope.payload != call[field] || replayedCall["call_id"] != callID || !strings.HasPrefix(stringValue(replayedCall["id"]), "fc_") {
						t.Fatalf("history identity or payload changed: %#v", replayedCall)
					}
					replayedOutput := objectValue(items[len(items)-2])
					if replayedOutput["type"] != "function_call_output" || replayedOutput["call_id"] != callID || !reflect.DeepEqual(replayedOutput["output"], output) {
						t.Fatalf("history output changed: %#v", replayedOutput)
					}
					for _, key := range []string{"name", "namespace"} {
						if _, exists := replayedOutput[key]; exists {
							t.Fatalf("client %s leaked into output", key)
						}
					}
					if string(jsonBytes(source)) != before {
						t.Fatal("history was mutated")
					}
				})
			}
		}
	}
}
