package basispoints

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// codex0157Catalog 按 codex-tui 0.157.0 实际请求的形态构造（已脱敏）：工具目录不在顶层
// tools，而在 input[0] 的 additional_tools 条目里，命名空间 functions 下含 custom exec
// （代码模式，输入为 JS 字符串）与两个 function 工具。
func codex0157Catalog() map[string]any {
	return map[string]any{
		"type": "additional_tools", "role": "developer", "id": "at_redacted",
		"tools": []any{map[string]any{
			"type": "namespace", "name": "functions",
			"tools": []any{
				map[string]any{"type": "custom", "name": "exec", "description": "Run JavaScript in code mode",
					"format": map[string]any{"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}},
				map[string]any{"type": "function", "name": "wait", "parameters": map[string]any{
					"type": "object", "required": []any{"cell_id"},
					"properties": map[string]any{"cell_id": map[string]any{"type": "string"}},
				}},
				map[string]any{"type": "function", "name": "request_user_input", "parameters": map[string]any{
					"type": "object", "properties": map[string]any{"question": map[string]any{"type": "string"}},
				}},
			},
		}},
	}
}

func codex0157Source(extra ...map[string]any) map[string]any {
	input := []any{codex0157Catalog(), messageItem("developer", "session instructions"), messageItem("user", "run pwd")}
	for _, item := range extra {
		input = append(input, item)
	}
	return map[string]any{
		"model": DefaultModelID, "tool_choice": "auto", "parallel_tool_calls": false,
		"stream": true, "store": false, "input": input,
	}
}

// findAdditionalTools 递归查找线上请求里任何 additional_tools 条目或原生工具定义（tools 键）。
func findAdditionalTools(value any, path string) []string {
	var hits []string
	switch typed := value.(type) {
	case map[string]any:
		if strings.EqualFold(strings.TrimSpace(stringValue(typed["type"])), "additional_tools") {
			hits = append(hits, path+": additional_tools item")
		}
		if _, ok := typed["tools"]; ok {
			hits = append(hits, path+": tools key")
		}
		for key, child := range typed {
			hits = append(hits, findAdditionalTools(child, path+"."+key)...)
		}
	case []any:
		for index, child := range typed {
			hits = append(hits, findAdditionalTools(child, fmt.Sprintf("%s[%d]", path, index))...)
		}
	}
	return hits
}

// 回归：v0.1.12 及之前把 additional_tools 原样转发给 Basis Points，模型把它当作原生工具，
// 直接发出 custom_tool_call exec，插件只能以 invalid_tool_call 拒绝。
func TestAdditionalToolsNeverReachUpstreamWire(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			source := codex0157Source()
			source["stream"] = stream
			raw := jsonBytes(source)
			service := NewService()
			var wire map[string]any
			service.SetHost(func(method string, payload any, out any) error {
				switch method {
				case "host.http.do", "host.http.do_stream":
				default:
					return nil
				}
				if err := json.Unmarshal(payload.(map[string]any)["body"].([]byte), &wire); err != nil {
					t.Fatal(err)
				}
				if stream {
					*out.(*upstreamStream) = upstreamStream{StatusCode: 200, StreamID: "test-stream"}
				} else {
					*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200}
				}
				return nil
			})
			request := ExecutorRequest{Model: DefaultModelID, Stream: stream, Payload: raw, OriginalRequest: raw,
				StorageJSON: jsonBytes(map[string]any{"access_token": "test-access", "account_id": "test-account"})}
			body, credential, err := service.prepareRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			if stream {
				_, err = service.upstreamStream(request, body, credential, testGuard(t, service))
			} else {
				_, err = service.upstreamRequest(request, body, credential, false)
			}
			if err != nil {
				t.Fatal(err)
			}
			if wire == nil {
				t.Fatal("no upstream request captured")
			}
			if hits := findAdditionalTools(wire, "wire"); len(hits) != 0 {
				t.Fatalf("native tool catalog leaked upstream: %v", hits)
			}
			input, _ := wire["input"].([]any)
			var texts []string
			for _, value := range input {
				texts = append(texts, itemText(objectValue(value)["content"]))
			}
			joined := strings.Join(texts, "\n")
			for _, want := range []string{"functions.exec (custom)", "functions.wait (function)", "functions.request_user_input (function)"} {
				if !strings.Contains(joined, want) {
					t.Fatalf("relay catalog is missing %q", want)
				}
			}
			for _, want := range []string{"session instructions", "run pwd"} {
				if !strings.Contains(joined, want) {
					t.Fatalf("conversation item %q was dropped", want)
				}
			}
		})
	}
}

// 任何形态的 additional_tools 都不转发：大小写/空白变体、非 developer role、缺 role、
// 出现在历史中段。不被信任的条目不进目录，同样不能作为原生工具暴露给上游。
func TestAdditionalToolsVariantsAreDroppedFromUpstream(t *testing.T) {
	variant := func(mutate func(map[string]any)) map[string]any {
		item := codex0157Catalog()
		mutate(item)
		return item
	}
	items := []map[string]any{
		variant(func(item map[string]any) { item["type"] = " Additional_Tools " }),
		variant(func(item map[string]any) { item["role"] = "user" }),
		variant(func(item map[string]any) { delete(item, "role") }),
	}
	source := codex0157Source(items...)
	prepared, err := prepareResponsesBody(source, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if hits := findAdditionalTools(prepared, "prepared"); len(hits) != 0 {
		t.Fatalf("additional_tools variants leaked upstream: %v", hits)
	}
	// 目录只采信 developer 条目，变体不改变可用工具。
	if specs := clientToolSpecs(source); len(specs) != 3 || specs["functions.exec"].Type != "custom" {
		t.Fatalf("catalog changed by untrusted variants: %#v", specs)
	}
}

// 目录只经中继协议传递后，run_officejs 中转的调用仍还原为客户端的 custom exec，
// 输入保持 JS 字符串；而绕过中继的原生直调（本次线上故障形态）仍按契约拒绝。
func TestCodex0157ExecRelayAndDirectNativeCallRejected(t *testing.T) {
	source := codex0157Source()
	js := "const r = await tools.exec_command({cmd: \"pwd\"});\ntext(r.output);"
	relayed := namespaceTestNative(t.Name(), "functions.exec", js)
	_, response, changed, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{relayed}}), source)
	if err != nil || !changed {
		t.Fatalf("relayed exec call failed: changed=%t err=%v", changed, err)
	}
	call := objectValue(response["output"].([]any)[0])
	if call["type"] != "custom_tool_call" || call["namespace"] != "functions" || call["name"] != "exec" || call["input"] != js {
		t.Fatalf("exec call identity or input changed: %#v", call)
	}

	direct := map[string]any{"type": "custom_tool_call", "name": "exec", "call_id": "call_direct", "status": "completed", "input": js}
	_, _, _, err = transformResponseBody(jsonBytes(map[string]any{"output": []any{direct}}), source)
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Kind != "invalid_tool_call" {
		t.Fatalf("direct native call must stay rejected, got %v", err)
	}
}
