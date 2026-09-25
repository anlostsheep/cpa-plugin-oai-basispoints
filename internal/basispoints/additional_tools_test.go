package basispoints

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func codexAdditionalTools() map[string]any {
	return map[string]any{
		"type": "additional_tools", "role": "developer", "id": "synthetic_catalog",
		"tools": []any{map[string]any{
			"type": "namespace", "name": "functions",
			"tools": []any{
				map[string]any{"type": "custom", "name": "exec", "format": map[string]any{"type": "text"}},
				map[string]any{"type": "function", "name": "wait", "parameters": map[string]any{
					"type": "object", "required": []any{"cell_id"},
					"properties": map[string]any{"cell_id": map[string]any{"type": "string"}},
				}},
			},
		}},
	}
}

func codexAdditionalToolsSource() map[string]any {
	return map[string]any{
		"model": "gpt-6-astra", "tool_choice": "auto", "parallel_tool_calls": false,
		"input": []any{codexAdditionalTools(), messageItem("user", "List the current directory")},
	}
}

func TestCodexAdditionalToolsCatalogCallAndReplay(t *testing.T) {
	source := codexAdditionalToolsSource()
	specs := clientToolSpecs(source)
	if len(specs) != 2 || specs["functions.exec"].Type != "custom" || specs["functions.wait"].Type != "function" {
		t.Fatalf("Codex tool catalog was not recognized: %#v", specs)
	}
	prepared, err := prepareResponsesBody(source, defaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if catalog := itemText(objectValue(prepared["input"].([]any)[0])["content"]); !strings.Contains(catalog, "functions.exec (custom)") || !strings.Contains(catalog, "functions.wait (function)") {
		t.Fatalf("Codex tools missing from upstream catalog: %q", catalog)
	}

	native := namespaceTestNative(t.Name(), "functions.exec", "pwd")
	_, response, changed, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{native}}), source)
	if err != nil || !changed {
		t.Fatalf("Codex tool relay failed: changed=%t err=%v", changed, err)
	}
	call := objectValue(response["output"].([]any)[0])
	if call["type"] != "custom_tool_call" || call["namespace"] != "functions" || call["name"] != "exec" || call["input"] != "pwd" {
		t.Fatalf("Codex client tool identity changed: %#v", call)
	}
	result := map[string]any{"type": "custom_tool_call_output", "call_id": call["call_id"], "output": "directory listing"}
	replay := translateInputItems([]any{call, result}, clientToolSpecs(source))
	if !reflect.DeepEqual(replay[0], native) || objectValue(replay[1])["type"] != "function_call_output" || objectValue(replay[1])["output"] != result["output"] {
		t.Fatalf("native call or output replay changed: %#v", replay)
	}
}

func TestAdditionalToolsTopLevelAuthorityAndChoice(t *testing.T) {
	source := codexAdditionalToolsSource()
	source["tools"] = []any{}
	if got := clientToolSpecs(source); len(got) != 0 {
		t.Fatalf("explicit empty top-level catalog was bypassed: %#v", got)
	}
	source["tools"] = []any{map[string]any{"type": "function", "name": "local", "parameters": map[string]any{"type": "object"}}}
	if got := clientToolSpecs(source); len(got) != 1 || got["local"].Name != "local" {
		t.Fatalf("top-level catalog was not authoritative: %#v", got)
	}
	delete(source, "tools")
	source["tool_choice"] = "none"
	if got := callableClientToolSpecs(source); len(got) != 0 {
		t.Fatalf("tool_choice=none was bypassed: %#v", got)
	}
	source["tool_choice"] = map[string]any{"type": "allowed_tools", "mode": "auto", "tools": []any{map[string]any{"type": "function", "name": "wait", "namespace": "functions"}}}
	if got := callableClientToolSpecs(source); len(got) != 1 || got["functions.wait"].Name != "wait" {
		t.Fatalf("tool_choice did not constrain additional tools: %#v", got)
	}
	denied := namespaceTestNative(t.Name()+"_denied", "functions.exec", "pwd")
	if _, _, _, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{denied}}), source); err == nil {
		t.Fatal("disallowed additional tool was relayed")
	}
	allowed := namespaceTestNative(t.Name()+"_allowed", "functions.wait", map[string]any{"cell_id": "cell_1"})
	_, response, changed, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{allowed}}), source)
	if err != nil || !changed || objectValue(response["output"].([]any)[0])["arguments"] != `{"cell_id":"cell_1"}` {
		t.Fatalf("allowed function tool was not relayed: response=%#v err=%v", response, err)
	}
	invalid := namespaceTestNative(t.Name()+"_invalid", "functions.wait", map[string]any{})
	if _, _, _, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{invalid}}), source); err == nil {
		t.Fatal("invalid function arguments were relayed")
	}
}

func TestAdditionalToolsRejectsUntrustedAndConflictingCatalogs(t *testing.T) {
	source := codexAdditionalToolsSource()
	malformed := codexAdditionalTools()
	malformed["role"] = "user"
	source["input"] = []any{malformed}
	if got := clientToolSpecs(source); len(got) != 0 {
		t.Fatalf("user item was promoted into a tool catalog: %#v", got)
	}

	first := codexAdditionalTools()
	second := codexAdditionalTools()
	secondTools := second["tools"].([]any)[0].(map[string]any)["tools"].([]any)
	secondTools[0].(map[string]any)["type"] = "function"
	secondTools[0].(map[string]any)["parameters"] = map[string]any{"type": "object"}
	source["input"] = []any{first, second}
	if specs := clientToolSpecs(source); len(specs) != 1 || specs["functions.wait"].Name != "wait" {
		t.Fatalf("conflicting tool was not excluded: %#v", specs)
	}
	native := namespaceTestNative(t.Name(), "functions.exec", "pwd")
	_, _, _, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{native}}), source)
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Kind != "invalid_tool_call" {
		t.Fatalf("conflicting tool was relayed: %v", err)
	}
	if catalog := clientToolProtocolInstructions(source); strings.Contains(catalog, "functions.exec (") {
		t.Fatal("conflicting tool appeared in the upstream catalog")
	}
	source["input"] = []any{first, codexAdditionalTools()}
	if got := clientToolSpecs(source); len(got) != 2 {
		t.Fatalf("identical duplicate changed the available tools: %#v", got)
	}
	if catalog := clientToolProtocolInstructions(source); strings.Count(catalog, "- functions.exec (custom)") != 1 {
		t.Fatalf("identical duplicate appeared more than once: %q", catalog)
	}
}
