package basispoints

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func codexStorage() []byte {
	return jsonBytes(map[string]any{"type": "codex", "access_token": "test-access", "account_id": "test-account"})
}

// P2e（共享模式）：标记文件同时返回 native codex 与 oai-basispoints 两条记录，原生模型照常可用；
// 两条记录都不携带 refresh_token（Metadata 与 StorageJSON），CPA 因此永远不会轮换该凭据，
// 外部刷新脚本是唯一刷新方。多条记录由 CPA 标为 plugin_virtual、不写回；只有虚拟记录
// 额外标 runtime_only，native 记录不标（管理面板对 runtime_only 凭据只读，会隐藏额度刷新）。
// 未标记（含大小写不同）的文件不接管，交还 CPA 原生加载器。
func TestDedicatedAuthFileSharedMode(t *testing.T) {
	cfg := defaultConfig()
	cfg.DedicatedAuthFiles = []string{"excel-only.json"}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	dedicated := cfg.dedicatedSet()
	raw := jsonBytes(map[string]any{"type": "codex", "access_token": "test-access", "account_id": "test-account", "refresh_token": "rt-secret", "refreshToken": "rt-secret-2"})

	resp, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "excel-only.json", RawJSON: raw}), dedicated)
	if err != nil {
		t.Fatal(err)
	}
	auths, ok := resp["Auths"].([]any)
	if resp["Handled"] != true || !ok || len(auths) != 2 {
		t.Fatalf("marked file must expand into native + virtual: %#v", resp)
	}
	if objectValue(auths[0])["Provider"] != AuthProviderID || objectValue(auths[1])["Provider"] != Provider {
		t.Fatalf("want [native codex, oai-basispoints], got %v / %v", objectValue(auths[0])["Provider"], objectValue(auths[1])["Provider"])
	}
	nativeAttrs, _ := objectValue(auths[0])["Attributes"].(map[string]string)
	if nativeAttrs["runtime_only"] == "true" {
		t.Fatalf("native record must not be runtime_only (panels hide quota refresh for it): %#v", nativeAttrs)
	}
	virtualAttrs, _ := objectValue(auths[1])["Attributes"].(map[string]string)
	if virtualAttrs["runtime_only"] != "true" {
		t.Fatalf("virtual record must stay runtime_only: %#v", virtualAttrs)
	}
	for _, a := range auths {
		record := objectValue(a)
		metadata := objectValue(record["Metadata"])
		for _, key := range refreshTokenKeys {
			if _, present := metadata[key]; present {
				t.Fatalf("%s record carries %s in Metadata; CPA would rotate it", record["Provider"], key)
			}
		}
		if strings.Contains(string(record["StorageJSON"].([]byte)), "rt-secret") {
			t.Fatalf("%s record carries the refresh token in StorageJSON", record["Provider"])
		}
	}
	if metadata := objectValue(objectValue(auths[0])["Metadata"]); metadata["access_token"] != "test-access" {
		t.Fatal("native record must keep the access token so native Codex keeps working")
	}

	// 区分大小写：与外部刷新脚本的标记契约一致，大小写不同即未标记 → 交还 CPA。
	for _, name := range []string{"Excel-Only.json", "other.json"} {
		other, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: name, RawJSON: raw}), dedicated)
		if err != nil {
			t.Fatal(err)
		}
		if other["Handled"] != false {
			t.Fatalf("%s is unmarked and must be left to CPA's native loader: %#v", name, other)
		}
	}
}

// 标记契约校验与外部刷新脚本一致：非法条目/重复即配置错误，不静默跳过。
func TestDedicatedAuthFilesStrictValidation(t *testing.T) {
	for _, bad := range [][]string{
		{" excel.json"},
		{"excel.json "},
		{"sub/excel.json"},
		{"sub\\excel.json"},
		{".."},
		{""},
		{"excel.json", "excel.json"},
	} {
		cfg := defaultConfig()
		cfg.DedicatedAuthFiles = bad
		if err := cfg.normalize(); err == nil {
			t.Fatalf("expected invalid_config for %q", bad)
		}
	}
	cfg := defaultConfig()
	cfg.DedicatedAuthFiles = []string{}
	if err := cfg.normalize(); err != nil || cfg.DedicatedAuthFiles != nil {
		t.Fatalf("empty list should normalize to nil: %#v err=%v", cfg.DedicatedAuthFiles, err)
	}
}

func TestServiceAuthParseHonorsDedicatedConfig(t *testing.T) {
	svc := NewService()
	configYAML := []byte("data_dir: \"\"\ndedicated_auth_files:\n  - excel-only.json\n")
	if _, err := svc.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": configYAML})); err != nil {
		t.Fatal(err)
	}
	resp, err := svc.Handle("auth.parse", jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "excel-only.json", RawJSON: codexStorage()}))
	if err != nil {
		t.Fatal(err)
	}
	if auths, _ := objectValue(resp)["Auths"].([]any); len(auths) != 2 || objectValue(auths[0])["Provider"] != AuthProviderID || objectValue(auths[1])["Provider"] != Provider {
		t.Fatalf("marked file not expanded into shared mode through Handle: %#v", resp)
	}
	unmarked, err := svc.Handle("auth.parse", jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "other.json", RawJSON: codexStorage()}))
	if err != nil {
		t.Fatal(err)
	}
	if objectValue(unmarked)["Handled"] != false {
		t.Fatalf("unmarked file must be left to CPA through Handle: %#v", unmarked)
	}
}

// P2b：请求头对齐网页版真实值，逐字段断言。
func TestAuthHeadersUseWebProfile(t *testing.T) {
	cfg := defaultConfig()
	c := credential{AccessToken: "secret-token", AccountID: "acct-1", AuthMode: "chatgpt", AccountUserID: "user-9__acct-1"}
	h := authHeaders(c, cfg, true)

	// 直接访问 map（键采用 HAR 首字母大写形式，不走 Header.Get 的规范化）。
	want := map[string]string{
		"X-Openai-Internal-Basispoints-Client-Runtime":        "web",
		"X-Openai-Internal-Basispoints-Client-Platform-Class": "OfficeOnline",
		"X-Openai-Internal-Basispoints-Office-Platform":       "OfficeOnline",
		"X-Openai-Internal-Basispoints-Browser-Name":          "chrome",
		"X-Openai-Internal-Basispoints-Browser-UA-Platform":   "macOS",
		"X-Openai-Internal-Basispoints-Browser-UA-Mobile":     "false",
		"X-Openai-Internal-Basispoints-Browser-UA-Brands":     "Google Chrome,Not_A Brand,Chromium",
		"X-Openai-Account-User-Id":                            "user-9__acct-1",
		"X-Openai-Internal-Basispoints-Client-Product":        "basispoints-excel-plugin",
		"X-Openai-Internal-Basispoints-Client-Editor":         "excel",
		"X-Openai-Internal-Basispoints-Client-Host":           "office",
		"X-Openai-Internal-Basispoints-Client-Agent-Profile":  "excel",
		"X-Openai-Internal-Basispoints-Office-Host":           "Excel",
	}
	for key, value := range want {
		if got := h[key]; len(got) != 1 || got[0] != value {
			t.Fatalf("header %q = %#v, want %q", key, got, value)
		}
	}
	if ua := h["User-Agent"]; len(ua) != 1 || ua[0] != DefaultUserAgent {
		t.Fatalf("User-Agent = %#v, want configured default", h["User-Agent"])
	}
	// x-stainless-* 保持不动。
	if h["X-Stainless-Runtime"][0] != "browser:chrome" {
		t.Fatal("x-stainless-* changed unexpectedly")
	}
	// 不发 referer（大小写不敏感）。
	if len(h.Values("Referer")) != 0 {
		t.Fatal("referer must not be sent")
	}
}

// P2b：取不到复合用户标识时跳过 x-openai-account-user-id，而不是发空值。
func TestAuthHeadersOmitEmptyAccountUserID(t *testing.T) {
	h := authHeaders(credential{AccessToken: "t", AccountID: "a"}, defaultConfig(), false)
	if _, present := h["X-Openai-Account-User-Id"]; present {
		t.Fatal("empty account-user-id should be omitted")
	}
}

// P2b：User-Agent、ua_platform、ua_brands 来自配置。
func TestAuthHeadersHonorConfiguredUA(t *testing.T) {
	cfg := defaultConfig()
	cfg.UserAgent = "Custom-UA/1.0"
	cfg.UAPlatform = "Windows"
	cfg.UABrands = "Brand A,Brand B"
	h := authHeaders(credential{AccessToken: "t", AccountID: "a"}, cfg, false)
	if h["User-Agent"][0] != "Custom-UA/1.0" {
		t.Fatalf("UA not from config: %#v", h["User-Agent"])
	}
	if h["X-Openai-Internal-Basispoints-Browser-UA-Platform"][0] != "Windows" {
		t.Fatal("ua_platform not from config")
	}
	if h["X-Openai-Internal-Basispoints-Browser-UA-Brands"][0] != "Brand A,Brand B" {
		t.Fatal("ua_brands not from config")
	}
}

// P2a：错误分类按来源。表驱动覆盖客户端/上游/工具的不同来源。
func TestErrorClassificationBySource(t *testing.T) {
	storage := codexStorage()

	// 客户端请求体坏 JSON → 4xx（客户端问题）。
	t.Run("client_bad_json", func(t *testing.T) {
		req := ExecutorRequest{Model: DefaultModelID, StorageJSON: storage, OriginalRequest: []byte("{not json")}
		_, _, err := NewService().prepareRequest(req)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status < 400 || apiErr.Status >= 500 {
			t.Fatalf("client bad JSON must be 4xx, got %v", err)
		}
	})

	// 上游返回坏 JSON（非 2xx 状态 + 非 JSON 正文）→ 保留上游状态。
	t.Run("upstream_bad_json_preserves_status", func(t *testing.T) {
		for _, status := range []int{400, 401, 403, 422, 429, 500} {
			err := upstreamRequestError(status, []byte("<<< not json >>>"), map[string]any{}, credential{})
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Status != status {
				t.Fatalf("status %d not preserved: %v", status, err)
			}
		}
	})

	// 上游 401 / 429 → 上游语义（不改写为 5xx，也不改写为客户端 4xx 之外的东西）。
	t.Run("upstream_auth_and_rate_limit", func(t *testing.T) {
		for _, status := range []int{401, 429} {
			err := upstreamRequestError(status, jsonBytes(map[string]any{"error": map[string]any{"message": "upstream says no"}}), map[string]any{}, credential{})
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Status != status {
				t.Fatalf("upstream %d semantics lost: %v", status, err)
			}
		}
	})

	// run_officejs 的 code 无法解析 → 422 invalid_tool_code（非 5xx，避免冷却凭据）。
	t.Run("invalid_tool_code_is_422", func(t *testing.T) {
		source := namespaceTestSource("function", "js", "mcp__node_repl")
		native := map[string]any{
			"type": "function_call", "name": transportName, "id": "fc_x", "call_id": "call_x",
			"arguments": string(jsonBytes(map[string]any{"code": `{"tool":"mcp__node_repl.js","args":{}} trailing-garbage`})),
		}
		_, _, _, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{native}}), source)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status != 422 || apiErr.Kind != "invalid_tool_code" {
			t.Fatalf("unparseable tool code must be 422 invalid_tool_code, got %v", err)
		}
	})

	// 容错解析：双重 JSON 转义的 code 应被恢复（一次纯解析重试），而非报错。
	t.Run("tolerant_double_encoded_code", func(t *testing.T) {
		source := namespaceTestSource("function", "js", "mcp__node_repl")
		inner := string(jsonBytes(map[string]any{"tool": "mcp__node_repl.js", "args": map[string]any{"code": "ok"}}))
		doubleEncoded, _ := json.Marshal(inner) // 把 code 再套一层 JSON 字符串转义
		native := map[string]any{
			"type": "function_call", "name": transportName, "id": "fc_y", "call_id": "call_y",
			"arguments": string(jsonBytes(map[string]any{"code": string(doubleEncoded)})),
		}
		_, response, changed, err := transformResponseBody(jsonBytes(map[string]any{"output": []any{native}}), source)
		if err != nil || !changed {
			t.Fatalf("double-encoded code should be recovered: changed=%t err=%v", changed, err)
		}
		call := objectValue(response["output"].([]any)[0])
		if call["name"] != "js" || !strings.Contains(stringValue(call["arguments"]), "\"code\":\"ok\"") {
			t.Fatalf("recovered call malformed: %#v", call)
		}
	})
}

// 面板（YAML）是单一来源：YAML 中出现的键覆盖旧的 settings.json 镜像，镜像随之更新；
// YAML 未提供的键才从镜像补齐。外部刷新脚本读取该镜像中的 dedicated_auth_files。
func TestYAMLOverridesStaleSettingsMirror(t *testing.T) {
	dir := t.TempDir()
	first := NewService()
	if _, err := first.Handle("plugin.register", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: " + dir + "\ndedicated_auth_files:\n  - old.json\nuser_agent: custom-ua\n")})); err != nil {
		t.Fatal(err)
	}
	// 面板改了专用列表，但没提供 user_agent。
	second := NewService()
	if _, err := second.Handle("plugin.reconfigure", jsonBytes(map[string]any{"config_yaml": []byte("data_dir: " + dir + "\ndedicated_auth_files:\n  - new.json\n")})); err != nil {
		t.Fatal(err)
	}
	cfg := second.config()
	if len(cfg.DedicatedAuthFiles) != 1 || cfg.DedicatedAuthFiles[0] != "new.json" {
		t.Fatalf("panel edit lost to stale mirror: %#v", cfg.DedicatedAuthFiles)
	}
	if cfg.UserAgent != "custom-ua" {
		t.Fatalf("key absent from YAML should be filled from mirror, got %q", cfg.UserAgent)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var mirror map[string]any
	if err := json.Unmarshal(raw, &mirror); err != nil {
		t.Fatal(err)
	}
	files, _ := mirror["dedicated_auth_files"].([]any)
	if len(files) != 1 || files[0] != "new.json" {
		t.Fatalf("mirror not updated to panel value: %#v", mirror["dedicated_auth_files"])
	}
}
