package basispoints

import (
	"encoding/json"
	"strings"
	"testing"
)

func alphaSearchConfig() Config {
	cfg := defaultConfig()
	cfg.Models = []string{"gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-luna", "gpt-5.6-terra"}
	cfg.AlphaSearchModel = "gpt-6-luna"
	if err := cfg.normalize(); err != nil {
		panic(err)
	}
	return cfg
}

// cpaModelRouteRequest 按 CPA 7.3.17 rpcModelRouteRequest 的线上形态构造：
// pluginapi.ModelRouteRequest 没有 json 标签（字段名即键名，[]byte 为 base64），外加 host_callback_id。
func cpaModelRouteRequest(sourceFormat, model string, providers []string, body []byte) []byte {
	return jsonBytes(map[string]any{
		"Plugin":             map[string]any{"Name": "OpenAI Basis Points", "Version": Version},
		"PluginID":           "oai-basispoints",
		"SourceFormat":       sourceFormat,
		"RequestedModel":     model,
		"Stream":             false,
		"Headers":            map[string][]string{"Content-Type": {"application/json"}},
		"Query":              map[string][]string{},
		"Body":               body,
		"Metadata":           map[string]any{"requested_model": model},
		"AvailableProviders": providers,
		"host_callback_id":   "cb-1",
	})
}

func routeServiceWith(cfg Config) *Service {
	svc := NewService()
	svc.cfg = cfg
	return svc
}

func TestAlphaSearchRouteDecisionMatrix(t *testing.T) {
	svc := routeServiceWith(alphaSearchConfig())
	providers := []string{"codex", "xai"}
	body := []byte(`{"model":"gpt-5.6-sol","commands":{"search_query":[{"q":"news"}]}}`)
	for _, tc := range []struct {
		name, format, model string
		providers           []string
		routed              bool
	}{
		{"bp_model", alphaSearchSourceFormat, "gpt-5.6-sol", providers, true},
		{"bp_model_other_alias", alphaSearchSourceFormat, "gpt-6-astra", providers, true},
		{"thinking_suffix", alphaSearchSourceFormat, "gpt-5.6-terra(xhigh)", providers, true},
		{"credential_prefix", alphaSearchSourceFormat, "team/gpt-5.6-luna", providers, true},
		{"format_case_and_space", " Codex-Alpha-Search ", "gpt-5.6-sol", providers, true},
		{"provider_case", alphaSearchSourceFormat, "gpt-5.6-sol", []string{" CODEX "}, true},
		{"native_model", alphaSearchSourceFormat, "gpt-6-sol", providers, false},
		{"target_model_itself", alphaSearchSourceFormat, "gpt-6-luna", providers, false},
		{"empty_model", alphaSearchSourceFormat, "", providers, false},
		{"only_suffix", alphaSearchSourceFormat, "(xhigh)", providers, false},
		{"responses_request", "openai-response", "gpt-5.6-sol", providers, false},
		{"codex_request", "codex", "gpt-5.6-sol", providers, false},
		{"claude_request", "claude", "gpt-5.6-sol", providers, false},
		{"empty_format", "", "gpt-5.6-sol", providers, false},
		{"no_codex_provider", alphaSearchSourceFormat, "gpt-5.6-sol", []string{"xai"}, false},
		{"no_providers", alphaSearchSourceFormat, "gpt-5.6-sol", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := svc.Handle("model.route", cpaModelRouteRequest(tc.format, tc.model, tc.providers, body))
			if err != nil {
				t.Fatalf("model.route must never fail: %v", err)
			}
			got := result.(map[string]any)
			if got["Handled"] != tc.routed {
				t.Fatalf("Handled=%v want %v (%v)", got["Handled"], tc.routed, got)
			}
			if !tc.routed {
				if len(got) != 1 {
					t.Fatalf("unrouted response must only carry Handled=false: %v", got)
				}
				return
			}
			if got["TargetKind"] != "provider" || got["Target"] != "codex" || got["TargetModel"] != "gpt-6-luna" {
				t.Fatalf("unexpected route: %v", got)
			}
		})
	}
}

func TestAlphaSearchRouteDisabledByDefault(t *testing.T) {
	cfg := defaultConfig()
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	if cfg.AlphaSearchModel != "" {
		t.Fatalf("default must be disabled, got %q", cfg.AlphaSearchModel)
	}
	svc := routeServiceWith(cfg)
	result, err := svc.Handle("model.route", cpaModelRouteRequest(alphaSearchSourceFormat, DefaultModelID, []string{"codex"}, nil))
	if err != nil || result.(map[string]any)["Handled"] != false {
		t.Fatalf("disabled router routed: %v %v", result, err)
	}
	if registration(cfg)["capabilities"].(map[string]any)["model_router"] != false {
		t.Fatal("model_router must not be declared while disabled")
	}
}

// 任何畸形输入都返回「不处理」，绝不返回错误（错误会让 CPA 记警告；panic 会熔断插件）。
func TestAlphaSearchRouteNeverFails(t *testing.T) {
	svc := routeServiceWith(alphaSearchConfig())
	for _, raw := range []string{
		``, `null`, `[]`, `"text"`, `{`, `{"SourceFormat":123}`,
		`{"SourceFormat":"codex-alpha-search","RequestedModel":{"x":1}}`,
		`{"SourceFormat":"codex-alpha-search","RequestedModel":"gpt-5.6-sol","AvailableProviders":"codex"}`,
		`{"SourceFormat":"codex-alpha-search","RequestedModel":"gpt-5.6-sol","Body":"not-base64!"}`,
	} {
		result, err := svc.Handle("model.route", json.RawMessage(raw))
		if err != nil {
			t.Fatalf("%q: model.route returned error %v", raw, err)
		}
		if result.(map[string]any)["Handled"] != false {
			t.Fatalf("%q: malformed request routed: %v", raw, result)
		}
	}
}

func TestAlphaSearchRouteRegistrationFollowsConfig(t *testing.T) {
	svc := NewService()
	register := func(method, yaml string) map[string]any {
		t.Helper()
		result, err := svc.Handle(method, jsonBytes(map[string]any{"config_yaml": []byte(yaml)}))
		if err != nil {
			t.Fatal(err)
		}
		return result.(map[string]any)["capabilities"].(map[string]any)
	}
	base := "data_dir: \"\"\nmodels: [gpt-5.6-sol]\n"
	if caps := register("plugin.register", base); caps["model_router"] != false {
		t.Fatalf("router declared without alpha_search_model: %v", caps["model_router"])
	}
	if caps := register("plugin.reconfigure", base+"alpha_search_model: gpt-6-luna\n"); caps["model_router"] != true {
		t.Fatal("router not declared after enabling alpha_search_model")
	}
	result, err := svc.Handle("model.route", cpaModelRouteRequest(alphaSearchSourceFormat, "gpt-5.6-sol", []string{"codex"}, nil))
	if err != nil || result.(map[string]any)["TargetModel"] != "gpt-6-luna" {
		t.Fatalf("reconfigured router not active: %v %v", result, err)
	}
	// 关闭要显式设为空字符串（删除键会被 settings.json 镜像补回）。
	if caps := register("plugin.reconfigure", base+"alpha_search_model: \"\"\n"); caps["model_router"] != false {
		t.Fatal("router still declared after disabling")
	}
}

func TestAlphaSearchModelValidation(t *testing.T) {
	for _, tc := range []struct {
		value string
		ok    bool
		want  string
	}{
		{"", true, ""},
		{"  gpt-6-luna  ", true, "gpt-6-luna"},
		{"team/gpt-6-luna", true, "team/gpt-6-luna"},
		{"gpt-5.6-sol", false, ""},
		{"gpt-5.6-sol(xhigh)", false, ""},
		{"team/gpt-5.6-sol", false, ""},
		{"gpt 6 luna", false, ""},
		{"gpt-6-luna\n", true, "gpt-6-luna"},
		{"gpt-6\tluna", false, ""},
	} {
		cfg := defaultConfig()
		cfg.Models = []string{"gpt-5.6-sol"}
		cfg.AlphaSearchModel = tc.value
		err := cfg.normalize()
		if tc.ok {
			if err != nil || cfg.AlphaSearchModel != tc.want {
				t.Fatalf("%q: err=%v got=%q want=%q", tc.value, err, cfg.AlphaSearchModel, tc.want)
			}
			continue
		}
		if !isKind(err, "invalid_config") {
			t.Fatalf("%q: want invalid_config, got %v", tc.value, err)
		}
	}
}

// 声明路由后每个请求都会经过 model.route，且带着完整请求体：判定不能随请求体变慢太多。
func BenchmarkAlphaSearchRouteLargeBody(b *testing.B) {
	svc := routeServiceWith(alphaSearchConfig())
	body := []byte(`{"model":"gpt-5.6-sol","input":"` + strings.Repeat("x", 1<<20) + `"}`)
	for _, format := range []string{"openai-response", alphaSearchSourceFormat} {
		raw := cpaModelRouteRequest(format, "gpt-5.6-sol", []string{"codex"}, body)
		b.Run(format, func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if _, err := svc.Handle("model.route", raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
