package basispoints

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestAuthParsePreservesNativeCodexCredentials(t *testing.T) {
	root := map[string]any{
		"type": "codex", "access_token": "test-access", "refresh_token": "test-refresh",
		"account_id": "test-account", "expired": "2030-01-01T00:00:00Z",
		"last_refresh": "2026-09-24T00:00:00Z", "priority": float64(7),
		"custom_field": map[string]any{"enabled": true},
	}
	root["id_token"] = "e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOiB7ImNoYXRncHRfcGxhbl90eXBlIjogInBybyJ9fQ."
	root["note"] = "keep note"
	raw := jsonBytes(root)

	// 未标记：不接管，交还 CPA 原生加载器（CPA 自己刷新并写回）。
	unmarked, err := authParse(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "native.json", RawJSON: raw}))
	if err != nil {
		t.Fatal(err)
	}
	if unmarked["Handled"] != false {
		t.Fatalf("unmarked codex file must be left to CPA's native loader: %#v", unmarked)
	}

	// 标记（共享模式）：native 记录保留全部账号设置，只剔除 refresh_token。
	response, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "native.json", RawJSON: raw}), map[string]bool{"native.json": true})
	if err != nil {
		t.Fatal(err)
	}
	native := objectValue(response["Auths"].([]any)[0])
	metadata := objectValue(native["Metadata"])
	attributes := native["Attributes"].(map[string]string)
	if attributes["plan_type"] != "pro" || attributes["priority"] != "7" || attributes["note"] != "keep note" {
		t.Fatal("native account settings changed")
	}
	for key, want := range root {
		if key == "refresh_token" {
			if _, present := metadata[key]; present {
				t.Fatal("shared-mode native record must not carry refresh_token")
			}
			continue
		}
		if !reflect.DeepEqual(metadata[key], want) {
			t.Errorf("native field %q was not preserved", key)
		}
	}
	var storage map[string]any
	if err := json.Unmarshal(native["StorageJSON"].([]byte), &storage); err != nil {
		t.Fatal(err)
	}
	if _, present := storage["refresh_token"]; present || storage["access_token"] != "test-access" || storage["note"] != "keep note" {
		t.Fatalf("native storage must equal the source minus refresh_token: %#v", storage)
	}
}

func TestModelRegistrationAdvertisesImagesAndReasoningLevels(t *testing.T) {
	response := modelRegistration(defaultConfig())
	for _, model := range response["Models"].([]map[string]any) {
		if !reflect.DeepEqual(model["SupportedInputModalities"], []string{"text", "image"}) {
			t.Fatal("image modality missing")
		}
		thinking := objectValue(model["Thinking"])
		if !reflect.DeepEqual(thinking["Levels"], []string{"low", "medium", "high", "xhigh", "max", "ultra"}) {
			t.Fatal("reasoning levels missing")
		}
	}
}

func TestPrepareRequestPreservesRemoteImagesAndReasoningEffort(t *testing.T) {
	images := []string{"https://example.com/image.png"}
	efforts := []struct{ input, want string }{
		{"low", "low"}, {"medium", "medium"}, {"high", "high"}, {"xhigh", "xhigh"},
		{"max", "xhigh"}, {"ultra", "ultra"}, {" MAX ", "xhigh"}, {"", "medium"},
	}
	for _, image := range images {
		for _, effort := range efforts {
			for _, nested := range []bool{true, false} {
				content := []any{map[string]any{"type": "input_text", "text": "Describe this image"}, map[string]any{"type": "input_image", "image_url": image, "detail": "high"}}
				message := map[string]any{"role": "user", "content": content}
				source := map[string]any{"model": DefaultModelID, "input": []any{message}}
				if nested {
					source["reasoning"] = map[string]any{"effort": effort.input}
				} else {
					source["reasoning_effort"] = effort.input
				}
				raw := jsonBytes(source)
				for _, original := range []bool{true, false} {
					request := ExecutorRequest{Model: DefaultModelID, Payload: raw, StorageJSON: jsonBytes(map[string]any{"access_token": "test-access", "account_id": "test-account"})}
					if original {
						request.OriginalRequest = raw
					}
					body, _, err := NewService().prepareRequest(request)
					if err != nil {
						t.Fatal(err)
					}
					if got := body["reasoning_effort"]; got != effort.want {
						t.Fatalf("effort %q = %v, want %s", effort.input, got, effort.want)
					}
					items := body["input"].([]any)
					if !reflect.DeepEqual(items[len(items)-1], message) {
						t.Fatal("image input changed during request preparation")
					}
				}
			}
		}
	}
}

func TestAuthParseRejectsMalformedNativeStorage(t *testing.T) {
	raw := []byte(`{"type":"codex","access_token":"test","account_id":"test"} trailing`)
	// 仅标记文件由插件展开（未标记文件交还 CPA 原生加载器，由 CPA 自行报错）。
	_, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "invalid.json", RawJSON: raw}), map[string]bool{"invalid.json": true})
	if err == nil {
		t.Fatal("malformed native credential was accepted")
	}
}

func TestModelRegistrationUsesExistingResponseInterceptor(t *testing.T) {
	cfg := defaultConfig()
	capabilities := registration(cfg)["capabilities"].(map[string]any)
	if capabilities["response_interceptor"] != true {
		t.Fatal("model catalog interceptor is not registered")
	}
	for _, model := range modelRegistration(cfg)["Models"].([]map[string]any) {
		for _, field := range []string{"MetadataModelID", "SupportedServiceTiers", "ContextLength"} {
			if _, exists := model[field]; exists {
				t.Fatalf("unexpected host metadata dependency: %s", field)
			}
		}
	}
}
