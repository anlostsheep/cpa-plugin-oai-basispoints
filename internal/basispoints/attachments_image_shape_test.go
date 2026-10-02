package basispoints

// 0.1.17.1：用户消息图片的引用形状（#17）、冲突预检、按字节识别后的缓存复用，以及图片请求错误的
// 位置诊断与脱敏。前三个测试移植自上游 08e349c（MIT），按 fork 适配宿主桩；其余为 fork 新增。

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestImageFileReferencesUseOnlyTypeAndFileID(t *testing.T) {
	for _, detail := range []any{nil, "auto", "low", "high", "original"} {
		for _, uploaded := range []bool{false, true} {
			t.Run(fmt.Sprintf("detail=%v/uploaded=%t", detail, uploaded), func(t *testing.T) {
				part := map[string]any{"type": "input_image", "file_id": "file-existing", "extra": "client-metadata"}
				if detail != nil {
					part["detail"] = detail
				}
				wantID := "file-existing"
				if uploaded {
					delete(part, "file_id")
					part["image_url"], _ = testImageDataURL(t)
					wantID = "file-uploaded"
				}
				request := imageRequest(part)
				before := string(request.Payload)
				service := NewService()
				uploads := 0
				service.SetHost(opTolerant(func(method string, payload any, out any) error {
					uploads++
					if !uploaded || method != "host.http.do" || !strings.HasSuffix(payload.(map[string]any)["url"].(string), "/attachments") {
						t.Fatal("unexpected host call")
					}
					*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Body: jsonBytes(map[string]any{"openai_file_id": wantID})}
					return nil
				}))
				body, _, err := service.prepareRequest(request)
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]any{"type": "input_image", "file_id": wantID}
				if got := objectValue(lastUserContent(body)[0]); !reflect.DeepEqual(got, want) {
					t.Fatalf("image reference = %v, want %v", got, want)
				}
				if (uploaded && uploads != 1) || (!uploaded && uploads != 0) || string(request.Payload) != before {
					t.Fatal("image normalization changed source or upload count")
				}
			})
		}
	}
}

func TestImageFileReferencesRejectConflictingURLs(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	for _, imageURL := range []string{dataURL, "https://private.example/image.png?secret=private-token"} {
		service := NewService()
		service.SetHost(opTolerant(func(string, any, any) error {
			t.Fatal("conflicting image references reached network")
			return nil
		}))
		request := imageRequest(map[string]any{"type": "input_image", "image_url": imageURL, "file_id": "file-private-id"})
		_, _, err := service.prepareRequest(request)
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status != 400 || apiErr.Kind != "invalid_image" || !strings.Contains(err.Error(), ".content[0]") {
			t.Fatalf("expected indexed invalid image error, got %v", err)
		}
		if strings.Contains(err.Error(), imageURL) || strings.Contains(err.Error(), "private") {
			t.Fatal("conflicting image diagnostic exposed private input")
		}
	}
}

// 全部待处理图片先本地验证：前图有效、后图字节格式坏，同样零上传/零生成（不是先上传前图再报错）。
func TestInvalidLaterImageUploadsNothing(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	badDataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte("private-not-an-image"))
	valid := map[string]any{"type": "input_image", "image_url": dataURL}
	broken := map[string]any{"type": "input_image", "image_url": badDataURL}
	// 位置诊断按上游看到的 input（含前置 developer 目录消息）计数。
	for name, tc := range map[string]struct {
		input    []any
		position string
	}{
		"same_message": {
			input:    []any{map[string]any{"role": "user", "content": []any{valid, broken}}},
			position: "input[1].content[1]",
		},
		"later_message": {
			input: []any{
				map[string]any{"role": "user", "content": []any{valid}},
				map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "ok"}}},
				map[string]any{"role": "user", "content": []any{broken}},
			},
			position: "input[3].content[0]",
		},
	} {
		t.Run(name, func(t *testing.T) {
			service := NewService()
			calls := 0
			service.SetHost(opTolerant(func(string, any, any) error {
				calls++
				return errors.New("no upload or Responses call is allowed")
			}))
			request := imageRequest()
			request.Payload = jsonBytes(map[string]any{"input": tc.input})
			_, _, err := service.prepareRequest(request)
			if !isKind(err, "invalid_image") || calls != 0 {
				t.Fatalf("invalid image must be rejected before any host call: calls=%d err=%v", calls, err)
			}
			if !strings.Contains(err.Error(), tc.position) {
				t.Fatalf("missing position %q in %v", tc.position, err)
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), base64.StdEncoding.EncodeToString([]byte("private-not-an-image"))) {
				t.Fatal("invalid image diagnostic exposed input data")
			}
		})
	}
}

// 冲突预检：前面图片合法、冲突出现在后面或另一条用户消息里，都在任何附件上传前拒绝（上游逐张
// 处理会先上传前面的图片）。
func TestImageConflictIsRejectedBeforeAnyUpload(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	valid := map[string]any{"type": "input_image", "image_url": dataURL}
	conflict := map[string]any{"type": "input_image", "image_url": dataURL, "file_id": "file-private-id"}
	for name, input := range map[string][]any{
		"same_message": {map[string]any{"role": "user", "content": []any{valid, conflict}}},
		"later_message": {
			map[string]any{"role": "user", "content": []any{valid}},
			map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "ok"}}},
			map[string]any{"role": "user", "content": []any{conflict}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			service := NewService()
			calls := 0
			service.SetHost(opTolerant(func(string, any, any) error {
				calls++
				return errors.New("no upload or Responses call is allowed")
			}))
			request := imageRequest()
			request.Payload = jsonBytes(map[string]any{"input": input})
			_, _, err := service.prepareRequest(request)
			if !isKind(err, "invalid_image") || calls != 0 {
				t.Fatalf("conflict must be rejected before any host call: calls=%d err=%v", calls, err)
			}
		})
	}
}

// 同一份字节换了不同的 image/* 声明，识别后是同一格式，命中同一个缓存条目，只上传一次。
func TestImageCacheReusesAcrossDeclaredTypes(t *testing.T) {
	_, pngData := testImageDataURL(t)
	encoded := base64.StdEncoding.EncodeToString(pngData)
	service := NewService()
	uploads := 0
	service.SetHost(opTolerant(func(method string, payload any, out any) error {
		uploads++
		*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Body: jsonBytes(map[string]any{"openai_file_id": "file-cached"})}
		return nil
	}))
	for _, declared := range []string{"image/png", "image/x-png", "image/unknown"} {
		body, _, err := service.prepareRequest(imageRequest(map[string]any{"type": "input_image", "image_url": "data:" + declared + ";base64," + encoded}))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(objectValue(lastUserContent(body)[0]), map[string]any{"type": "input_image", "file_id": "file-cached"}) {
			t.Fatal("cached reference wrong")
		}
	}
	if uploads != 1 {
		t.Fatalf("same bytes under different declared types must upload once, got %d", uploads)
	}
}

// 非 image 声明仍被门禁拒绝，不因字节是合法 PNG 而放行。
func TestNonImageDeclaredTypeStillRejected(t *testing.T) {
	_, pngData := testImageDataURL(t)
	service := NewService()
	service.SetHost(opTolerant(func(string, any, any) error {
		t.Fatal("non-image declared type reached network")
		return nil
	}))
	_, _, err := service.prepareRequest(imageRequest(map[string]any{"type": "input_image", "image_url": "data:application/octet-stream;base64," + base64.StdEncoding.EncodeToString(pngData)}))
	if !isKind(err, "invalid_image") {
		t.Fatalf("want invalid_image, got %v", err)
	}
}

func TestUpstreamImageDiagnosticListsOnlyBoundedLocations(t *testing.T) {
	parts := []any{
		map[string]any{"type": "input_text", "text": "private-prompt"},
		map[string]any{"type": "input_image", "file_id": "file-private-id"},
		map[string]any{"type": "input_image", "image_url": "https://private.example/image.png?secret=private-token"},
		map[string]any{"type": "input_image", "image_url": "data:image/png;base64,private-image-data"},
		map[string]any{"type": "input_image"},
	}
	for i := 0; i < 13; i++ {
		parts = append(parts, map[string]any{"type": "input_image", "file_id": "file-private-extra"})
	}
	body := map[string]any{"input": []any{messageItem("developer", "private-instructions"), map[string]any{"role": "user", "content": parts}}}
	err := upstreamRequestError(400, []byte(`{"message":"unsupported image format"}`), body, credential{})
	for _, want := range []string{"input_images=17", "input[1].content[1]:file_id", "input[1].content[2]:image_url", "input[1].content[3]:data_url", "input[1].content[4]:missing", "...(1 more)"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %q in diagnostic: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "content[17]") {
		t.Fatal("image diagnostic exposed input content or exceeded the location limit")
	}
}

// 整条错误消息都要安全：上游在 message / detail 里回显 URL、file_id、data URL 或凭据时同样脱敏，
// 覆盖 JSON 解码后的转义形式与非 JSON 原文在 500 字符处截断的边界。
func TestUpstreamImageErrorRedactsEchoedReferences(t *testing.T) {
	dataURL := "data:image/png;base64,c2VjcmV0LWltYWdlLWJ5dGVz"
	remote := "https://private.example/image.png?secret=private-token"
	body := map[string]any{"input": []any{map[string]any{"role": "user", "content": []any{
		map[string]any{"type": "input_image", "file_id": "file-privateabc123"},
		map[string]any{"type": "input_image", "image_url": remote},
		map[string]any{"type": "input_image", "image_url": dataURL},
	}}}}
	cred := credential{AccessToken: "tok-private-access", AccountID: "acct-private", Email: "private@example.invalid"}
	forbidden := []string{"file-privateabc123", "private.example", "private-token", "c2VjcmV0LWltYWdlLWJ5dGVz", "tok-private-access", "acct-private", "private@example.invalid", "file-otherecho99"}
	padding := strings.Repeat("x", 480)
	cases := map[string]string{
		"message":         `{"message":"rejected file-privateabc123 and ` + remote + ` and ` + dataURL + ` for tok-private-access"}`,
		"nested_error":    `{"error":{"message":"bad reference file-otherecho99 at ` + remote + `"}}`,
		"detail_string":   `{"detail":"cannot read ` + dataURL + `"}`,
		"detail_list":     `{"detail":[{"loc":["body","input",0],"msg":"unknown file file-privateabc123 for acct-private","type":"value_error"}]}`,
		"json_escaped":    `{"message":"url https:\/\/private.example\/image.png?secret=private-token"}`,
		"plain_truncated": padding + "file-privateabc123 " + remote + " " + dataURL + " private@example.invalid",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			err := upstreamRequestError(400, []byte(raw), body, cred)
			for _, f := range forbidden {
				if strings.Contains(err.Error(), f) {
					t.Fatalf("error leaked %q: %v", f, err)
				}
			}
			if !isKind(err, "upstream_error") || !strings.Contains(err.Error(), "image_refs=input[0].content[0]:file_id") {
				t.Fatalf("classification or image_refs lost: %v", err)
			}
		})
	}
}

// 没有图片的请求保持原有错误文本（脱敏只作用于图片错误路径）。
func TestUpstreamErrorWithoutImagesKeepsMessage(t *testing.T) {
	body := map[string]any{"input": []any{messageItem("user", "hi")}}
	err := upstreamRequestError(400, []byte(`{"message":"see https://docs.example/limits"}`), body, credential{})
	if !strings.Contains(err.Error(), "https://docs.example/limits") || strings.Contains(err.Error(), "image_refs") {
		t.Fatalf("non-image error changed: %v", err)
	}
}

// Codex 审核补充的脱敏反例：Unicode 转义的凭据、URL 后紧跟转义引号（不能把解析退回原文输出）、
// 非 JSON 原文、短 file_id、互相重叠的引用、大小写变化的 URL。
func TestImageErrorSummaryCounterexamples(t *testing.T) {
	cred := credential{AccessToken: "tok-private-access", AccountID: "acct-private", Email: "private@example.invalid"}
	body := map[string]any{"input": []any{map[string]any{"role": "user", "content": []any{
		map[string]any{"type": "input_image", "file_id": "abc"},
		map[string]any{"type": "input_image", "file_id": "abcd"},
		map[string]any{"type": "input_image", "file_id": "abcdefgh"},
		map[string]any{"type": "input_image", "image_url": "https://Private.Example/a.png"},
	}}}}
	cases := []struct {
		name, raw string
		forbidden []string
	}{
		{"unicode_escaped_credentials", `{"message":"token tok\u002dprivate\u002daccess acct\u002dprivate private\u0040example.invalid"}`, []string{"tok-private-access", "private-access", "acct-private", "private@example.invalid"}},
		{"url_before_escaped_quote", `{"message":"see https://docs.example/x\" format error","input":[{"text":"private-prompt"}]}`, []string{"private-prompt", "docs.example"}},
		{"non_json", "private-prompt https://private.example/a.png file-privateid0001 tok-private-access", []string{"private", "tok-"}},
		{"short_and_overlapping_ids", `{"message":"unknown abc, abcd, abcdefgh"}`, []string{"abc", "efgh"}},
		{"case_changed_url", `{"message":"cannot fetch HTTPS://PRIVATE.EXAMPLE/A.PNG or https://Private.Example/a.png"}`, []string{"PRIVATE.EXAMPLE", "Private.Example"}},
		{"long_message_truncated_after_redaction", `{"message":"` + strings.Repeat("y", 295) + ` https://private.example/aaaaaaaaaaaaaaaaaaaa"}`, []string{"private.example", "https://pri"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := upstreamRequestError(400, []byte(tc.raw), body, cred)
			for _, f := range tc.forbidden {
				if strings.Contains(err.Error(), f) {
					t.Fatalf("error leaked %q: %v", f, err)
				}
			}
			if !isKind(err, "upstream_error") || !strings.Contains(err.Error(), "image_refs=") {
				t.Fatalf("classification or image_refs lost: %v", err)
			}
		})
	}
	if err := upstreamRequestError(400, []byte("plain failure text"), body, cred); !strings.Contains(err.Error(), "non-JSON error body (18 bytes)") {
		t.Fatalf("non-JSON image error must use the fixed summary: %v", err)
	}
}

// 冲突按「两个非空字符串值」判断：空值或错误类型视为未提供，不触发冲突（与 README 一致）。
func TestImageConflictUsesNonEmptyValues(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	for name, tc := range map[string]struct {
		part     map[string]any
		uploads  int
		wantPart map[string]any
	}{
		"empty_file_id_with_data_url": {map[string]any{"type": "input_image", "file_id": "", "image_url": dataURL}, 1, map[string]any{"type": "input_image", "file_id": "file-uploaded"}},
		"file_id_with_empty_url":      {map[string]any{"type": "input_image", "file_id": "file-existing", "image_url": ""}, 0, map[string]any{"type": "input_image", "file_id": "file-existing"}},
		"both_empty":                  {map[string]any{"type": "input_image", "file_id": "", "image_url": ""}, 0, map[string]any{"type": "input_image", "file_id": "", "image_url": ""}},
		"non_string_file_id":          {map[string]any{"type": "input_image", "file_id": 42, "image_url": dataURL}, 1, map[string]any{"type": "input_image", "file_id": "file-uploaded"}},
	} {
		t.Run(name, func(t *testing.T) {
			service := NewService()
			uploads := 0
			service.SetHost(opTolerant(func(method string, payload any, out any) error {
				uploads++
				*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Body: jsonBytes(map[string]any{"openai_file_id": "file-uploaded"})}
				return nil
			}))
			body, _, err := service.prepareRequest(imageRequest(tc.part))
			if err != nil {
				t.Fatal(err)
			}
			if got := objectValue(lastUserContent(body)[0]); uploads != tc.uploads || !reflect.DeepEqual(got, tc.wantPart) {
				t.Fatalf("uploads=%d part=%#v", uploads, got)
			}
		})
	}
}

// 合法 JSON 但没有可用错误字段：只给固定摘要，不能退回原文（原文里可能有提示词或转义凭据）。
func TestImageErrorSummaryNeverFallsBackToRawJSON(t *testing.T) {
	cred := credential{AccessToken: "tok-private-access"}
	body := map[string]any{"input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_image", "file_id": "file-x1"}}}}}
	for name, raw := range map[string]string{
		"empty_object":          `{}`,
		"only_input":            `{"input":"PRIVATE_PROMPT"}`,
		"empty_message":         `{"message":"   ","input":"PRIVATE_PROMPT"}`,
		"non_string_message":    `{"message":{"text":"PRIVATE_PROMPT"},"error":42}`,
		"unknown_field_escaped": `{"note":"tok-private-access PRIVATE_PROMPT"}`,
		"empty_detail_list":     `{"detail":[],"input":"PRIVATE_PROMPT"}`,
		"detail_nested_msg":     `{"detail":[{"msg":{"input":"PRIVATE_PROMPT"}}]}`,
		"detail_empty_entry":    `{"detail":[{}],"input":"PRIVATE_PROMPT"}`,
		"detail_nested_loc":     `{"detail":[{"loc":[{"input":"PRIVATE_PROMPT"}],"msg":"bad"}]}`,
		"detail_object_type":    `{"detail":[{"msg":"bad","type":{"input":"PRIVATE_PROMPT"}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			err := upstreamRequestError(400, []byte(raw), body, cred)
			if !strings.Contains(err.Error(), "error body without a message") {
				t.Fatalf("want fixed summary, got %v", err)
			}
			if strings.Contains(err.Error(), "PRIVATE") || strings.Contains(err.Error(), "private-access") {
				t.Fatalf("raw body leaked: %v", err)
			}
		})
	}
}

// 类型正确的 detail 列表照常输出（只保留 loc / msg / type），并经过同样的脱敏。
func TestImageErrorSummaryKeepsValidDetail(t *testing.T) {
	body := map[string]any{"input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_image", "file_id": "file-x1"}}}}}
	raw := `{"detail":[{"loc":["body","input",0],"msg":"unsupported image file-x1","type":"value_error","input":"PRIVATE_PROMPT"}]}`
	err := upstreamRequestError(422, []byte(raw), body, credential{})
	for _, want := range []string{`"msg":"unsupported image [REDACTED]"`, `"type":"value_error"`, `"loc":["body","input",0]`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("missing %s in %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "PRIVATE") || strings.Contains(err.Error(), "file-x1") {
		t.Fatalf("leaked: %v", err)
	}
}
