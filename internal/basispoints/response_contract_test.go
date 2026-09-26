package basispoints

// 移植自原仓库 JaxsonWang/cpa-plugin-oai-basispoints v0.1.10 的 response_contract_test.go。
// 适配本 fork 的流式架构（延迟心跳 + 流会话）：
//   - 流式下重生成在后台往返中进行，尝试次数须在流关闭后读取，并加锁计数（-race）；
//   - 流会话会多次 emit（开场、心跳、回放），捕获的输出按追加处理；
//   - 流式最终格式错误以流内 response.failed（code=invalid_tool_call，含诊断）正常关闭，
//     而不是同步返回错误；上游 429 仍在延迟心跳窗口内同步返回带状态码的错误。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func relayFixture(id string, broken bool) (map[string]any, string) {
	patch := "*** Begin Patch" + string(rune(10)) + `+await page.locator('[data-id="1"]').click();` + string(rune(10)) + "*** End Patch"
	code := string(jsonBytes(map[string]any{"tool": "apply_patch", "args": patch}))
	if broken {
		code = strings.ReplaceAll(code, string([]byte{92, 34}), string(rune(34)))
	}
	return map[string]any{"type": "function_call", "name": transportName, "call_id": id, "arguments": string(jsonBytes(map[string]any{"code": code}))}, patch
}

func TestRelayRegenerationHTTP(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, mode := range []string{"valid", "recover", "exhausted", "upstream_error", "nonstream_sse"} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, mode), func(t *testing.T) {
				var mu sync.Mutex
				attempts := 0
				bad, _ := relayFixture(t.Name()+"-bad", true)
				good, patch := relayFixture(t.Name()+"-good", false)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					attempts++
					attempt := attempts
					mu.Unlock()
					var sent map[string]any
					if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					if sent["stream"] != stream {
						t.Error("stream mode changed")
					}
					hasHint := strings.Contains(string(jsonBytes(sent)), transportRetryHint)
					if hasHint != (attempt == 2) {
						t.Error("retry hint missing or present on first attempt")
					}
					if mode == "upstream_error" {
						w.WriteHeader(429)
						_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
						return
					}
					native := good
					if mode == "exhausted" || (mode == "recover" && attempt == 1) {
						native = bad
					}
					response := map[string]any{"id": "resp_local_fixture", "status": "completed", "output": []any{native}, "usage": map[string]any{"total_tokens": 17}}
					w.Header().Set("X-Request-Id", "local-http-fixture")
					if stream || mode == "nonstream_sse" {
						w.Header().Set("Content-Type", "text/event-stream")
						var events strings.Builder
						writeSSE(&events, "response.completed", map[string]any{"type": "response.completed", "response": response})
						_, _ = io.WriteString(w, events.String())
					} else {
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(response)
					}
				}))
				defer server.Close()
				svc := NewService()
				svc.cfg.ResponsesURL = server.URL
				var hostMu sync.Mutex
				upstreamBodies := map[string][]byte{}
				var emitted []byte
				closes := 0
				nextStream := 0
				done := make(chan map[string]any, 1)
				svc.SetHost(func(method string, payload any, out any) error {
					p := payload.(map[string]any)
					hostMu.Lock()
					defer hostMu.Unlock()
					switch method {
					case "host.http.do", "host.http.do_stream":
						req, err := http.NewRequest(http.MethodPost, p["url"].(string), bytes.NewReader(p["body"].([]byte)))
						if err != nil {
							return err
						}
						req.Header = p["headers"].(http.Header)
						res, err := server.Client().Do(req)
						if err != nil {
							return err
						}
						defer res.Body.Close()
						raw, err := io.ReadAll(res.Body)
						if err != nil {
							return err
						}
						if method == "host.http.do" {
							*out.(*upstreamResponse) = upstreamResponse{StatusCode: res.StatusCode, Headers: res.Header, Body: raw}
						} else {
							nextStream++
							id := fmt.Sprintf("fixture-http-%d", nextStream)
							upstreamBodies[id] = raw
							*out.(*upstreamStream) = upstreamStream{StatusCode: res.StatusCode, Headers: res.Header, StreamID: id}
						}
					case "host.http.stream_read":
						*out.(*streamChunk) = streamChunk{Payload: upstreamBodies[stringValue(p["stream_id"])], Done: true}
					case "host.http.stream_close":
						closes++
					case "host.stream.emit":
						emitted = append(emitted, p["payload"].([]byte)...)
					case "host.stream.close":
						done <- p
					default:
						return fmt.Errorf("unexpected method %s", method)
					}
					return nil
				})
				src := map[string]any{"model": DefaultModelID, "input": "Apply patch", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
				req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(src), Stream: stream, StreamID: "client-fixture", StorageJSON: jsonBytes(map[string]any{"access_token": "fixture", "account_id": "fixture"})}
				method := "executor.execute"
				if stream {
					method = "executor.execute_stream"
				}
				result, err := svc.Handle(method, jsonBytes(req))
				wantAttempts := 1
				if mode == "recover" || mode == "exhausted" {
					wantAttempts = 2
				}
				// 流式成功返回头部后，往返（含重生成）在后台完成：等流关闭后再核对。
				var closed map[string]any
				if stream && err == nil {
					select {
					case closed = <-done:
					case <-time.After(10 * time.Second):
						t.Fatal("stream did not close")
					}
				}
				mu.Lock()
				gotAttempts := attempts
				mu.Unlock()
				if gotAttempts != wantAttempts {
					t.Fatalf("attempts=%d want=%d", gotAttempts, wantAttempts)
				}
				hostMu.Lock()
				gotCloses, gotEmitted := closes, append([]byte(nil), emitted...)
				hostMu.Unlock()
				if stream && gotCloses != gotAttempts {
					t.Fatalf("streams leaked: closes=%d attempts=%d", gotCloses, gotAttempts)
				}
				if mode == "upstream_error" || (mode == "exhausted" && !stream) {
					api, ok := err.(*APIError)
					wantStatus := 422
					if mode == "upstream_error" {
						wantStatus = 429
					}
					if !ok || api.Status != wantStatus {
						t.Fatalf("want status %d, got %v", wantStatus, err)
					}
					if mode == "exhausted" && (!strings.Contains(api.Message, "code invalid_json byte_offset=") || strings.Contains(api.Message, "data-id")) {
						t.Fatalf("unsafe/incomplete diagnostic: %v", api)
					}
					if result != nil || len(gotEmitted) != 0 || len(done) != 0 || rememberedNativeCall(stringValue(bad["call_id"])) != nil {
						t.Fatal("failed response leaked data/state")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if mode == "exhausted" {
					// 流式：已开流，最终格式错误以流内 response.failed 正常关闭（不冷却凭据）。
					if closed["error"] != nil {
						t.Fatalf("request-scoped failure must close normally: %v", closed)
					}
					var failed map[string]any
					for _, event := range clientStreamEvents(t, gotEmitted) {
						if event["type"] == "response.completed" {
							t.Fatal("exhausted relay reported success")
						}
						if event["type"] == "response.failed" {
							failed = objectValue(objectValue(event["response"])["error"])
						}
					}
					message := stringValue(failed["message"])
					if failed["code"] != "invalid_tool_call" || !strings.Contains(message, "code invalid_json byte_offset=") || strings.Contains(message, "data-id") {
						t.Fatalf("unsafe/incomplete in-stream diagnostic: %v", failed)
					}
					if rememberedNativeCall(stringValue(bad["call_id"])) != nil {
						t.Fatal("rejected attempt cached")
					}
					return
				}
				var response map[string]any
				if stream {
					if closed["error"] != nil {
						t.Fatal(closed)
					}
					for _, event := range clientStreamEvents(t, gotEmitted) {
						if event["type"] == "response.completed" {
							response = objectValue(event["response"])
						}
					}
				} else {
					payload := result.(map[string]any)
					response, _ = rawObject(payload["Payload"].([]byte))
					headers := payload["Headers"].(http.Header)
					if headers.Get("Content-Type") != "application/json" || headers.Get("Content-Length") != "" {
						t.Fatalf("stale entity headers: %v", headers)
					}
				}
				call := objectValue(response["output"].([]any)[0])
				if call["type"] != "custom_tool_call" || call["input"] != patch {
					t.Fatal("tool payload changed")
				}
				if fmt.Sprint(objectValue(response["usage"])["total_tokens"]) != "17" {
					t.Fatal("terminal usage lost")
				}
				if rememberedNativeCall(stringValue(bad["call_id"])) != nil {
					t.Fatal("rejected attempt cached")
				}
			})
		}
	}
}

// ws 传输同样只重生成一次：第二条连接的 response.create 带纠正提示；再失败则流内 response.failed。
func TestRelayRegenerationWS(t *testing.T) {
	for _, mode := range []string{"recover", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			bad, _ := relayFixture(t.Name()+"-bad", true)
			good, patch := relayFixture(t.Name()+"-good", false)
			var mu sync.Mutex
			var frames []map[string]any
			mock := &mockBPS{}
			mock.respond = func(ctx context.Context, conn *websocket.Conn) {
				mock.mu.Lock()
				first := mock.firstFrame
				mock.mu.Unlock()
				mu.Lock()
				frames = append(frames, first)
				attempt := len(frames)
				mu.Unlock()
				native := good
				if mode == "exhausted" || attempt == 1 {
					native = bad
				}
				response := map[string]any{"id": "resp_ws_fixture", "status": "completed", "output": []any{native}, "usage": map[string]any{"total_tokens": 9}}
				_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.completed", "response": response})
				_, _, _ = conn.Read(ctx)
			}
			server := httptest.NewServer(http.HandlerFunc(mock.handler))
			defer server.Close()
			svc := newWSService(t, server)
			var hostMu sync.Mutex
			var emitted []byte
			done := make(chan map[string]any, 1)
			svc.SetHost(func(method string, payload any, out any) error {
				p := payload.(map[string]any)
				hostMu.Lock()
				defer hostMu.Unlock()
				switch method {
				case "host.stream.emit":
					emitted = append(emitted, p["payload"].([]byte)...)
				case "host.stream.close":
					done <- p
				}
				return nil
			})
			src := map[string]any{"model": DefaultModelID, "input": "Apply patch", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
			req := ExecutorRequest{Model: DefaultModelID, Payload: jsonBytes(src), Stream: true, StreamID: "client-ws", StorageJSON: jsonBytes(map[string]any{"access_token": "fixture", "account_id": "fixture"})}
			if _, err := svc.Handle("executor.execute_stream", jsonBytes(req)); err != nil {
				t.Fatal(err)
			}
			var closed map[string]any
			select {
			case closed = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("stream did not close")
			}
			if closed["error"] != nil {
				t.Fatalf("request-scoped outcome must close normally: %v", closed)
			}
			mu.Lock()
			got := append([]map[string]any(nil), frames...)
			mu.Unlock()
			if len(got) != 2 {
				t.Fatalf("ws attempts=%d want 2", len(got))
			}
			if strings.Contains(string(jsonBytes(got[0])), transportRetryHint) || !strings.Contains(string(jsonBytes(got[1])), transportRetryHint) {
				t.Fatal("retry hint missing or present on first attempt")
			}
			hostMu.Lock()
			wire := append([]byte(nil), emitted...)
			hostMu.Unlock()
			var completed, failed map[string]any
			for _, event := range clientStreamEvents(t, wire) {
				switch event["type"] {
				case "response.completed":
					completed = objectValue(event["response"])
				case "response.failed":
					failed = objectValue(objectValue(event["response"])["error"])
				}
			}
			if mode == "recover" {
				call := objectValue(completed["output"].([]any)[0])
				if call["type"] != "custom_tool_call" || call["input"] != patch {
					t.Fatalf("tool payload changed: %v", call)
				}
				return
			}
			if completed != nil || failed["code"] != "invalid_tool_call" || !strings.Contains(stringValue(failed["message"]), "code invalid_json") {
				t.Fatalf("exhausted ws relay: completed=%v failed=%v", completed != nil, failed)
			}
		})
	}
}

// 不完整（截断）的响应不重生成：重试不会让截断的输出变得完整，只会重复计费。
func TestRelayRetrySkipsIncompleteAndOtherErrors(t *testing.T) {
	body := map[string]any{"input": []any{messageItem("user", "hi")}}
	relay := relayError("code invalid_json byte_offset=3")
	if _, ok := relayRetryBody(body, map[string]any{"status": "incomplete"}, relay, 0); ok {
		t.Fatal("incomplete response must not be regenerated")
	}
	if _, ok := relayRetryBody(body, map[string]any{"status": "completed"}, relay, 1); ok {
		t.Fatal("regenerated more than once")
	}
	if _, ok := relayRetryBody(body, map[string]any{"status": "completed"}, fail(429, "upstream_error", "rate limited"), 0); ok {
		t.Fatal("non-relay error must not be regenerated")
	}
	retry, ok := relayRetryBody(body, map[string]any{"status": "completed"}, relay, 0)
	if !ok {
		t.Fatal("relay error was not regenerated")
	}
	if len(body["input"].([]any)) != 1 {
		t.Fatal("original body mutated")
	}
	items := retry["input"].([]any)
	if hint := itemText(objectValue(items[len(items)-1])["content"]); !strings.Contains(hint, transportRetryHint) || !strings.Contains(hint, "code invalid_json byte_offset=3") {
		t.Fatalf("retry hint missing diagnostic: %q", hint)
	}
}

func TestResponseFormatsAndTerminalStates(t *testing.T) {
	good := map[string]any{"id": "resp_fixture", "status": "completed", "output": []any{messageItem("assistant", "OK")}, "usage": map[string]any{"total_tokens": 23}}
	sse := func(kind string, r map[string]any) []byte {
		var b strings.Builder
		writeSSE(&b, kind, map[string]any{"type": kind, "response": r})
		return []byte(b.String())
	}
	incomplete := cloneObject(good)
	incomplete["status"] = "incomplete"
	incomplete["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
	for _, tc := range []struct {
		name string
		raw  []byte
		want string
	}{
		{"json", jsonBytes(good), "completed"},
		{"sse", sse("response.completed", good), "completed"},
		{"sse_eof", bytes.TrimSpace(sse("response.completed", good)), "completed"},
		{"incomplete_json", jsonBytes(incomplete), "incomplete"},
		{"incomplete_sse", sse("response.incomplete", incomplete), "incomplete"},
		{"html", []byte("<html>PRIVATE_BODY</html>"), ""},
		{"empty", nil, ""},
		{"null", []byte("null"), ""},
		{"missing_status", jsonBytes(map[string]any{"output": []any{}}), ""},
		{"unknown_status", jsonBytes(map[string]any{"status": "unknown", "output": []any{}}), ""},
		{"trailing", append(jsonBytes(good), []byte("junk")...), ""},
		{"failure", sse("response.failed", good), ""},
		{"cancelled", sse("response.cancelled", good), ""},
		{"status_mismatch", sse("response.completed", incomplete), ""},
		{"multiple_terminal", append(sse("response.completed", good), sse("response.completed", good)...), ""},
		{"no_terminal", []byte("data: [DONE]"), ""},
		{"invalid_event", []byte("data: {PRIVATE_BODY"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := parseResponse(tc.raw, http.Header{"Content-Type": {"text/event-stream"}})
			if tc.want == "" {
				if err == nil {
					t.Fatal("invalid response accepted")
				}
				if strings.Contains(err.Error(), "PRIVATE_BODY") {
					t.Fatal("response data leaked")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if response["status"] != tc.want || fmt.Sprint(objectValue(response["usage"])["total_tokens"]) != "23" {
				t.Fatalf("terminal status/usage lost: %v", response)
			}
			if tc.want == "incomplete" {
				wire := string(syntheticStream(response))
				if strings.Contains(wire, "response.completed") || !strings.Contains(wire, "response.incomplete") {
					t.Fatal("incomplete synthesized as success")
				}
			}
		})
	}
}

// 上游在帧内报告凭据失效或限流时，严格解析仍保留 401/429，交给 CPA 换号或冷却。
func TestStrictParsingKeepsCredentialFailureStatus(t *testing.T) {
	for _, tc := range []struct {
		code string
		want int
	}{{"rate_limit_exceeded", 429}, {"token_expired", 401}, {"account_deactivated", 403}} {
		raw := []byte("event: response.failed\ndata: " + string(jsonBytes(map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]any{"code": tc.code}}})) + "\n\n")
		_, err := parseResponse(raw, http.Header{"Content-Type": {"text/event-stream"}})
		api, ok := err.(*APIError)
		if !ok || api.Status != tc.want {
			t.Fatalf("%s: want %d, got %v", tc.code, tc.want, err)
		}
		failedJSON := jsonBytes(map[string]any{"status": "failed", "output": []any{}, "error": map[string]any{"code": tc.code}})
		if _, err := parseResponse(failedJSON, http.Header{"Content-Type": {"application/json"}}); err == nil || err.(*APIError).Status != tc.want {
			t.Fatalf("%s JSON: want %d, got %v", tc.code, tc.want, err)
		}
	}
}

// ws：带完整响应对象的 response.incomplete 是合法终态，原样交给客户端。
func TestWSIncompleteIsTerminal(t *testing.T) {
	incomplete := completedResponseWithUsage()
	incomplete["status"] = "incomplete"
	incomplete["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
	mock := &mockBPS{}
	mock.respond = func(ctx context.Context, conn *websocket.Conn) {
		_ = writeWSFrame(ctx, conn, map[string]any{"type": "response.incomplete", "response": incomplete})
		_, _, _ = conn.Read(ctx)
	}
	server := httptest.NewServer(http.HandlerFunc(mock.handler))
	defer server.Close()
	svc := newWSService(t, server)
	got, err := svc.streamOverWS(context.Background(), ExecutorRequest{StreamID: "s-incomplete"}, wsBody(), credential{AccessToken: "tok", AccountID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if got["status"] != "incomplete" {
		t.Fatalf("incomplete terminal lost: %v", got["status"])
	}
	wire := string(syntheticStream(got))
	if strings.Contains(wire, "response.completed") || !strings.Contains(wire, "response.incomplete") {
		t.Fatal("incomplete synthesized as success")
	}
}
