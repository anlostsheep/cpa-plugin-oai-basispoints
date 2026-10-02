package basispoints

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

func (s *Service) config() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.clone()
}

func (s *Service) prepareRequest(request ExecutorRequest) (map[string]any, credential, error) {
	// 独立压缩端点未实现：明确拒绝，避免普通生成冒充压缩结果（移植自原仓库 #9）。
	if request.Alt == "responses/compact" {
		return nil, credential{}, fail(400, "unsupported_compaction", "oai-basispoints does not support /responses/compact; send full input history to /responses")
	}
	c, err := credentialFromExecutor(request)
	if err != nil {
		return nil, credential{}, err
	}
	if !c.ExpiresAt.IsZero() && !time.Now().Before(c.ExpiresAt) {
		return nil, credential{}, fail(401, "auth_expired", "ChatGPT OAuth access token has expired")
	}
	body := request.OriginalRequest
	if len(body) == 0 {
		body = request.Payload
	}
	source, err := rawObject(body)
	if err != nil {
		return nil, credential{}, err
	}
	cfg := s.config()
	model := stringValue(source["model"])
	if model == "" {
		model = strings.TrimSpace(request.Model)
	}
	source["model"] = model
	source["stream"] = request.Stream
	prepared, err := prepareResponsesBody(source, cfg)
	if err != nil {
		return nil, credential{}, err
	}
	// 先按原始图片计算会话标识，再替换附件引用，避免上传 ID 改变 task/turn。
	if err := s.uploadInputImages(request, prepared, c, cfg); err != nil {
		return nil, credential{}, err
	}
	return prepared, c, nil
}

// basispointsClientInfo 构造网页版（OfficeOnline）的 x-openai-internal-basispoints-*
// 客户端标识。键名采用 HAR 中的首字母大写形式，HTTP 头与 WS 的 bps_client_info
// 查询参数共用同一份内容，保证两条传输链路一致。
func basispointsClientInfo(cfg Config) map[string]string {
	uaPlatform := strings.TrimSpace(cfg.UAPlatform)
	if uaPlatform == "" {
		uaPlatform = DefaultUAPlatform
	}
	uaBrands := strings.TrimSpace(cfg.UABrands)
	if uaBrands == "" {
		uaBrands = DefaultUABrands
	}
	return map[string]string{
		"X-Openai-Internal-Basispoints-Client-Product":        "basispoints-excel-plugin",
		"X-Openai-Internal-Basispoints-Client-Platform":       "excel",
		"X-Openai-Internal-Basispoints-Client-Agent-Profile":  "excel",
		"X-Openai-Internal-Basispoints-Client-Editor":         "excel",
		"X-Openai-Internal-Basispoints-Client-Host":           "office",
		"X-Openai-Internal-Basispoints-Client-Runtime":        "web",
		"X-Openai-Internal-Basispoints-Client-Platform-Class": "OfficeOnline",
		"X-Openai-Internal-Basispoints-Office-Host":           "Excel",
		"X-Openai-Internal-Basispoints-Office-Platform":       "OfficeOnline",
		"X-Openai-Internal-Basispoints-Browser-Name":          "chrome",
		"X-Openai-Internal-Basispoints-Browser-UA-Platform":   uaPlatform,
		"X-Openai-Internal-Basispoints-Browser-UA-Mobile":     "false",
		"X-Openai-Internal-Basispoints-Browser-UA-Brands":     uaBrands,
	}
}

func authHeaders(c credential, cfg Config, stream bool) http.Header {
	accept := "application/json"
	if stream {
		accept = "text/event-stream"
	}
	userAgent := strings.TrimSpace(cfg.UserAgent)
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	// 网页版客户端画像；access token 本身绝不写入日志。不发 referer。
	headers := http.Header{
		"Authorization":               []string{"Bearer " + c.AccessToken},
		"ChatGPT-Account-ID":          []string{c.AccountID},
		"X-OpenAI-Account-ID":         []string{c.AccountID},
		"X-Basispoints-Auth-Mode":     []string{c.AuthMode},
		"Content-Type":                []string{"application/json"},
		"Accept":                      []string{accept},
		"Accept-Encoding":             []string{"identity"},
		"Origin":                      []string{"https://bps.openai.com"},
		"X-Stainless-Arch":            []string{"unknown"},
		"X-Stainless-Lang":            []string{"js"},
		"X-Stainless-OS":              []string{"Unknown"},
		"X-Stainless-Package-Version": []string{"6.31.0"},
		"X-Stainless-Retry-Count":     []string{"0"},
		"X-Stainless-Runtime":         []string{"browser:chrome"},
		"User-Agent":                  []string{userAgent},
	}
	for key, value := range basispointsClientInfo(cfg) {
		headers[key] = []string{value}
	}
	// 取不到复合用户标识时跳过该头，而不是发送空值。
	if c.AccountUserID != "" {
		headers["X-Openai-Account-User-Id"] = []string{c.AccountUserID}
	}
	return headers
}

func (s *Service) upstreamRequest(request ExecutorRequest, body map[string]any, c credential, stream bool) (upstreamResponse, error) {
	cfg := s.config()
	if cfg.ResponsesURL == "" {
		return upstreamResponse{}, fail(500, "invalid_config", "responses_url is empty")
	}
	payload := map[string]any{
		"host_callback_id": request.HostCallbackID,
		"method":           http.MethodPost,
		"url":              cfg.ResponsesURL,
		"headers":          authHeaders(c, cfg, stream),
		"body":             jsonBytes(body),
	}
	var response upstreamResponse
	if err := s.guardedDo(request, c.AccessToken, payload, &response); err != nil {
		if isKind(err, "plugin_stopped") || isKind(err, "upstream_timeout") {
			return upstreamResponse{}, err
		}
		return upstreamResponse{}, fail(502, "upstream_transport", "Basis Points transport failed: "+redactSecret(err.Error(), c.AccessToken))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, upstreamRequestError(response.StatusCode, response.Body, body, c)
	}
	return response, nil
}

// upstreamStream 发起流式上游请求。g 提供 operation_id（使头部等待可被取消）与中止原因；
// 非 2xx 时在同一守卫下读取错误正文。
func (s *Service) upstreamStream(request ExecutorRequest, body map[string]any, c credential, g *upstreamGuard) (upstreamStream, error) {
	cfg := s.config()
	payload := map[string]any{
		"host_callback_id": request.HostCallbackID,
		"method":           http.MethodPost,
		"url":              cfg.ResponsesURL,
		"headers":          authHeaders(c, cfg, true),
		"body":             jsonBytes(body),
	}
	if g.operationID != "" {
		payload["operation_id"] = g.operationID
	}
	var stream upstreamStream
	err := s.call("host.http.do_stream", payload, &stream)
	g.attach(stream.StreamID)
	g.markHeaders()
	if reason := g.aborted(); reason != nil {
		return stream, reason
	}
	if err != nil {
		return stream, fail(502, "upstream_transport", "Basis Points stream transport failed: "+g.redact(err))
	}
	if stream.StreamID == "" {
		return stream, fail(502, "upstream_transport", "host returned no Basis Points stream ID")
	}
	if stream.StatusCode < 200 || stream.StatusCode >= 300 {
		// 状态码已知：错误正文只做有界读取（仅用于诊断），读不完也照样返回带状态码的错误，
		// 不能让慢正文把本可同步返回的 401/429 拖过心跳窗口。
		raw, readErr := s.readErrorBody(stream, g)
		// 守卫的中止原因（插件停止 / 客户端断开 / 总超时）优先于状态码：否则部分正文会
		// 掩盖 plugin_stopped，使窗口后的流以带 error 关闭、CPA 因插件停止而冷却凭据。
		// 只有错误正文自身的读取上限造成的截断才可忽略（该路径不设置守卫中止原因）。
		// aborted() 同步检查本代停止 / 断开 / 截止时间，不依赖看守协程的调度时序。
		if reason := g.aborted(); reason != nil {
			return stream, reason
		}
		if readErr != nil && len(raw) == 0 {
			// 正文读取失败（非超时截断）：保留诊断，但仍带上游状态码。
			return stream, fail(stream.StatusCode, "upstream_error", "Basis Points error body could not be read: "+safeError(readErr))
		}
		return stream, upstreamRequestError(stream.StatusCode, raw, body, c)
	}
	return stream, nil
}

// errorBodyReadLimit 是非 2xx 错误正文的读取上限。
var errorBodyReadLimit = 2 * time.Second

// readErrorBody 在 errorBodyReadLimit 内读取错误正文；超时即关闭上游流，返回已读到的部分
// （可能为空，此时 err 为 nil）。读取本身出错时返回该错误。之后立即关闭流，不等守卫 release。
func (s *Service) readErrorBody(stream upstreamStream, g *upstreamGuard) ([]byte, error) {
	result := make(chan error, 1)
	var mu sync.Mutex
	var partial []byte
	go func() {
		_, err := s.readGuardedInto(stream, g, func(chunk []byte) error {
			mu.Lock()
			partial = append(partial, chunk...)
			mu.Unlock()
			return nil
		})
		result <- err
	}()
	timer := time.NewTimer(errorBodyReadLimit)
	defer timer.Stop()
	var readErr error
	select {
	case readErr = <-result:
		g.closeStream(stream.StreamID)
	case <-timer.C:
		g.closeStream(stream.StreamID) // 解除阻塞中的读取；截断不算读取错误
		<-result
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]byte(nil), partial...), readErr
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	return redactTokenMessage(err.Error())
}

func (s *Service) readUpstreamStream(stream upstreamStream) ([]byte, error) {
	return s.readUpstreamStreamUntil(stream, nil, nil)
}

// readUpstreamStreamUntil 在独立守卫下读取一条已打开的上游流（无 operation 阶段）。
func (s *Service) readUpstreamStreamUntil(stream upstreamStream, stop, shutdown <-chan struct{}) ([]byte, error) {
	cfg := s.config()
	g := s.newUpstreamGuard("", "", stop, shutdown, time.Duration(cfg.TimeoutSeconds)*time.Second, timeoutError(cfg))
	defer g.release()
	g.attach(stream.StreamID)
	return s.readGuarded(stream, g)
}

// readGuarded 读取完整上游流。守卫的看守协程会在断开/停止/超时时关闭流，解除**正在阻塞**
// 的 stream_read；每次读取返回后先检查中止原因，绝不把被截断的缓冲当作完整响应。
// 关闭上游流由守卫的 release 负责（恰好一次）。
func (s *Service) readGuarded(stream upstreamStream, g *upstreamGuard) ([]byte, error) {
	return s.readGuardedInto(stream, g, nil)
}

// readGuardedInto 同 readGuarded，并在每个数据块到达时回调 onChunk（可为 nil）；onChunk 返回
// 错误即中止读取（例如增量交付时客户端已断开或上游事件不一致），不再发起下一次 stream_read。
func (s *Service) readGuardedInto(stream upstreamStream, g *upstreamGuard, onChunk func([]byte) error) ([]byte, error) {
	cfg := s.config()
	if stream.StreamID == "" {
		return nil, fail(502, "upstream_transport", "upstream stream ID is empty")
	}
	var buffer bytes.Buffer
	for {
		if err := g.aborted(); err != nil {
			return nil, err
		}
		var chunk streamChunk
		readErr := s.call("host.http.stream_read", map[string]any{"stream_id": stream.StreamID}, &chunk)
		if err := g.aborted(); err != nil {
			return nil, err
		}
		if readErr != nil {
			return nil, fail(502, "upstream_transport", "Basis Points stream read failed: "+g.redact(readErr))
		}
		if chunk.Error != "" {
			return nil, fail(502, "upstream_transport", "Basis Points stream interrupted: "+g.redact(errors.New(chunk.Error)))
		}
		if len(chunk.Payload) > 0 {
			if buffer.Len()+len(chunk.Payload) > cfg.MaxResponseBytes {
				return nil, fail(502, "upstream_response_too_large", "Basis Points response exceeds configured limit")
			}
			_, _ = buffer.Write(chunk.Payload)
			if onChunk != nil {
				// onChunk 可能阻塞（向下游交付正文）：期间守卫若已记录超时/停止，这一原因优先于
				// 回调随后报告的解析/一致性错误，也不能把这一块（尤其 Done 块）当作成功读完。
				// 客户端断开例外：立即静默结束。
				chunkErr := onChunk(chunk.Payload)
				if chunkErr != nil && (errors.Is(chunkErr, errClientDisconnected) || isKind(chunkErr, "client_disconnected")) {
					return nil, chunkErr
				}
				if err := g.aborted(); err != nil {
					return nil, err
				}
				if chunkErr != nil {
					return nil, chunkErr
				}
			}
		}
		if chunk.Done {
			return buffer.Bytes(), nil
		}
	}
}

type sseDecoder struct {
	buffer strings.Builder
	data   []string
	event  string
}

func newSSEDecoder() *sseDecoder { return &sseDecoder{} }

func (d *sseDecoder) feed(chunk []byte, emit func(event, data string) error) error {
	d.buffer.Write(chunk)
	text := d.buffer.String()
	for {
		index := strings.IndexByte(text, '\n')
		if index < 0 {
			d.buffer.Reset()
			d.buffer.WriteString(text)
			return nil
		}
		line := strings.TrimSuffix(text[:index], "\r")
		text = text[index+1:]
		if line == "" {
			if len(d.data) > 0 {
				if err := emit(d.event, strings.Join(d.data, "\n")); err != nil {
					return err
				}
			}
			d.data = nil
			d.event = ""
			continue
		}
		if strings.HasPrefix(line, "event:") {
			d.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			value := strings.TrimPrefix(line, "data:")
			d.data = append(d.data, strings.TrimPrefix(value, " "))
		}
	}
}

// imageRefPattern 匹配上游错误文本里可能回显的图片引用：data URL、http(s) URL、file-… 形式的文件 ID。
var imageRefPattern = regexp.MustCompile(`(?i)data:[^\s"'<>]*|https?://[^\s"'<>]+|\bfile-[A-Za-z0-9_-]{4,}`)

// maxImageErrorMessage 是图片请求错误摘要中上游文本的上限（字节，脱敏之后再截断）。
const maxImageErrorMessage = 300

// safeDetailEntry 只接受类型正确的校验错误条目：msg 为非空字符串，type 缺省或为字符串，loc 缺省
// 或为只含字符串 / 数字的数组。嵌套对象等其他形态一律丢弃，不进入摘要。
func safeDetailEntry(entry map[string]any) (map[string]any, bool) {
	if entry == nil {
		return nil, false
	}
	msg, ok := entry["msg"].(string)
	if !ok || strings.TrimSpace(msg) == "" {
		return nil, false
	}
	safe := map[string]any{"msg": msg}
	if value, present := entry["type"]; present && value != nil {
		kind, ok := value.(string)
		if !ok {
			return nil, false
		}
		safe["type"] = kind
	}
	if value, present := entry["loc"]; present && value != nil {
		list, ok := value.([]any)
		if !ok {
			return nil, false
		}
		for _, item := range list {
			switch item.(type) {
			case string, float64, json.Number:
			default:
				return nil, false
			}
		}
		safe["loc"] = list
	}
	return safe, true
}

// jsonErrorField 从已解析的上游错误对象里只取允许的错误字段（与 errorMessage 的 JSON 规则一致：
// detail 列表只保留 loc / msg / type，其次 detail 字符串、error.message、message、error 字符串）。
// 没有可用字段、字段为空或类型不符时返回 ok=false——调用方不得退回原文。
func jsonErrorField(object map[string]any) (string, bool) {
	if details, ok := object["detail"].([]any); ok && len(details) > 0 {
		safe := make([]map[string]any, 0, len(details))
		for _, value := range details {
			if entry, ok := safeDetailEntry(objectValue(value)); ok {
				safe = append(safe, entry)
			}
		}
		if len(safe) > 0 {
			return string(jsonBytes(map[string]any{"detail": safe})), true
		}
	}
	candidates := []any{object["detail"], objectValue(object["error"])["message"], object["message"], object["error"]}
	for _, candidate := range candidates {
		if text, ok := candidate.(string); ok && strings.TrimSpace(text) != "" {
			return text, true
		}
	}
	return "", false
}

// imageErrorSummary 生成图片请求错误的安全摘要：只用 jsonErrorField 从解析后的对象里取允许的
// 错误字段，不对原文做任何字符串改写，也不调用会退回原文的 errorMessage；原文不是 JSON、或没有
// 可用的错误字段时，只给固定摘要。提取出的文本在解码之后脱敏：凭据精确替换，
// 本次请求的图片引用按长度降序替换（短值不跳过，避免长值被前缀替换后泄露后缀），再用模式兜底
// 替换 data: / http(s):// / file-… 形式的回显；最后才截断。代价是图片请求错误里的普通链接也会被
// 替换。
func imageErrorSummary(raw []byte, c credential, secrets []string) string {
	var object map[string]any
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return fmt.Sprintf("non-JSON error body (%d bytes)", len(raw))
	}
	message, ok := jsonErrorField(object)
	if !ok {
		return "error body without a message"
	}
	ordered := make([]string, 0, len(secrets)+3)
	for _, secret := range append([]string{c.AccessToken, c.AccountID, c.Email}, secrets...) {
		if secret != "" {
			ordered = append(ordered, secret)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
	for _, secret := range ordered {
		message = strings.ReplaceAll(message, secret, "[REDACTED]")
	}
	message = imageRefPattern.ReplaceAllString(message, "[REDACTED_IMAGE]")
	message = redactTokenMessage(message)
	if len(message) > maxImageErrorMessage {
		message = strings.ToValidUTF8(message[:maxImageErrorMessage], "") + "…"
	}
	return message
}

// 仅附加非敏感摘要，不记录对话正文、图片内容或认证信息。图片请求的错误另附 image_refs（只有
// 位置与引用类别，移植自上游 08e349c），上游文本改用 imageErrorSummary 的安全摘要。
func upstreamRequestError(status int, raw []byte, body map[string]any, c credential) error {
	images, originalDetails := 0, 0
	var imageRefs, imageSecrets []string
	items, _ := body["input"].([]any)
	for i, value := range items {
		parts, _ := objectValue(value)["content"].([]any)
		for j, value := range parts {
			part := objectValue(value)
			if stringValue(part["type"]) != "input_image" {
				continue
			}
			images++
			if stringValue(part["detail"]) == "original" {
				originalDetails++
			}
			kind := "missing"
			if fileID := stringValue(part["file_id"]); fileID != "" {
				kind = "file_id"
				imageSecrets = append(imageSecrets, fileID)
			} else if imageURL := stringValue(part["image_url"]); imageURL != "" {
				kind = "image_url"
				imageSecrets = append(imageSecrets, imageURL)
				if len(imageURL) >= 5 && strings.EqualFold(imageURL[:5], "data:") {
					kind = "data_url"
					if _, payload, found := strings.Cut(imageURL, ","); found {
						imageSecrets = append(imageSecrets, payload)
					}
				}
			}
			// 只输出发往上游的请求中的索引和引用类别，不输出 URL、文件 ID 或图片内容。
			if len(imageRefs) < 16 {
				imageRefs = append(imageRefs, fmt.Sprintf("input[%d].content[%d]:%s", i, j, kind))
			}
		}
	}
	var message string
	if images > 0 {
		message = imageErrorSummary(raw, c, imageSecrets)
	} else {
		// 不带图片的请求保持原有处理。
		redacted := string(raw)
		for _, secret := range []string{c.AccessToken, c.AccountID, c.Email} {
			if secret != "" {
				redacted = strings.ReplaceAll(redacted, secret, "[REDACTED]")
			}
		}
		message = redactTokenMessage(errorMessage([]byte(redacted)))
	}
	tier := "unspecified"
	if value, exists := body["service_tier"]; exists {
		switch stringValue(value) {
		case "auto", "default", "flex", "priority", "scale":
			tier = stringValue(value)
		default:
			tier = "invalid"
		}
	}
	imageSummary := ""
	if images > 0 {
		imageSummary = "; image_refs=" + strings.Join(imageRefs, ",")
		if images > len(imageRefs) {
			imageSummary += fmt.Sprintf(",...(%d more)", images-len(imageRefs))
		}
	}
	return fail(status, "upstream_error", fmt.Sprintf("Basis Points HTTP %d: %s (reasoning_effort=%s; service_tier=%s; input_images=%d; original_detail_images=%d%s)", status, message, stringValue(body["reasoning_effort"]), tier, images, originalDetails, imageSummary))
}
