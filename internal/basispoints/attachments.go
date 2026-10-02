package basispoints

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
)

// 只缓存摘要和文件 ID，不保存图片或凭据；容量不限制单次请求的图片数量。
const maxAttachmentCacheEntries = 512

type cachedAttachment struct {
	key    [sha256.Size]byte
	fileID string
}

type pendingAttachment struct {
	done   chan struct{}
	fileID string
	err    error
}

type attachmentCache struct {
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*list.Element
	order   list.List
	pending map[[sha256.Size]byte]*pendingAttachment
}

func (c *attachmentCache) getOrUpload(key [sha256.Size]byte, upload func() (string, error)) (string, error) {
	c.mu.Lock()
	if entry := c.entries[key]; entry != nil {
		c.order.MoveToFront(entry)
		fileID := entry.Value.(cachedAttachment).fileID
		c.mu.Unlock()
		return fileID, nil
	}
	if pending := c.pending[key]; pending != nil {
		c.mu.Unlock()
		<-pending.done
		return pending.fileID, pending.err
	}
	if c.pending == nil {
		c.pending = make(map[[sha256.Size]byte]*pendingAttachment)
	}
	pending := &pendingAttachment{done: make(chan struct{})}
	c.pending[key] = pending
	c.mu.Unlock()

	fileID, err := upload()
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, key)
	if err == nil {
		if c.entries == nil {
			c.entries = make(map[[sha256.Size]byte]*list.Element)
		}
		c.entries[key] = c.order.PushFront(cachedAttachment{key: key, fileID: fileID})
		if c.order.Len() > maxAttachmentCacheEntries {
			oldest := c.order.Back()
			delete(c.entries, oldest.Value.(cachedAttachment).key)
			c.order.Remove(oldest)
		}
	}
	pending.fileID, pending.err = fileID, err
	close(pending.done)
	return fileID, err
}

type inlineImage struct {
	mediaType string
	filename  string
	data      []byte
}

func decodeInlineImage(dataURL string) (inlineImage, error) {
	metadata, encoded, found := strings.Cut(dataURL[5:], ",")
	if !found {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL is missing its data separator")
	}
	isBase64 := strings.HasSuffix(strings.ToLower(metadata), ";base64")
	if isBase64 {
		metadata = metadata[:len(metadata)-len(";base64")]
	}
	mediaType, _, err := mime.ParseMediaType(metadata)
	if err != nil || !strings.HasPrefix(mediaType, "image/") {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL must declare an image media type")
	}
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL has invalid percent encoding")
	}
	var data []byte
	if isBase64 {
		data, err = base64.StdEncoding.DecodeString(decoded)
	} else {
		data = []byte(decoded)
	}
	if err != nil || len(data) == 0 {
		return inlineImage{}, fail(400, "invalid_image", "input_image data URL contains empty or invalid image data")
	}
	// 以字节签名确定格式（移植自上游 08e349c，#15）：声明的 image/* 只作门禁，实际 MIME 与
	// 后缀以字节为准，避免 MIME 别名或系统扩展名数据库产生无后缀、.jfif 等文件名。
	switch detected := http.DetectContentType(data); detected {
	case "image/png":
		return inlineImage{mediaType: detected, filename: "image.png", data: data}, nil
	case "image/jpeg":
		return inlineImage{mediaType: detected, filename: "image.jpeg", data: data}, nil
	case "image/gif":
		return inlineImage{mediaType: detected, filename: "image.gif", data: data}, nil
	case "image/webp":
		return inlineImage{mediaType: detected, filename: "image.webp", data: data}, nil
	}
	return inlineImage{}, fail(400, "invalid_image", "input_image bytes must identify a supported format: PNG, JPEG, GIF, or WebP")
}

func attachmentURL(responsesURL string) (string, error) {
	base, err := url.Parse(strings.TrimRight(responsesURL, "/"))
	if err != nil || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", fail(500, "invalid_config", "cannot derive attachments endpoint from responses_url")
	}
	// 官方附件接口与 responses 同目录、同源，不能把自定义上游的凭据发往其他站点。
	return base.ResolveReference(&url.URL{Path: "attachments"}).String(), nil
}

// isUserImageMessage 判断输入条目是否为用户消息（只有用户消息里的图片走附件上传与引用规范化）。
func isUserImageMessage(item map[string]any) bool {
	itemType := stringValue(item["type"])
	return stringValue(item["role"]) == "user" && (itemType == "" || itemType == "message")
}

// attachmentKey 是附件缓存身份：与响应端点同源，并绑定账户、认证模式、凭据与实际格式；
// 相同字节以不同 image/* 声明上传时命中同一条目。
func attachmentKey(endpoint string, c credential, mediaType string, data []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write(jsonBytes([]string{endpoint, c.AccountID, c.AuthMode, c.AccessToken, mediaType}))
	_, _ = hash.Write(data)
	var key [sha256.Size]byte
	copy(key[:], hash.Sum(nil))
	return key
}

// plannedImage 是预检阶段确定的一次图片处理计划：fileID 非空表示沿用已有引用，否则按
// imageURL 上传；key 是缓存身份。
type plannedImage struct {
	itemIndex int
	partIndex int
	fileID    string
	imageURL  string
	key       [sha256.Size]byte
}

// planInputImages 在零网络前提下预检整个请求的用户消息图片：引用冲突、data URL 形状与
// 实际字节格式（声明门禁、基址64、格式签名）。全部通过后调用方才允许上传；任一后部图片
// 失败都让本次请求零上传、零生成（引用形状按 v0.1.17.1：非空字符串才参与冲突判断）。
func planInputImages(items []any, responsesURL string, c credential) ([]plannedImage, string, error) {
	var plan []plannedImage
	endpoint := ""
	for i, value := range items {
		item := objectValue(value)
		if !isUserImageMessage(item) {
			continue
		}
		parts, _ := item["content"].([]any)
		for j, value := range parts {
			part := objectValue(value)
			if stringValue(part["type"]) != "input_image" {
				continue
			}
			imageURL := stringValue(part["image_url"])
			fileID := stringValue(part["file_id"])
			if fileID != "" && imageURL != "" {
				return nil, "", fail(400, "invalid_image", fmt.Sprintf("input[%d].content[%d]: input_image cannot contain both image_url and file_id", i, j))
			}
			if fileID != "" {
				plan = append(plan, plannedImage{itemIndex: i, partIndex: j, fileID: fileID})
				continue
			}
			// 非 data URL（远程 URL、空字段）不新增下载行为，原样保留。
			if len(imageURL) < 5 || !strings.EqualFold(imageURL[:5], "data:") {
				continue
			}
			image, err := decodeInlineImage(imageURL)
			if err != nil {
				var apiErr *APIError
				if errors.As(err, &apiErr) {
					return nil, "", fail(apiErr.Status, apiErr.Kind, fmt.Sprintf("input[%d].content[%d]: %s", i, j, apiErr.Message))
				}
				return nil, "", err
			}
			if endpoint == "" {
				if endpoint, err = attachmentURL(responsesURL); err != nil {
					return nil, "", err
				}
			}
			plan = append(plan, plannedImage{itemIndex: i, partIndex: j, imageURL: imageURL, key: attachmentKey(endpoint, c, image.mediaType, image.data)})
		}
	}
	return plan, endpoint, nil
}

// uploadInputImages 规范化用户消息里的图片（移植自上游 08e349c，#17）：
//   - 先对整个请求做无网络预检（planInputImages）：引用冲突、data URL 与字节格式全部
//     通过后才开始上传；任何一张后部图片非法都不会上传前面的图片或发出生成请求；
//   - data URL 上传后、已有 file_id 直接，都只发 {type, file_id}：Basis Points 的文件引用
//     不接受 detail 等字段（真实上游带 detail 返回 422）；
//   - 远程 URL 原样保留；工具结果里的图片不在这里处理；
//   - 上传中途的网络失败如实返回，不假装回滚已上传的图片（缓存条目仍可复用）。
func (s *Service) uploadInputImages(request ExecutorRequest, body map[string]any, c credential, cfg Config) error {
	items, _ := body["input"].([]any)
	plan, endpoint, err := planInputImages(items, cfg.ResponsesURL, c)
	if err != nil {
		// 插件已停止（取消已存在）时取消优先于校验错误，与守卫的中止优先级一致，
		// 避免插件重载被误报成客户端输入错误。
		if stoppedByShutdown(request.lifeCtx) {
			return stoppedError()
		}
		return err
	}
	if len(plan) == 0 {
		return nil
	}
	touched := map[int][]any{}
	for _, entry := range plan {
		fileID := entry.fileID
		if fileID == "" {
			fileID, err = s.attachments.getOrUpload(entry.key, func() (string, error) {
				image, err := decodeInlineImage(entry.imageURL)
				if err != nil {
					return "", err
				}
				return s.uploadImage(request, endpoint, image, c)
			})
			if err != nil {
				return err
			}
		}
		parts, planned := touched[entry.itemIndex]
		if !planned {
			parts = append([]any(nil), objectValue(items[entry.itemIndex])["content"].([]any)...)
			touched[entry.itemIndex] = parts
		}
		parts[entry.partIndex] = map[string]any{"type": "input_image", "file_id": fileID}
	}
	for index, parts := range touched {
		copy := cloneObject(objectValue(items[index]))
		copy["content"] = parts
		items[index] = copy
	}
	return nil
}

func (s *Service) uploadImage(request ExecutorRequest, endpoint string, image inlineImage, c credential) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	partHeaders := make(textproto.MIMEHeader)
	partHeaders.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": image.filename}))
	partHeaders.Set("Content-Type", image.mediaType)
	part, err := writer.CreatePart(partHeaders)
	if err != nil {
		return "", fail(500, "attachment_encoding", "cannot encode image attachment")
	}
	if _, err := part.Write(image.data); err != nil {
		return "", fail(500, "attachment_encoding", "cannot write image attachment")
	}
	if err := writer.Close(); err != nil {
		return "", fail(500, "attachment_encoding", "cannot finish image attachment")
	}
	headers := authHeaders(c, s.config(), false)
	headers.Set("Content-Type", writer.FormDataContentType())
	var response upstreamResponse
	if err := s.guardedDo(request, c.AccessToken, map[string]any{
		"host_callback_id": request.HostCallbackID,
		"method":           http.MethodPost,
		"url":              endpoint,
		"headers":          headers,
		"body":             body.Bytes(),
	}, &response); err != nil {
		if isKind(err, "plugin_stopped") || isKind(err, "upstream_timeout") {
			return "", err
		}
		return "", fail(502, "attachment_transport", "Basis Points attachment upload transport failed: "+attachmentErrorMessage([]byte(err.Error()), c, image))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fail(response.StatusCode, "attachment_upload_error", fmt.Sprintf("Basis Points attachment upload HTTP %d: %s", response.StatusCode, attachmentErrorMessage(response.Body, c, image)))
	}
	var result struct {
		FileID string `json:"openai_file_id"`
	}
	if json.Unmarshal(response.Body, &result) != nil || strings.TrimSpace(result.FileID) == "" {
		return "", fail(502, "invalid_attachment_response", "Basis Points attachment upload returned no openai_file_id")
	}
	return strings.TrimSpace(result.FileID), nil
}

func attachmentErrorMessage(raw []byte, c credential, image inlineImage) string {
	message := string(raw)
	for _, secret := range []string{c.AccessToken, c.AccountID, c.Email, base64.StdEncoding.EncodeToString(image.data), string(image.data)} {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[REDACTED]")
		}
	}
	return redactTokenMessage(errorMessage([]byte(message)))
}
