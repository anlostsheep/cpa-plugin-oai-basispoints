package basispoints

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	Version        = "0.1.14"
	Provider       = "oai-basispoints"
	AuthProviderID = "codex"
	PluginID       = Provider

	DefaultResponsesURL  = "https://bps.openai.com/basispoints/api/responses"
	DefaultUpstreamModel = "gpt-6-astra"
	DefaultModelID       = "gpt-6-astra-basispoints"

	// 网页版（OfficeOnline）请求头默认值，取自真实 HAR 抓包。
	DefaultUserAgent     = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
	DefaultUAPlatform    = "macOS"
	DefaultUABrands      = "Google Chrome,Not_A Brand,Chromium"
	DefaultChromeVersion = "153.0.0.0"

	// TransportHTTP 保持 v0.1.10 的一次性缓冲 HTTP 行为；TransportWS 走 WebSocket。
	TransportHTTP = "http"
	TransportWS   = "ws"

	// DefaultHeartbeatSeconds：流式缓冲期间向客户端发送 response.in_progress 心跳的间隔，
	// 需小于 sub2api stream_data_interval_timeout(180s) 与 Codex 空闲超时(300s)。0 关闭心跳。
	DefaultHeartbeatSeconds = 15
	maxHeartbeatSeconds     = 120
)

var supportedReasoningEfforts = map[string]struct{}{
	"low": {}, "medium": {}, "high": {}, "xhigh": {}, "ultra": {},
}

// APIError carries a downstream HTTP status through the CPA plugin envelope.
type APIError struct {
	Status  int
	Kind    string
	Message string
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *APIError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.Status
}

func (e *APIError) Code() string {
	if e == nil || e.Kind == "" {
		return "plugin_error"
	}
	return e.Kind
}

func fail(status int, kind, message string) error {
	return &APIError{Status: status, Kind: kind, Message: message}
}

type HostCall func(method string, payload any, out any) error

// ExecutorRequest mirrors CPA's JSON executor contract. HTTPClient is not part
// of the JSON ABI; this plugin deliberately uses the host callback instead.
type ExecutorRequest struct {
	AuthID          string            `json:"AuthID"`
	AuthProvider    string            `json:"AuthProvider"`
	Model           string            `json:"Model"`
	Format          string            `json:"Format"`
	Stream          bool              `json:"Stream"`
	Alt             string            `json:"Alt"`
	Headers         http.Header       `json:"Headers"`
	Query           url.Values        `json:"Query"`
	OriginalRequest []byte            `json:"OriginalRequest"`
	SourceFormat    string            `json:"SourceFormat"`
	Payload         []byte            `json:"Payload"`
	Metadata        map[string]any    `json:"Metadata"`
	StorageJSON     []byte            `json:"StorageJSON"`
	AuthMetadata    map[string]any    `json:"AuthMetadata"`
	AuthAttributes  map[string]string `json:"AuthAttributes"`
	StreamID        string            `json:"stream_id,omitempty"`
	HostCallbackID  string            `json:"host_callback_id,omitempty"`

	// lifeCtx / lifeDone 由 execute 在处理任何宿主 HTTP 调用（含附件上传）之前通过
	// beginStream 绑定：插件停止时取消（cause=errPluginStopped），往返结束时调用 lifeDone。
	// 未导出，不参与 JSON。
	lifeCtx  context.Context
	lifeDone func()
	// deadline 是模型往返（首次 + 至多一次重新生成）共享的总截止时间，零值表示不限。
	// timeout_seconds 约束的是整次客户端请求，重新生成不能重置它。
	deadline time.Time
}

// roundTripTimeout 返回下一次上游往返可用的超时：设置了 deadline 时取剩余时长与配置的
// 较小值；已到期返回 ok=false，调用方应直接以超时结束，不再发起新的往返。
func (r ExecutorRequest) roundTripTimeout(cfg Config) (time.Duration, bool) {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if r.deadline.IsZero() {
		return timeout, true
	}
	remaining := time.Until(r.deadline)
	if remaining <= 0 {
		return 0, false
	}
	if remaining < timeout {
		timeout = remaining
	}
	return timeout, true
}

type ExecutorResponse struct {
	Payload  []byte         `json:"Payload"`
	Headers  http.Header    `json:"Headers"`
	Metadata map[string]any `json:"Metadata,omitempty"`
}

type StreamResponse struct {
	Headers http.Header `json:"Headers"`
}

// 非流式宿主回调直接序列化 pluginapi.HTTPResponse，字段名与流式 RPC 不同。
type upstreamResponse struct {
	StatusCode int         `json:"StatusCode"`
	Headers    http.Header `json:"Headers"`
	Body       []byte      `json:"Body"`
}

type upstreamStream struct {
	StatusCode int         `json:"status_code"`
	Headers    http.Header `json:"headers"`
	StreamID   string      `json:"stream_id"`
}

type streamChunk struct {
	Payload []byte `json:"payload"`
	Error   string `json:"error"`
	Done    bool   `json:"done"`
}

type Config struct {
	DataDir            string            `yaml:"data_dir" json:"data_dir"`
	ResponsesURL       string            `yaml:"responses_url" json:"responses_url"`
	UpstreamModel      string            `yaml:"upstream_model" json:"upstream_model"`
	Models             []string          `yaml:"models" json:"models"`
	ModelMappings      map[string]string `yaml:"model_mappings" json:"model_mappings"`
	TimeoutSeconds     int               `yaml:"timeout_seconds" json:"timeout_seconds"`
	MaxResponseBytes   int               `yaml:"max_response_bytes" json:"max_response_bytes"`
	AuthMode           string            `yaml:"auth_mode" json:"auth_mode"`
	ToolsVersionID     string            `yaml:"tools_version_id" json:"tools_version_id"`
	DedicatedAuthFiles []string          `yaml:"dedicated_auth_files" json:"dedicated_auth_files"`
	UserAgent          string            `yaml:"user_agent" json:"user_agent"`
	UAPlatform         string            `yaml:"ua_platform" json:"ua_platform"`
	UABrands           string            `yaml:"ua_brands" json:"ua_brands"`
	ChromeVersion      string            `yaml:"chrome_version" json:"chrome_version"`
	Transport          string            `yaml:"transport" json:"transport"`
	ProxyURL           string            `yaml:"proxy_url" json:"proxy_url"`
	HeartbeatSeconds   *int              `yaml:"heartbeat_seconds" json:"heartbeat_seconds"`
}

func defaultConfig() Config {
	return Config{
		DataDir:          "plugins/oai-basispoints-data",
		ResponsesURL:     DefaultResponsesURL,
		UpstreamModel:    DefaultUpstreamModel,
		Models:           []string{DefaultModelID},
		TimeoutSeconds:   300,
		MaxResponseBytes: 64 << 20,
		AuthMode:         "chatgpt",
		UserAgent:        DefaultUserAgent,
		UAPlatform:       DefaultUAPlatform,
		UABrands:         DefaultUABrands,
		ChromeVersion:    DefaultChromeVersion,
		Transport:        TransportHTTP,
		HeartbeatSeconds: intPtr(DefaultHeartbeatSeconds),
	}
}

func intPtr(value int) *int { return &value }

func (c *Config) normalize() error {
	if c == nil {
		return fail(400, "invalid_config", "configuration is missing")
	}
	c.ResponsesURL = strings.TrimSpace(c.ResponsesURL)
	if c.ResponsesURL == "" {
		c.ResponsesURL = DefaultResponsesURL
	}
	u, err := url.Parse(c.ResponsesURL)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return fail(400, "invalid_config", "responses_url must be an absolute HTTP(S) URL")
	}
	c.UpstreamModel = strings.TrimSpace(c.UpstreamModel)
	if c.UpstreamModel == "" {
		c.UpstreamModel = DefaultUpstreamModel
	}
	c.AuthMode = strings.TrimSpace(c.AuthMode)
	if c.AuthMode == "" {
		c.AuthMode = "chatgpt"
	}
	if c.TimeoutSeconds < 10 || c.TimeoutSeconds > 1800 {
		return fail(400, "invalid_config", "timeout_seconds must be between 10 and 1800")
	}
	if c.MaxResponseBytes < 64<<10 || c.MaxResponseBytes > 128<<20 {
		return fail(400, "invalid_config", "max_response_bytes must be between 64 KiB and 128 MiB")
	}
	seen := map[string]bool{}
	models := make([]string, 0, len(c.Models))
	for _, model := range c.Models {
		model = strings.TrimSpace(model)
		if model == "" || seen[model] {
			continue
		}
		seen[model] = true
		models = append(models, model)
	}
	if len(models) == 0 {
		models = []string{DefaultModelID}
		seen[DefaultModelID] = true
	}
	if c.ModelMappings != nil {
		mappings := make(map[string]string, len(c.ModelMappings))
		for alias, upstream := range c.ModelMappings {
			alias, upstream = strings.TrimSpace(alias), strings.TrimSpace(upstream)
			if alias == "" || upstream == "" {
				return fail(400, "invalid_config", "model_mappings requires non-empty aliases and upstream model names")
			}
			if !seen[alias] {
				return fail(400, "invalid_config", "model_mappings alias is not enabled in models: "+alias)
			}
			if _, exists := mappings[alias]; exists {
				return fail(400, "invalid_config", "model_mappings contains a duplicate normalized alias: "+alias)
			}
			mappings[alias] = upstream
		}
		c.ModelMappings = mappings
	}
	c.Models = models

	// 网页版请求头默认值：留空时回退到 HAR 抓包默认，避免发送空头。
	if c.UserAgent = strings.TrimSpace(c.UserAgent); c.UserAgent == "" {
		c.UserAgent = DefaultUserAgent
	}
	if c.UAPlatform = strings.TrimSpace(c.UAPlatform); c.UAPlatform == "" {
		c.UAPlatform = DefaultUAPlatform
	}
	if c.UABrands = strings.TrimSpace(c.UABrands); c.UABrands == "" {
		c.UABrands = DefaultUABrands
	}
	if c.ChromeVersion = strings.TrimSpace(c.ChromeVersion); c.ChromeVersion == "" {
		c.ChromeVersion = DefaultChromeVersion
	}

	// 传输方式：仅支持 http 与 ws，默认 http，保持 v0.1.10 行为不变。
	c.Transport = strings.ToLower(strings.TrimSpace(c.Transport))
	switch c.Transport {
	case "":
		c.Transport = TransportHTTP
	case TransportHTTP, TransportWS:
	default:
		return fail(400, "invalid_config", "transport must be either \"http\" or \"ws\"")
	}

	// ws 后备出站代理：与凭据 proxy_url 同一规则（CPA proxyutil.Parse）：留空、direct/none
	// 表示直连；否则须为带主机的 socks5/socks5h/http/https URL。
	if c.ProxyURL = strings.TrimSpace(c.ProxyURL); c.ProxyURL != "" {
		if err := validateProxyValue(c.ProxyURL); err != nil {
			return fail(400, "invalid_config", "proxy_url must be direct/none or an absolute socks5/socks5h/http/https URL")
		}
	}

	// 心跳间隔：nil 取默认；0 关闭；上限 120s，保证远低于下游空闲超时。
	if c.HeartbeatSeconds == nil {
		c.HeartbeatSeconds = intPtr(DefaultHeartbeatSeconds)
	} else if *c.HeartbeatSeconds < 0 || *c.HeartbeatSeconds > maxHeartbeatSeconds {
		return fail(400, "invalid_config", fmt.Sprintf("heartbeat_seconds must be between 0 and %d", maxHeartbeatSeconds))
	}

	// 被标记为「Excel 专用」的凭据文件名：与外部刷新脚本的标记契约同一规则——
	// 裸 auth-dir 文件名、区分大小写、不允许首尾空白/路径/重复。不合法即配置错误。
	if len(c.DedicatedAuthFiles) == 0 {
		c.DedicatedAuthFiles = nil
	} else {
		seenFiles := make(map[string]bool, len(c.DedicatedAuthFiles))
		for _, name := range c.DedicatedAuthFiles {
			if !validDedicatedEntry(name) {
				return fail(400, "invalid_config", fmt.Sprintf("dedicated_auth_files entry %q must be a bare auth-dir filename", name))
			}
			if seenFiles[name] {
				return fail(400, "invalid_config", fmt.Sprintf("dedicated_auth_files has duplicate entry %q", name))
			}
			seenFiles[name] = true
		}
	}
	return nil
}

// heartbeatInterval 返回心跳间隔；0 表示关闭。
func (c Config) heartbeatInterval() time.Duration {
	if c.HeartbeatSeconds == nil {
		return DefaultHeartbeatSeconds * time.Second
	}
	return time.Duration(*c.HeartbeatSeconds) * time.Second
}

// dedicatedSet 返回专用凭据文件名集合，供 auth.parse 隔离判定使用。
func (c Config) dedicatedSet() map[string]bool {
	if len(c.DedicatedAuthFiles) == 0 {
		return nil
	}
	set := make(map[string]bool, len(c.DedicatedAuthFiles))
	for _, name := range c.DedicatedAuthFiles {
		set[name] = true
	}
	return set
}

func (c Config) clone() Config {
	c.Models = append([]string(nil), c.Models...)
	c.ModelMappings = maps.Clone(c.ModelMappings)
	c.DedicatedAuthFiles = append([]string(nil), c.DedicatedAuthFiles...)
	if c.HeartbeatSeconds != nil {
		c.HeartbeatSeconds = intPtr(*c.HeartbeatSeconds)
	}
	return c
}

func normalizeEffort(value any) string {
	s, _ := value.(string)
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "x-high", "extra-high", "extra_high", "max":
		s = "xhigh"
	}
	if _, ok := supportedReasoningEfforts[s]; ok {
		return s
	}
	return "medium"
}

func rawObject(raw []byte) (map[string]any, error) {
	var object map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, fail(400, "invalid_request", "request body must be a JSON object")
	}
	return object, nil
}

func jsonBytes(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func stringValue(value any) string {
	s, _ := value.(string)
	return strings.TrimSpace(s)
}

func numberValue(value any) int64 {
	switch n := value.(type) {
	case json.Number:
		i, _ := n.Int64()
		return i
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	}
	return 0
}

func errorMessage(body []byte) string {
	var object map[string]any
	if json.Unmarshal(body, &object) == nil {
		// 校验错误仅保留字段路径和原因，避免把 input 中的私有内容写入日志。
		if details, ok := object["detail"].([]any); ok && len(details) > 0 {
			safe := make([]map[string]any, 0, len(details))
			for _, value := range details {
				entry := objectValue(value)
				if entry == nil {
					continue
				}
				safe = append(safe, map[string]any{"loc": entry["loc"], "msg": entry["msg"], "type": entry["type"]})
			}
			if len(safe) > 0 {
				return string(jsonBytes(map[string]any{"detail": safe}))
			}
		}
		if detail := stringValue(object["detail"]); detail != "" {
			return detail
		}

		if nested, ok := object["error"].(map[string]any); ok {
			if message := stringValue(nested["message"]); message != "" {
				return message
			}
		}
		if message := stringValue(object["message"]); message != "" {
			return message
		}
		if message := stringValue(object["error"]); message != "" {
			return message
		}
	}
	if len(body) > 0 {
		message := strings.TrimSpace(string(body))
		if len(message) > 500 {
			message = message[:500]
		}
		return message
	}
	return "Basis Points upstream request failed"
}

func timeoutError(cfg Config) error {
	return fail(504, "upstream_timeout", fmt.Sprintf("Basis Points request timed out after %d seconds", cfg.TimeoutSeconds))
}

// upstreamModelForAlias 只解析已启用的别名；未单独映射时沿用原有全局配置。
func (c Config) upstreamModelForAlias(alias string) (string, bool) {
	for _, candidate := range c.Models {
		if alias == candidate {
			if upstream, exists := c.ModelMappings[alias]; exists {
				return upstream, true
			}
			return c.UpstreamModel, true
		}
	}
	return "", false
}

// resolveUpstreamModel 同时接受客户端别名和 CPA 执行器传入的已配置上游名称。
func (c Config) resolveUpstreamModel(model string) (string, bool) {
	model = strings.TrimSpace(model)
	if model == "" && len(c.Models) > 0 {
		model = c.Models[0]
	}
	if upstream, ok := c.upstreamModelForAlias(model); ok {
		return upstream, true
	}
	for _, alias := range c.Models {
		if upstream, ok := c.upstreamModelForAlias(alias); ok && model == upstream {
			return upstream, true
		}
	}
	return "", false
}
