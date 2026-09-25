package basispoints

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"gopkg.in/yaml.v3"
)

type Service struct {
	attachments attachmentCache
	mu          sync.RWMutex
	cfg         Config
	host        HostCall
	stopped     bool
}

func NewService() *Service {
	cfg := defaultConfig()
	return &Service{cfg: cfg}
}

func (s *Service) SetHost(host HostCall) {
	s.mu.Lock()
	s.host = host
	s.mu.Unlock()
}

func (s *Service) call(method string, payload any, out any) error {
	s.mu.RLock()
	host := s.host
	s.mu.RUnlock()
	if host == nil {
		return errors.New("host callback is not initialized")
	}
	return host(method, payload, out)
}

func (s *Service) configure(raw json.RawMessage) error {
	cfg := defaultConfig()
	var request struct {
		ConfigYAML []byte `json:"config_yaml"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &request); err != nil {
			return fail(400, "invalid_config", "plugin configuration request is invalid")
		}
	}
	if len(request.ConfigYAML) > 0 {
		if err := unmarshalYAML(request.ConfigYAML, &cfg); err != nil {
			return fail(400, "invalid_config", "plugin configuration is invalid: "+err.Error())
		}
	}
	// Persist only non-secret settings. Token material always remains in CPA's
	// auth store and is supplied in ExecutorRequest.StorageJSON.
	if cfg.DataDir != "" {
		if data, err := os.ReadFile(filepath.Join(cfg.DataDir, "settings.json")); err == nil {
			_ = json.Unmarshal(data, &cfg)
		}
	}
	if err := cfg.normalize(); err != nil {
		return err
	}
	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
			return fail(500, "config_storage", "cannot create plugin data directory")
		}
		data, _ := json.MarshalIndent(cfg, "", "  ")
		_ = os.WriteFile(filepath.Join(cfg.DataDir, "settings.json"), data, 0600)
	}
	s.mu.Lock()
	s.cfg = cfg
	s.stopped = false
	s.mu.Unlock()
	return nil
}

func (s *Service) Handle(method string, raw json.RawMessage) (any, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := s.configure(raw); err != nil {
			return nil, err
		}
		return registration(s.config()), nil
	case "plugin.quiesce":
		return map[string]any{}, nil
	case "auth.identifier":
		// CPA 按这个标识把文件认证交给插件解析；Basis Points 使用
		// Codex OAuth 文件中的访问令牌。
		return map[string]any{"identifier": AuthProviderID}, nil
	case "executor.identifier":
		return map[string]any{"identifier": Provider}, nil
	case "auth.parse":
		return authParse(raw)
	case "auth.login.start":
		return nil, fail(400, "login_unavailable", "Import an existing CPA codex OAuth credential; interactive login is not used")
	case "auth.login.poll":
		return map[string]any{"Status": "error", "Message": "Import an existing CPA codex OAuth credential"}, nil
	case "auth.refresh":
		return authRefresh(raw)
	case "model.register", "model.static", "model.for_auth":
		return modelRegistration(s.config()), nil
	case "response.intercept_after":
		return s.interceptModelCatalog(raw)
	case "executor.execute":
		return s.execute(raw, false)
	case "executor.execute_stream":
		return s.execute(raw, true)
	case "executor.count_tokens":
		return map[string]any{"Payload": jsonBytes(map[string]any{"input_tokens": 0})}, nil
	case "executor.http_request":
		return nil, fail(400, "unsupported_method", "use the Basis Points model executor")
	case "plugin.shutdown":
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()
		return map[string]any{}, nil
	default:
		return nil, fail(400, "unsupported_method", "unsupported plugin method: "+method)
	}
}

func (s *Service) execute(raw json.RawMessage, stream bool) (any, error) {
	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	if stopped {
		return nil, fail(503, "plugin_stopped", "oai-basispoints is shut down")
	}
	var request ExecutorRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, fail(400, "invalid_request", "executor request is invalid")
	}
	body, credential, err := s.prepareRequest(request)
	if err != nil {
		return nil, err
	}
	if stream {
		return s.executeStream(request, body, credential)
	}
	response, err := s.upstreamRequest(request, body, credential, false)
	if err != nil {
		return nil, err
	}
	source, err := rawObject(request.OriginalRequest)
	if err != nil {
		source, err = rawObject(request.Payload)
	}
	if err != nil {
		return nil, err
	}
	transformed, _, _, err := transformResponseBody(response.Body, source)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"Payload": transformed,
		"Headers": response.Headers,
	}, nil
}

func (s *Service) executeStream(request ExecutorRequest, body map[string]any, credential credential) (any, error) {
	if request.StreamID == "" {
		return nil, fail(500, "stream_id_missing", "executor.execute_stream requires stream_id")
	}
	upstream, err := s.upstreamStream(request, body, credential)
	if err != nil {
		return nil, err
	}
	go func() {
		closeWithError := func(err error) {
			payload := map[string]any{"stream_id": request.StreamID}
			if err != nil {
				payload["error"] = safeError(err)
			}
			_ = s.call("host.stream.close", payload, nil)
		}
		raw, readErr := s.readUpstreamStream(upstream)
		if readErr != nil {
			closeWithError(readErr)
			return
		}
		source, parseErr := rawObject(request.OriginalRequest)
		if parseErr != nil {
			source, parseErr = rawObject(request.Payload)
		}
		if parseErr != nil {
			closeWithError(parseErr)
			return
		}
		response, parseErr := parseFinalStreamResponse(raw)
		if parseErr != nil {
			closeWithError(parseErr)
			return
		}
		_, transformedResponse, _, transformErr := transformResponseBody(jsonBytes(response), source)
		if transformErr != nil {
			closeWithError(transformErr)
			return
		}
		if emitErr := s.call("host.stream.emit", map[string]any{"stream_id": request.StreamID, "payload": syntheticStream(transformedResponse)}, nil); emitErr != nil {
			closeWithError(fail(499, "client_disconnected", "client disconnected while receiving stream"))
			return
		}
		closeWithError(nil)
	}()
	return map[string]any{"Headers": map[string][]string{"Content-Type": {"text/event-stream"}, "Cache-Control": {"no-cache"}}}, nil
}

func (s *Service) status() map[string]any {
	cfg := s.config()
	s.mu.RLock()
	stopped := s.stopped
	s.mu.RUnlock()
	return map[string]any{
		"provider":          Provider,
		"version":           Version,
		"responses_url":     cfg.ResponsesURL,
		"upstream_model":    cfg.UpstreamModel,
		"models":            cfg.Models,
		"model_mappings":    cfg.ModelMappings,
		"stopped":           stopped,
		"reasoning_efforts": []string{"low", "medium", "high", "xhigh", "ultra"},
	}
}

func registration(cfg Config) map[string]any {
	return map[string]any{
		"schema_version": 6,
		"metadata": map[string]any{
			"Name":             "OpenAI Basis Points",
			"Version":          Version,
			"Author":           "jaxson-wang",
			"GitHubRepository": "https://github.com/anlostsheep/cpa-plugin-oai-basispoints",
			"Description":      "CPA Responses adapter for bps.openai.com with safe client-tool relay",
			"ConfigFields": []map[string]any{
				{"Name": "responses_url", "Type": "string", "Description": "Basis Points Responses endpoint."},
				{"Name": "upstream_model", "Type": "string", "Description": "未单独配置 model_mappings 的别名使用的上游模型。"},
				{"Name": "models", "Type": "array", "Description": "启用的客户端模型别名列表，数量不限。"},
				{"Name": "model_mappings", "Type": "object", "Description": "客户端别名到实际上游模型的映射；键必须已列入 models。"},
				{"Name": "timeout_seconds", "Type": "integer", "Description": "Upstream request timeout."},
				{"Name": "max_response_bytes", "Type": "integer", "Description": "Maximum upstream response size."},
				{"Name": "auth_mode", "Type": "string", "Description": "Basis Points authentication mode; normally chatgpt."},
				{"Name": "tools_version_id", "Type": "string", "Description": "Optional authoritative Basis Points tools catalog version."},
			},
		},
		"capabilities": map[string]any{
			"auth_provider":           true,
			"model_provider":          true,
			"executor":                true,
			"executor_model_scope":    "both",
			"executor_input_formats":  []string{"openai-response"},
			"executor_output_formats": []string{"openai-response"},
			"response_interceptor":    true,
			"management_api":          false,
		},
		"config": cfg,
	}
}

func modelRegistration(cfg Config) map[string]any {
	models := make([]map[string]any, 0, len(cfg.Models))
	for _, model := range cfg.Models {
		upstream, _ := cfg.upstreamModelForAlias(model)
		models = append(models, map[string]any{
			"ID":                         model,
			"Object":                     "model",
			"Name":                       upstream,
			"OwnedBy":                    Provider,
			"DisplayName":                model,
			"SupportedGenerationMethods": []string{"responses"},
			"SupportedInputModalities":   []string{"text", "image"},
			"SupportedOutputModalities":  []string{"text"},
			"Thinking":                   map[string]any{"Levels": []string{"low", "medium", "high", "xhigh", "max", "ultra"}},
			"UserDefined":                true,
		})
	}
	return map[string]any{"Provider": Provider, "Models": models}
}

func unmarshalYAML(raw []byte, value any) error {
	// Kept in one function so config parsing is easy to test and the service
	// package does not expose YAML details to the ABI layer.
	return yaml.Unmarshal(raw, value)
}
