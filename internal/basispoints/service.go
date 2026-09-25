package basispoints

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type Service struct {
	attachments attachmentCache
	mu          sync.RWMutex
	cfg         Config
	host        HostCall
	stopped     bool

	// life 是当前生命周期（一代）。每次往返在 beginStream 时绑定进入时的那一代，之后只引用
	// 它自己的 ctx 与计数；shutdown 只取消并等待它那一代，configure 重建的新一代与旧往返
	// 完全隔离。lifeMu 串行化「停止（含等待）」与「重建」这两个状态转换。
	life   *lifecycle
	lifeMu sync.Mutex
}

// lifecycle 是插件的一代运行期：ctx 在 shutdown 时以 errPluginStopped 为 cause 取消，
// wg 跟踪这一代里进行中的流式往返。
type lifecycle struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
}

func newLifecycle() *lifecycle {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &lifecycle{ctx: ctx, cancel: cancel}
}

// errPluginStopped 是生命周期 ctx 的取消 cause。往返用 context.Cause 区分「插件停止」
// 与「客户端断开」（普通 cancel）和「超时」（DeadlineExceeded）。
var errPluginStopped = errors.New("oai-basispoints plugin stopped")

func stoppedByShutdown(ctx context.Context) bool {
	return ctx != nil && errors.Is(context.Cause(ctx), errPluginStopped)
}

// shutdownWait 是 plugin.shutdown 等待进行中流式往返退出的上限。
var shutdownWait = 5 * time.Second // var：测试可缩短

func NewService() *Service {
	cfg := defaultConfig()
	return &Service{cfg: cfg, life: newLifecycle()}
}

// beginStream 登记一次流式往返并绑定当前这一代。插件已停止时拒绝；返回的 ctx 在该代
// shutdown 时取消（cause=errPluginStopped），done 必须在往返（含最后一次宿主回调）结束后调用。
func (s *Service) beginStream() (context.Context, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, nil, fail(503, "plugin_stopped", "oai-basispoints is shut down")
	}
	life := s.life
	life.wg.Add(1)
	return life.ctx, life.wg.Done, nil
}

// shutdown 停止当前这一代：拒绝新往返、以 errPluginStopped 取消其 ctx，并等待这一代的
// 往返退出（上限 shutdownWait）。持有 lifeMu 直到等待结束，使并发的 configure 不能在
// 等待期间重建新一代。
func (s *Service) shutdown() {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	s.mu.Lock()
	s.stopped = true
	life := s.life
	s.mu.Unlock()
	life.cancel(errPluginStopped)
	finished := make(chan struct{})
	go func() {
		life.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(shutdownWait):
	}
}

// stoppedError 是往返因 plugin.shutdown 被取消时的错误：属于本次请求，向客户端发
// response.failed 后正常关闭，不让 CPA 因插件重载而冷却凭据。
func stoppedError() error {
	return fail(503, "plugin_stopped", "oai-basispoints was shut down while the request was in flight")
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
	presentKeys := map[string]bool{}
	if len(request.ConfigYAML) > 0 {
		if err := unmarshalYAML(request.ConfigYAML, &cfg); err != nil {
			return fail(400, "invalid_config", "plugin configuration is invalid: "+err.Error())
		}
		var keys map[string]any
		if err := unmarshalYAML(request.ConfigYAML, &keys); err == nil {
			for key := range keys {
				presentKeys[key] = true
			}
		}
	}
	// settings.json 只是「生效配置」的镜像：宿主 YAML（面板）里出现的键一律以 YAML 为准，
	// settings.json 只补 YAML 未提供的键。旧实现让 settings.json 覆盖 YAML，导致面板修改
	// 被旧镜像吞掉；外部刷新脚本读取该镜像中的 dedicated_auth_files 作为单一标记来源，
	// 因此镜像必须忠实反映面板配置。Token 永不写入这里。
	if cfg.DataDir != "" {
		if err := fillFromMirror(&cfg, filepath.Join(cfg.DataDir, "settings.json"), presentKeys); err != nil {
			return err
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
		if err := writeFileAtomic(filepath.Join(cfg.DataDir, "settings.json"), data, 0600); err != nil {
			return fail(500, "config_storage", "cannot persist plugin settings")
		}
	}
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	s.mu.Lock()
	s.cfg = cfg
	if s.stopped {
		s.life = newLifecycle()
	}
	s.stopped = false
	s.mu.Unlock()
	return nil
}

// fillFromMirror 用已有的 settings.json 镜像补齐 YAML 未提供的键。镜像不存在视为无镜像；
// 存在但不可读、不是 JSON 对象、或某个补缺字段类型不对，都是配置错误（fail-closed）：
// 否则损坏的 dedicated_auth_files 会被当作空列表，并在随后的原子写入中覆盖原镜像，
// 让插件与外部刷新脚本同时「忘记」专用标记。
func fillFromMirror(cfg *Config, path string, presentKeys map[string]bool) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fail(500, "invalid_config", "cannot read plugin settings mirror "+path)
	}
	var saved map[string]json.RawMessage
	if err := json.Unmarshal(data, &saved); err != nil || saved == nil {
		return fail(500, "invalid_config", "plugin settings mirror "+path+" is not a JSON object; fix or remove it")
	}
	keys := make([]string, 0, len(saved))
	for key := range saved {
		if !presentKeys[key] {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	nullable := nullableConfigFields()
	for _, key := range keys {
		// encoding/json 把 null 解码到非指针标量时静默跳过，不报错；镜像由插件自己写出，
		// 只有指针/切片/映射字段（nil 时写成 null）可以为 null，其余出现 null 即为损坏。
		if isJSONNull(saved[key]) {
			if allowed, known := nullable[key]; known && !allowed {
				return fail(500, "invalid_config", "plugin settings mirror "+path+": field "+key+" must not be null")
			}
			continue
		}
		if err := json.Unmarshal(jsonBytes(map[string]json.RawMessage{key: saved[key]}), cfg); err != nil {
			return fail(500, "invalid_config", "plugin settings mirror "+path+": field "+key+" has an invalid type")
		}
	}
	return nil
}

// nullableConfigFields 按 Config 的 json 标签列出各字段是否允许 null。
func nullableConfigFields() map[string]bool {
	out := map[string]bool{}
	t := reflect.TypeOf(Config{})
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		switch field.Type.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			out[name] = true
		default:
			out[name] = false
		}
	}
	return out
}

func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
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
		return authParseWithDedicated(raw, s.config().dedicatedSet())
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
		s.shutdown()
		return map[string]any{}, nil
	default:
		return nil, fail(400, "unsupported_method", "unsupported plugin method: "+method)
	}
}

func (s *Service) execute(raw json.RawMessage, stream bool) (any, error) {
	var request ExecutorRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, fail(400, "invalid_request", "executor request is invalid")
	}
	// 在任何宿主 HTTP 调用（含 prepareRequest 里的附件上传）之前登记生命周期，
	// 使 plugin.shutdown 能等待并取消它们。
	done, err := s.ensureLife(&request)
	if err != nil {
		return nil, err
	}
	body, credential, err := s.prepareRequest(request)
	if err != nil {
		done()
		return nil, err
	}
	if stream {
		// 执行分支接管 done：同步返回错误时由分支调用，异步时由往返协程调用。
		return s.executeStream(request, body, credential)
	}
	defer done()
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

// streamHeaders 是流式执行成功时同步返回给宿主的 SSE 头部。
func streamHeaders() map[string]any {
	return map[string]any{"Headers": map[string][]string{"Content-Type": {"text/event-stream"}, "Cache-Control": {"no-cache"}}}
}

// awaitConnect 实现「延迟心跳」：建连（http 等待响应头 / ws 拨号 + 首帧）在后台进行，
// 最多同步等待 grace（= 心跳间隔）。
//   - grace 内完成：返回结果。建连失败时下游一个字节都没有，调用方同步返回**带状态码**
//     的错误，CPA 能按 401/403/429 正确换号或冷却（绝大多数凭据/限流错误都在此窗口内）。
//   - grace 到期仍未完成：返回 nil，调用方先把流交给宿主并开始心跳，防止 sub2api(180s)
//     空闲超时；之后的建连失败只能以无状态码的流错误关闭（已接受的折中）。
//   - grace<=0（心跳关闭）：同步等到建连完成，保持 v0.1.10 行为。
func awaitConnect[T any](grace time.Duration, connected <-chan T, headers <-chan struct{}) *T {
	if grace <= 0 {
		result := <-connected
		return &result
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case result := <-connected:
		return &result
	case <-timer.C:
	}
	// 计时器与结果可能同时就绪（select 随机选择）：先非阻塞地领取已完成的结果。
	select {
	case result := <-connected:
		return &result
	default:
	}
	// 响应头已在窗口内到达（状态码已知）时，结果只差有界的错误正文读取：等它，
	// 让窗口内到达的 401/429 仍以带状态码的错误同步返回。
	if headers != nil {
		select {
		case <-headers:
			result := <-connected
			return &result
		default:
		}
	}
	return nil
}

type httpConnectResult struct {
	upstream upstreamStream
	err      error
}

func (s *Service) executeStream(request ExecutorRequest, body map[string]any, credential credential) (any, error) {
	done, err := s.ensureLife(&request)
	if err != nil {
		return nil, err
	}
	if request.StreamID == "" {
		done()
		return nil, fail(500, "stream_id_missing", "executor.execute_stream requires stream_id")
	}
	if s.config().Transport == TransportWS {
		return s.executeStreamWS(request, body, credential)
	}
	runCtx := request.lifeCtx
	cfg := s.config()
	// stop 在心跳 emit 失败（客户端断开）时关闭；守卫在整个往返（含等待响应头）内
	// 监听 stop / 本代 shutdown / 超时，并能取消阻塞中的 do_stream 与 stream_read。
	stop := make(chan struct{})
	var stopOnce sync.Once
	guard := s.newUpstreamGuard(request.HostCallbackID, credential.AccessToken, stop, runCtx.Done(), time.Duration(cfg.TimeoutSeconds)*time.Second, timeoutError(cfg))
	connected := make(chan httpConnectResult, 1)
	go func() {
		upstream, err := s.upstreamStream(request, body, credential, guard)
		connected <- httpConnectResult{upstream: upstream, err: err}
	}()
	early := awaitConnect(cfg.heartbeatInterval(), connected, guard.headers)
	if early != nil && early.err != nil {
		guard.release()
		done()
		return nil, early.err
	}
	session := s.newStreamSession(request.StreamID, cfg.heartbeatInterval(), func() {
		stopOnce.Do(func() { close(stop) })
	})
	session.bindLifecycle(runCtx)
	go func() {
		defer done()
		session.start()
		var upstream upstreamStream
		if early != nil {
			upstream = early.upstream
		} else {
			late := <-connected
			if late.err != nil {
				guard.release()
				session.fail(late.err)
				return
			}
			upstream = late.upstream
		}
		raw, readErr := s.readGuarded(upstream, guard)
		// release 等待看守协程退出并关闭上游流；此后本往返不再有守卫发起的宿主回调。
		guard.release()
		if readErr != nil {
			session.fail(readErr)
			return
		}
		source, parseErr := rawObject(request.OriginalRequest)
		if parseErr != nil {
			source, parseErr = rawObject(request.Payload)
		}
		if parseErr != nil {
			session.fail(parseErr)
			return
		}
		response, parseErr := parseFinalStreamResponse(raw)
		if parseErr != nil {
			session.fail(parseErr)
			return
		}
		_, transformedResponse, _, transformErr := transformResponseBody(jsonBytes(response), source)
		if transformErr != nil {
			session.fail(transformErr)
			return
		}
		// finish 在持锁的最终输出边界再次检查本代是否已停止。
		session.finish(transformedResponse)
	}()
	return streamHeaders(), nil
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
		"transport":         cfg.Transport,
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
			"Author":           "anlostsheep",
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
				{"Name": "dedicated_auth_files", "Type": "array", "Description": "标记为 Excel 专用的凭据文件名；这些文件只暴露 oai-basispoints 虚拟认证，CPA 不再调度其原生刷新。"},
				{"Name": "user_agent", "Type": "string", "Description": "网页版 User-Agent，默认取自 HAR 抓包。"},
				{"Name": "ua_platform", "Type": "string", "Description": "x-openai-internal-basispoints-browser-ua-platform 的值，默认 macOS。"},
				{"Name": "ua_brands", "Type": "string", "Description": "x-openai-internal-basispoints-browser-ua-brands 的值。"},
				{"Name": "chrome_version", "Type": "string", "Description": "浏览器主版本号，默认 153.0.0.0。"},
				{"Name": "transport", "Type": "string", "Description": "上游传输方式：http（默认，缓冲回放）或 ws（WebSocket）。"},
				{"Name": "proxy_url", "Type": "string", "Description": "WS 传输的出站代理 URL（仅 transport=ws 生效；http 传输走 CPA 全局 proxy-url），留空表示直连。"},
				{"Name": "heartbeat_seconds", "Type": "integer", "Description": "流式心跳间隔（秒），默认 15；0 关闭。防止长回合被下游空闲超时切断。"},
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

// writeFileAtomic 同目录临时文件 + fsync + rename，避免外部刷新脚本读到半写的镜像。
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".settings-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
