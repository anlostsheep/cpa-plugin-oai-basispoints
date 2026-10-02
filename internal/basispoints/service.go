package basispoints

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	case "model.route":
		return s.routeModel(raw), nil
	case "response.intercept_after":
		return s.interceptModelCatalog(raw)
	case "executor.execute":
		return s.execute(raw, false)
	case "executor.execute_stream":
		return s.execute(raw, true)
	case "executor.count_tokens":
		// 不伪造计数成功（移植自原仓库 v0.1.12）。
		return nil, fail(400, "unsupported_token_count", "oai-basispoints does not provide an accurate standalone token count")
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
	transport := s.config().Transport
	if !stream {
		transport = TransportHTTP
	}
	summary := s.newRelaySummary(request, transport, stream)
	// 在准备凭据与建连之前判断工具可调用性，同步失败的请求也计入正确的分母。
	summary.setToolCallableFrom(request)
	request.summary = summary
	// 在任何宿主 HTTP 调用（含 prepareRequest 里的附件上传）之前登记生命周期，
	// 使 plugin.shutdown 能等待并取消它们。
	done, err := s.ensureLife(&request)
	if err != nil {
		summary.finish("", err)
		return nil, err
	}
	body, credential, err := s.prepareRequest(request)
	if err != nil {
		done()
		summary.finish("prepare_error", err)
		return nil, err
	}
	if stream {
		// 执行分支接管 done：同步返回错误时由分支调用，异步时由往返协程调用。
		return s.executeStream(request, body, credential)
	}
	defer done()
	result, err := s.executeOnce(request, body, credential)
	summary.finish("", err)
	return result, err
}

// executeOnce 是非流式执行：交付前完成全量校验；中转格式错误只在插件内重新生成一次。
func (s *Service) executeOnce(request ExecutorRequest, body map[string]any, credential credential) (any, error) {
	summary := request.relaySummary(s, TransportHTTP, false)
	source, err := requestSource(request)
	if err != nil {
		return nil, err
	}
	summary.setToolCallable(len(callableClientToolSpecs(source)) > 0)
	// 两次往返共享同一截止时间（附件上传在此之前，另行计时）。
	request.deadline = time.Now().Add(time.Duration(s.config().TimeoutSeconds) * time.Second)
	for attempt := 0; ; attempt++ {
		summary.attemptStarted()
		upstream, err := s.upstreamRequest(request, body, credential, false)
		if err != nil {
			return nil, err
		}
		if len(upstream.Body) > s.config().MaxResponseBytes {
			return nil, fail(502, "upstream_response_too_large", "Basis Points response exceeds configured limit")
		}
		response, err := parseResponse(upstream.Body, upstream.Headers)
		if err != nil {
			return nil, err
		}
		payload, _, _, stats, err := transformResponseBodyStats(jsonBytes(response), source)
		summary.validated(stats, err)
		if err == nil {
			summary.setTerminalStatus(stringValue(response["status"]))
			return map[string]any{"Payload": payload, "Headers": reencodedHeaders(upstream.Headers)}, nil
		}
		retry, ok := relayRetryBody(body, response, err, attempt)
		if !ok {
			return nil, err
		}
		s.logRegenerate(request, "http", false, err)
		summary.regenerating()
		body = retry
	}
}

// relaySummary 返回本次执行的汇总；测试直接调用执行分支时按需创建。
func (r *ExecutorRequest) relaySummary(s *Service, transport string, stream bool) *relaySummary {
	if r.summary == nil {
		r.summary = s.newRelaySummary(*r, transport, stream)
	}
	return r.summary
}

// requestSource 取客户端原始请求（优先 OriginalRequest），用于工具目录校验与回放。
func requestSource(request ExecutorRequest) (map[string]any, error) {
	source, err := rawObject(request.OriginalRequest)
	if err != nil {
		source, err = rawObject(request.Payload)
	}
	return source, err
}

// relayRetryBody 判断本次失败能否重新生成：仅首次尝试、错误是中转契约错误（422
// invalid_tool_call）、且上游没有因截断而不完整时，返回附带纠正提示（含不含参数的
// 诊断原因）的新请求体。移植自原仓库 v0.1.10（#6）。
func relayRetryBody(body map[string]any, response map[string]any, err error, attempt int) (map[string]any, bool) {
	var apiError *APIError
	if attempt != 0 || !errors.As(err, &apiError) || apiError.Kind != "invalid_tool_call" || stringValue(response["status"]) == "incomplete" {
		return nil, false
	}
	retry := cloneObject(body)
	items, _ := body["input"].([]any)
	retry["input"] = appendBeforeCompaction(append([]any{}, items...), []any{messageItem("developer", transportRetryHint+" Diagnostic: "+apiError.Message)})
	return retry, true
}

// reencodedHeaders 结果已重新编码为 JSON，不能继续使用上游的 SSE/压缩/长度等实体头。
func reencodedHeaders(upstream http.Header) http.Header {
	headers := upstream.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Type", "application/json")
	for _, name := range []string{"Content-Length", "Content-Encoding", "Transfer-Encoding", "ETag"} {
		headers.Del(name)
	}
	return headers
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
//   - grace<=0（心跳关闭）：同步等到建连完成；不提前开流（http 正文增量仍照常交付）。
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
	summary := request.relaySummary(s, s.config().Transport, true)
	summary.setToolCallableFrom(request)
	done, err := s.ensureLife(&request)
	if err != nil {
		summary.finish("", err)
		return nil, err
	}
	if request.StreamID == "" {
		done()
		err := fail(500, "stream_id_missing", "executor.execute_stream requires stream_id")
		summary.finish("prepare_error", err)
		return nil, err
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
	// 首次往返与至多一次重新生成共享同一截止时间。
	request.deadline = time.Now().Add(time.Duration(cfg.TimeoutSeconds) * time.Second)
	guard := s.newUpstreamGuard(request.HostCallbackID, credential.AccessToken, stop, runCtx.Done(), time.Duration(cfg.TimeoutSeconds)*time.Second, timeoutError(cfg))
	connected := make(chan httpConnectResult, 1)
	summary.attemptStarted()
	go func() {
		upstream, err := s.upstreamStream(request, body, credential, guard)
		connected <- httpConnectResult{upstream: upstream, err: err}
	}()
	early := awaitConnect(cfg.heartbeatInterval(), connected, guard.headers)
	if early != nil && early.err != nil {
		guard.release()
		done()
		summary.finish("connect_error", early.err)
		return nil, early.err
	}
	session := s.newStreamSession(request.StreamID, cfg.heartbeatInterval(), func() {
		stopOnce.Do(func() { close(stop) })
	})
	// 先挂汇总钩子，再启动会触发关流的看门狗（bindLifecycle / bindDeadline）。
	summary.bind(session)
	session.bindLifecycle(runCtx)
	session.bindDeadline(request.deadline, timeoutError(cfg))
	go func() {
		defer done()
		// failWith 以错误收尾；汇总由关流钩子在 host.stream.close 之前写出（带 request_id），
		// 这里的 finish 只在钩子未触发时兜底。exit 为空时按错误推断。
		failWith := func(exit string, err error) {
			summary.setExit(exit)
			session.fail(err)
			summary.finishAfter(session, exit, err)
		}
		if early != nil {
			// 已在同步窗口内建连：等上游 response.created 以上游 id 开流（首次空闲心跳先到则用合成 id）。
			session.startIdle()
		} else {
			session.start()
		}
		var upstream upstreamStream
		if early != nil {
			upstream = early.upstream
		} else {
			late := <-connected
			if late.err != nil {
				guard.release()
				failWith("connect_error", late.err)
				return
			}
			upstream = late.upstream
		}
		source, parseErr := requestSource(request)
		if parseErr != nil {
			guard.release()
			failWith("prepare_error", parseErr)
			return
		}
		callable := len(callableClientToolSpecs(source)) > 0
		summary.setToolCallable(callable)
		// buffered：本轮有可调用工具时整轮校验通过后再交付，交付前仍可重新生成一次。
		buffered := cfg.StreamToolMode == StreamToolModeBuffered && callable
		if buffered {
			summary.setDelivery("buffered")
		}
		for attempt := 0; ; attempt++ {
			// 每次往返一台新的增量交付状态机；客户端 response id 由会话固定，不随重新生成改变。
			// 正文 commit 的那一刻就记下 text_committed（看门狗可能在读取结束前强制关流）。
			delivery := newStreamDelivery(session.open, func(meta map[string]any, frames []map[string]any) error {
				summary.markTextCommitted()
				return session.deliver(meta, frames)
			})
			delivery.bufferUntilValidated = buffered
			feeder := newIncrementalFeeder(delivery)
			raw, readErr := s.readGuardedInto(upstream, guard, feeder.feed)
			// release 等待看守协程退出并关闭上游流；此后本往返不再有守卫发起的宿主回调。
			guard.release()
			if readErr == nil {
				// EOF：处理最后一个没有空行结尾的事件，让它同样经过状态机校验。
				readErr = feeder.flush()
			}
			if delivery.committed {
				summary.markTextCommitted()
			}
			if readErr != nil {
				failWith("", readErr)
				return
			}
			response, parseErr := parseResponse(raw, upstream.Headers)
			if parseErr != nil {
				failWith("", parseErr)
				return
			}
			// 先核对终态与已交付（或缓冲校验）的正文一致，再做工具整批转换（转换会写入历史身份缓存）。
			if err := delivery.validateFinal(response); err != nil {
				failWith("", err)
				return
			}
			if !delivery.committed && !delivery.bufferUntilValidated && delivery.events > 0 && hasMessageText(response) {
				// 有 SSE 事件和正文却没有增量交付（上游未按 created/item/part 顺序给出）：退回终态回放。
				s.logEvent(request, "info", "basispoints: message text replayed at terminal without incremental delivery", map[string]any{
					"transport": "http", "upstream_events": delivery.events,
				})
			}
			_, transformedResponse, _, stats, transformErr := transformResponseBodyStats(jsonBytes(response), source)
			summary.validated(stats, transformErr)
			if transformErr == nil {
				summary.setTerminalStatus(stringValue(transformedResponse["status"]))
				// finish 在持锁的最终输出边界再次检查本代是否已停止；已 commit 时只补发未交付部分。
				// keepFinal 只用于增量交付且正文已 commit 的情形；buffered 一律完整回放。
				var outcome string
				if delivery.committed {
					outcome = session.finishWith(transformedResponse, delivery.keepFinal)
				} else {
					outcome = session.finish(transformedResponse)
				}
				var outcomeErr error
				if outcome == finishStopped {
					outcomeErr = stoppedError()
				}
				summary.finishAfter(session, outcome, outcomeErr)
				return
			}
			if delivery.committed {
				// 正文已交付给客户端：不能重新生成（会重复输出），直接以请求级失败结束。
				s.logEvent(request, "warn", "basispoints: tool call invalid after text was delivered; not regenerating", map[string]any{
					"transport": "http", "kind": errorKind(transformErr),
				})
				failWith("", transformErr)
				return
			}
			retry, ok := relayRetryBody(body, response, transformErr, attempt)
			if !ok {
				failWith("", transformErr)
				return
			}
			s.logRegenerate(request, "http", true, transformErr)
			summary.regenerating()
			// 重新生成一次：新的往返使用新的守卫，同样受客户端断开、插件停止约束，超时只剩
			// 首次往返用剩的时长；心跳在此期间继续保活。此时下游流已开启，本次往返的失败
			// （含 401/403/429）只能以无状态码的流错误关闭，与延迟心跳的既有折中一致。
			body = retry
			remaining, ok := request.roundTripTimeout(cfg)
			if !ok {
				failWith("", timeoutError(cfg))
				return
			}
			guard = s.newUpstreamGuard(request.HostCallbackID, credential.AccessToken, stop, runCtx.Done(), remaining, timeoutError(cfg))
			summary.attemptStarted()
			next, err := s.upstreamStream(request, body, credential, guard)
			if err != nil {
				guard.release()
				failWith("", err)
				return
			}
			upstream = next
		}
	}()
	return streamHeaders(), nil
}

// logEvent 尽力写一条宿主日志（CPA 日志，宿主附 request_id）；失败忽略，绝不影响请求。
// 只记录类别与计数，不记录请求或响应内容。
func (s *Service) logEvent(request ExecutorRequest, level, message string, fields map[string]any) {
	payload := map[string]any{"level": level, "message": message, "fields": fields}
	if request.HostCallbackID != "" {
		payload["host_callback_id"] = request.HostCallbackID
	}
	_ = s.call("host.log", payload, nil)
}

// logRegenerate 记录一次工具调用格式错误触发的重新生成（插件内部行为，CPA 看不到）。
func (s *Service) logRegenerate(request ExecutorRequest, transport string, stream bool, err error) {
	s.logEvent(request, "info", "basispoints: regenerating once after invalid tool call", map[string]any{
		"transport": transport, "stream": stream, "kind": errorKind(err),
	})
}

// hasMessageText 判断终态里是否有非空的 message 正文。
func hasMessageText(response map[string]any) bool {
	output, _ := response["output"].([]any)
	for _, value := range output {
		item := objectValue(value)
		if stringValue(item["type"]) != "message" {
			continue
		}
		content, _ := item["content"].([]any)
		for _, part := range content {
			if text, _ := objectValue(part)["text"].(string); text != "" {
				return true
			}
		}
	}
	return false
}

// incrementalFeeder 是 readGuardedInto 的分块回调：按首个非空白字节判别 JSON / SSE，
// SSE 逐块解码并交给增量交付状态机；JSON 响应只缓冲、不做增量。
type incrementalFeeder struct {
	delivery *streamDelivery
	decoder  *sseDecoder
	head     []byte
	detected bool
	sse      bool
}

func newIncrementalFeeder(delivery *streamDelivery) *incrementalFeeder {
	return &incrementalFeeder{delivery: delivery, decoder: newSSEDecoder()}
}

func (f *incrementalFeeder) feed(chunk []byte) error {
	data := chunk
	if !f.detected {
		f.head = append(f.head, chunk...)
		trimmed := bytes.TrimSpace(f.head)
		if len(trimmed) == 0 {
			return nil
		}
		f.detected, f.sse = true, trimmed[0] != '{'
		data, f.head = f.head, nil
	}
	if !f.sse {
		return nil
	}
	return f.decoder.feed(data, f.delivery.consume)
}

// flush 在正常读完后补一个空行，处理末尾未以空行结束的事件。
func (f *incrementalFeeder) flush() error {
	if !f.sse {
		return nil
	}
	return f.decoder.feed([]byte("\n\n"), f.delivery.consume)
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
		"stream_tool_mode":  cfg.StreamToolMode,
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
				{"Name": "transport", "Type": "string", "Description": "上游传输方式：http（默认，正文增量、工具/推理终态回放）或 ws（WebSocket，整轮回放）。"},
				{"Name": "proxy_url", "Type": "string", "Description": "WS 传输的出站代理 URL（仅 transport=ws 生效；http 传输走 CPA 全局 proxy-url），留空表示直连。"},
				{"Name": "heartbeat_seconds", "Type": "integer", "Description": "流式心跳间隔（秒），默认 15；0 关闭（仅关闭心跳，http 正文增量不受影响）。无输出时防止长回合被下游空闲超时切断。"},
				{"Name": "stream_tool_mode", "Type": "string", "Description": "http 流式在有可调用工具的回合如何交付：incremental（默认，正文边读边交付）或 buffered（整轮校验通过后再交付，交付前可重新生成一次，但首字延迟增大；无工具或 tool_choice=none 的回合不受影响）。"},
				{"Name": "alpha_search_model", "Type": "string", "Description": "Basis Points 模型的 Codex 网页搜索（/v1/alpha/search）改由原生 codex 凭据处理时，用于挑选凭据的原生模型名（如 gpt-6-luna）；留空关闭。不能填 Basis Points 模型。"},
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
			// 只在配置了网页搜索改道时声明：声明后 CPA 每个请求都会先调用 model.route。
			// reconfigure 时 CPA 会按新注册结果重建能力，开关可热生效。
			"model_router": cfg.AlphaSearchModel != "",
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
