package basispoints

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// WebSocket 传输实现（transport: ws）。真实 basispoints 网页版走 WebSocket：
// 每次 executor.execute_stream 建一条新连接、发一帧 response.create（input 为全量
// 历史）、读取事件流、读到 response.completed 后透传 usage 并关闭连接。不做连接
// 复用或连接池；失败时绝不自动重放已经输出过的内容。bearer 只出现在
// Sec-WebSocket-Protocol 子协议里，绝不写入任何日志或错误字符串。

const wsControlFrames = "upstream_sent,heartbeat,resume,cancel,server_draining"

// randomHex 返回 n 字节的随机十六进制串，用于连接亲和标识与 resume token。
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// 退化到时间派生值也不影响正确性：这些标识只需每连接唯一。
		return hex.EncodeToString([]byte(time.Now().UTC().Format("150405.000000000")))
	}
	return hex.EncodeToString(buf)
}

// basisPointsWSURL 由 responses_url 推导 wss 地址，并附带网页版实测的查询参数。
func basisPointsWSURL(cfg Config, c credential) (string, error) {
	u, err := url.Parse(strings.TrimSpace(cfg.ResponsesURL))
	if err != nil || u.Host == "" {
		return "", fail(500, "invalid_config", "cannot derive Basis Points WebSocket URL from responses_url")
	}
	switch u.Scheme {
	case "https", "wss":
		u.Scheme = "wss"
	case "http", "ws":
		u.Scheme = "ws"
	default:
		return "", fail(500, "invalid_config", "responses_url must be an http(s) URL for WebSocket transport")
	}
	authMode := strings.TrimSpace(c.AuthMode)
	if authMode == "" {
		authMode = "chatgpt"
	}
	query := url.Values{}
	query.Set("bps_client_info", string(jsonBytes(basispointsClientInfo(cfg))))
	query.Set("bps_auth_mode", authMode)
	query.Set("bps_ws_affinity", "affinity_"+randomHex(16))
	query.Set("bps_control_frames", wsControlFrames)
	u.RawQuery = query.Encode()
	return u.String(), nil
}

// proxiedHTTPClient 按 proxy_url 构造出站 HTTP 客户端；留空则直连。
func proxiedHTTPClient(cfg Config) (*http.Client, error) {
	if strings.TrimSpace(cfg.ProxyURL) == "" {
		return &http.Client{}, nil
	}
	proxyURL, err := url.Parse(cfg.ProxyURL)
	if err != nil {
		return nil, fail(400, "invalid_config", "proxy_url is invalid")
	}
	return &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}, nil
}

func (s *Service) dialBasisPointsWS(ctx context.Context, cfg Config, c credential) (*websocket.Conn, error) {
	wsURL, err := basisPointsWSURL(cfg, c)
	if err != nil {
		return nil, err
	}
	client, err := proxiedHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	userAgent := strings.TrimSpace(cfg.UserAgent)
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	// 子协议同时携带 responses 与不记录的 bearer；启用 permessage-deflate。
	opts := &websocket.DialOptions{
		HTTPClient:      client,
		Subprotocols:    []string{"responses", "openai-bearer." + c.AccessToken},
		CompressionMode: websocket.CompressionContextTakeover,
		HTTPHeader: http.Header{
			"User-Agent": []string{userAgent},
			"Origin":     []string{"https://bps.openai.com"},
		},
	}
	conn, resp, err := websocket.Dial(ctx, wsURL, opts)
	if err != nil {
		return nil, wsDialError(ctx, resp, err, c.AccessToken)
	}
	if cfg.MaxResponseBytes > 0 {
		conn.SetReadLimit(int64(cfg.MaxResponseBytes))
	}
	return conn, nil
}

// wsDialError 把握手失败转换为不含令牌的错误。依赖库在服务器回显了意外的
// Sec-WebSocket-Protocol 时会把子协议（含 openai-bearer.<token>）写进错误串，
// 因此 HTTP 拒绝只报固定文案 + 状态码；其他错误先按当前令牌精确脱敏再做前缀脱敏。
// 握手 HTTP 拒绝保留状态码（401/403/429 由 CPA 冷却或换号），一律归为传输类错误：
// 握手阶段尚无下游输出，交给 CPA 处理比向客户端发 response.failed 更合适。
func wsDialError(ctx context.Context, resp *http.Response, err error, token string) error {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fail(504, "upstream_timeout", "Basis Points WebSocket handshake timed out")
	case stoppedByShutdown(ctx):
		return stoppedError()
	case ctx.Err() != nil:
		return fail(499, "client_disconnected", "request cancelled during Basis Points WebSocket handshake")
	}
	if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols && resp.StatusCode != 0 {
		status := resp.StatusCode
		if status < 400 || status > 599 {
			status = 502
		}
		return fail(status, "upstream_transport", fmt.Sprintf("Basis Points WebSocket handshake rejected (HTTP %d)", resp.StatusCode))
	}
	if resp != nil {
		return fail(502, "upstream_transport", "Basis Points WebSocket handshake failed: protocol negotiation error")
	}
	return fail(502, "upstream_transport", "Basis Points WebSocket dial failed: "+redactSecret(err.Error(), token))
}

// writeCancelFrame 在取消/客户端断开时通知上游停止本轮，然后由调用方关闭连接。
func writeCancelFrame(conn *websocket.Conn, requestID, resumeToken string) {
	payload := map[string]any{
		"type":         "basispoints.response.cancel",
		"request_id":   requestID,
		"resume_token": resumeToken,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, jsonBytes(payload))
}

// wsTurn 是一次 WS 往返：建连并发出 response.create 之后的连接状态。
//
// 拆成「建连」与「读取」两段：建连阶段（拨号、发送首帧）失败时下游尚未收到任何字节，
// 调用方可带 error 关闭流，让 CPA 按状态换号/冷却；建连成功后才开始向下游发送心跳。
type wsTurn struct {
	cfg         Config
	conn        *websocket.Conn
	requestID   string
	resumeToken string
	closed      bool
}

func (t *wsTurn) closeConn(code websocket.StatusCode) {
	if !t.closed {
		t.closed = true
		_ = t.conn.Close(code, "")
	}
}

// openWSTurn 拨号并发送首帧 response.create（input 为全量历史，附三个 WS 专用字段）。
func (s *Service) openWSTurn(ctx context.Context, body map[string]any, c credential) (*wsTurn, error) {
	cfg := s.config()
	conn, err := s.dialBasisPointsWS(ctx, cfg, c)
	if err != nil {
		return nil, err
	}
	turn := &wsTurn{
		cfg:         cfg,
		conn:        conn,
		requestID:   uuidV5("cpa-oai-basispoints/ws-request/" + randomHex(16)),
		resumeToken: "resume_" + randomHex(16),
	}
	// 首帧 response.create：复用 prepareResponsesBody 产物，附加三个 WS 专用字段。
	// input 已是全量历史，不做裁剪。全新连接首帧即 response.create，无需引导帧。
	create := cloneObject(body)
	if create == nil {
		create = map[string]any{}
	}
	create["type"] = "response.create"
	create["basispoints_request_id"] = turn.requestID
	create["basispoints_resume_token"] = turn.resumeToken
	if err := conn.Write(ctx, websocket.MessageText, jsonBytes(create)); err != nil {
		if ctx.Err() != nil {
			writeCancelFrame(conn, turn.requestID, turn.resumeToken)
		}
		turn.closeConn(websocket.StatusGoingAway)
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			return nil, fail(504, "upstream_timeout", "Basis Points WebSocket send timed out")
		case stoppedByShutdown(ctx):
			return nil, stoppedError()
		case ctx.Err() != nil:
			return nil, fail(499, "client_disconnected", "request cancelled before Basis Points WebSocket send")
		}
		return nil, fail(502, "upstream_transport", "Basis Points WebSocket send failed: "+redactSecret(err.Error(), c.AccessToken))
	}
	return turn, nil
}

// streamOverWS 建立一条 WS、发送 response.create、读取事件直至 response.completed，
// 返回完整的 completed 响应对象（含 usage）。失败时不重放。
func (s *Service) streamOverWS(ctx context.Context, request ExecutorRequest, body map[string]any, c credential) (map[string]any, error) {
	turn, err := s.openWSTurn(ctx, body, c)
	if err != nil {
		return nil, err
	}
	return turn.readUntilCompleted(ctx)
}

// readUntilCompleted 读取事件直到 response.completed；ctx 取消（超时或客户端断开）时
// 发送 cancel 帧后返回错误。返回时关闭连接。
func (t *wsTurn) readUntilCompleted(ctx context.Context) (map[string]any, error) {
	cfg := t.cfg
	conn := t.conn
	requestID := t.requestID
	resumeToken := t.resumeToken
	closeConn := t.closeConn
	defer closeConn(websocket.StatusNormalClosure)

	// coder/websocket 在 Read 的 ctx 被取消时会直接关闭连接，届时 cancel 帧已无法发出。
	// 因此读取使用独立的 readCtx；另起 goroutine 监听请求 ctx：一旦取消/超时，先发送
	// cancel 帧，再取消 readCtx 解除阻塞的 Read。两者之间无其他并发写，符合库的读写约束。
	var resumeMu sync.Mutex
	serverResume := resumeToken
	currentResume := func() string {
		resumeMu.Lock()
		defer resumeMu.Unlock()
		return serverResume
	}
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()
	loopDone := make(chan struct{})
	defer close(loopDone)
	cancelSent := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			writeCancelFrame(conn, requestID, currentResume())
			close(cancelSent)
			readCancel()
		case <-loopDone:
		}
	}()

	for {
		_, data, readErr := conn.Read(readCtx)
		if readErr != nil {
			// 取消/超时：cancel 帧已由监听 goroutine 发出；绝不重放已输出内容。
			if ctx.Err() != nil {
				select {
				case <-cancelSent:
				case <-time.After(3 * time.Second):
				}
				closeConn(websocket.StatusGoingAway)
				if ctx.Err() == context.DeadlineExceeded {
					return nil, timeoutError(cfg)
				}
				// 本代 shutdown 以 errPluginStopped 为 cause 取消 ctx；普通 cancel 才是客户端断开。
				if stoppedByShutdown(ctx) {
					return nil, stoppedError()
				}
				return nil, fail(499, "client_disconnected", "client disconnected while receiving Basis Points stream")
			}
			return nil, fail(502, "upstream_transport", "Basis Points WebSocket closed before response.completed")
		}
		frame, ferr := rawObject(data)
		if ferr != nil {
			// 单帧坏 JSON 不影响整体，跳过；完成帧缺失会在连接关闭时报错。
			continue
		}
		switch typeName := stringValue(frame["type"]); {
		case typeName == "basispoints.response.resume_token":
			if token := stringValue(frame["resume_token"]); token != "" {
				resumeMu.Lock()
				serverResume = token
				resumeMu.Unlock()
			}
		case typeName == "response.completed":
			if response := objectValue(frame["response"]); response != nil {
				return response, nil
			}
			return nil, fail(502, "invalid_upstream_response", "Basis Points response.completed carried no response object")
		case isUpstreamFailureEvent(typeName):
			// 半途失败：报错，不重放。先解析帧内状态/错误码：凭据失效与限流保留
			// 401/403/429 交给 CPA，其余归为本次请求失败。
			return nil, classifyUpstreamFailure(frame, typeName)
		case strings.Contains(typeName, "server_draining"):
			return nil, fail(503, "server_draining", "Basis Points server is draining; retry the whole turn")
		default:
			// response.created / in_progress / output_item.* / *_delta / metadata /
			// upstream_sent / heartbeat 等：本传输在 completed 帧一次性拿到全量输出，
			// 无需逐帧累积。心跳与任意帧到达都视为连接存活。
			if response := objectValue(frame["response"]); response != nil && stringValue(response["status"]) == "completed" {
				return response, nil
			}
		}
	}
}

// executeStreamWS 是 executor.execute_stream 的 WS 分支：同步返回 SSE 头部，
// 在 goroutine 内完成一次 WS 往返，读到 completed 后复用 transformResponseBody +
// syntheticStream 一次性回放给客户端，透传 usage。失败时只关闭流、绝不重放。
type wsConnectResult struct {
	turn *wsTurn
	err  error
}

func (s *Service) executeStreamWS(request ExecutorRequest, body map[string]any, c credential) (any, error) {
	cfg := s.config()
	done, err := s.ensureLife(&request)
	if err != nil {
		return nil, err
	}
	runCtx := request.lifeCtx
	source, parseErr := rawObject(request.OriginalRequest)
	if parseErr != nil {
		source, parseErr = rawObject(request.Payload)
	}
	if parseErr != nil {
		done()
		return nil, parseErr
	}
	// ctx 派生自插件这一代的生命周期：plugin.shutdown 以 errPluginStopped 取消它，往返
	// 随即发送 cancel 帧并退出；心跳 emit 失败（客户端断开）也会 cancel 它。
	ctx, cancel := context.WithTimeout(runCtx, time.Duration(cfg.TimeoutSeconds)*time.Second)
	// 建连（拨号 + 首帧）在后台进行，按「延迟心跳」规则最多同步等待一个心跳间隔：
	// 窗口内失败时下游无任何字节，同步返回带状态码的错误交给 CPA。
	connected := make(chan wsConnectResult, 1)
	go func() {
		turn, err := s.openWSTurn(ctx, body, c)
		connected <- wsConnectResult{turn: turn, err: err}
	}()
	early := awaitConnect[wsConnectResult](cfg.heartbeatInterval(), connected, nil)
	if early != nil && early.err != nil {
		cancel()
		done()
		return nil, early.err
	}
	session := s.newStreamSession(request.StreamID, cfg.heartbeatInterval(), cancel)
	session.bindLifecycle(runCtx)
	go func() {
		defer done()
		defer cancel()
		session.start()
		var turn *wsTurn
		if early != nil {
			turn = early.turn
		} else {
			late := <-connected
			if late.err != nil {
				session.fail(late.err)
				return
			}
			turn = late.turn
		}
		completed, err := turn.readUntilCompleted(ctx)
		if err != nil {
			session.fail(err)
			return
		}
		_, transformed, _, transformErr := transformResponseBody(jsonBytes(completed), source)
		if transformErr != nil {
			session.fail(transformErr)
			return
		}
		// finish 在持锁的最终输出边界再次检查本代是否已停止。
		session.finish(transformed)
	}()
	return streamHeaders(), nil
}
