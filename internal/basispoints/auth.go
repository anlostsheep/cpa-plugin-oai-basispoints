package basispoints

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type authParseRequest struct {
	Provider string `json:"Provider"`
	Path     string `json:"Path"`
	FileName string `json:"FileName"`
	RawJSON  []byte `json:"RawJSON"`
}

type authRefreshRequest struct {
	AuthID      string         `json:"AuthID"`
	StorageJSON []byte         `json:"StorageJSON"`
	Metadata    map[string]any `json:"Metadata"`
}

type credential struct {
	AccessToken   string
	AccountID     string
	AccountUserID string
	AuthMode      string
	Email         string
	ExpiresAt     time.Time
	// ProxyURL 是凭据文件顶层的 proxy_url（按凭据设置的出口；CPA 全局 proxy-url 可为空）。
	// 插件把它原样交给 CPA（记录的 ProxyURL），CPA 据此为执行上下文注入对应传输，
	// 插件经宿主发出的 http 请求随之走该出口；ws 传输也优先使用它。
	ProxyURL string
}

func parseCredential(raw []byte) (credential, error) {
	var root map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil || root == nil {
		return credential{}, fail(400, "invalid_auth", "OAuth credential is not valid JSON")
	}
	token := findToken(root)
	if token == "" {
		return credential{}, fail(401, "invalid_auth", "ChatGPT OAuth credential has no access_token")
	}
	claims := jwtPayload(token)
	accountID := accountIDFromClaims(claims)
	if accountID == "" {
		accountID = findAccountID(root)
	}
	if accountID == "" {
		return credential{}, fail(401, "invalid_auth", "ChatGPT OAuth credential has no account ID")
	}
	authMode := firstString(root, "auth_mode", "authMode")
	if !strings.EqualFold(authMode, "chatgpt") {
		authMode = "chatgpt"
	}
	email := firstString(root, "email")
	if email == "" {
		email = stringValue(claims["email"])
	}
	expiresAt := jwtExpiry(claims)
	if rawExpiry := firstValue(root, "expires_at", "expired"); expiresAt.IsZero() {
		expiresAt = timeFromValue(rawExpiry)
	}
	accountUserID := accountUserIDFromClaims(claims)
	if accountUserID == "" {
		accountUserID = firstString(root, "chatgpt_account_user_id", "account_user_id")
	}
	proxyURL := strings.TrimSpace(stringValue(root["proxy_url"]))
	return credential{
		ProxyURL:      proxyURL,
		AccessToken:   token,
		AccountID:     accountID,
		AccountUserID: accountUserID,
		AuthMode:      authMode,
		Email:         email,
		ExpiresAt:     expiresAt,
	}, nil
}

// accountUserIDFromClaims 读取 x-openai-account-user-id 头所需的复合用户标识，
// 取不到时返回空字符串，调用方据此跳过该头。
func accountUserIDFromClaims(claims map[string]any) string {
	if auth, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		if id := firstString(auth, "chatgpt_account_user_id", "account_user_id"); id != "" {
			return id
		}
	}
	return firstString(claims, "chatgpt_account_user_id", "account_user_id")
}

func findToken(root map[string]any) string {
	for _, key := range []string{"access_token", "accessToken"} {
		if token := stringValue(root[key]); token != "" {
			return strings.TrimPrefix(strings.TrimSpace(token), "Bearer ")
		}
	}
	for _, key := range []string{"token_data", "tokenData", "sessionInfo", "session_info", "oauth", "tokens"} {
		if nested, ok := root[key].(map[string]any); ok {
			if token := findToken(nested); token != "" {
				return token
			}
		}
	}
	return ""
}

func findAccountID(root map[string]any) string {
	for _, key := range []string{"userInfo", "user_info", "auth", "token_data", "tokenData", "sessionInfo", "session_info"} {
		if nested, ok := root[key].(map[string]any); ok {
			if id := firstString(nested, "chatgpt_account_id", "account_id", "accountId"); id != "" {
				return id
			}
		}
	}
	return firstString(root, "chatgpt_account_id", "account_id", "accountId")
}

func firstString(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringValue(object[key]); value != "" {
			return value
		}
	}
	return ""
}

func firstValue(object map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := object[key]; ok {
			return value
		}
	}
	return nil
}

func jwtPayload(token string) map[string]any {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return map[string]any{}
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		data, err = base64.URLEncoding.DecodeString(parts[1])
	}
	if err != nil {
		return map[string]any{}
	}
	var claims map[string]any
	if json.Unmarshal(data, &claims) != nil || claims == nil {
		return map[string]any{}
	}
	return claims
}

func accountIDFromClaims(claims map[string]any) string {
	if auth, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		if accountID := firstString(auth, "chatgpt_account_id", "account_id"); accountID != "" {
			return accountID
		}
	}
	return firstString(claims, "chatgpt_account_id", "account_id")
}

func jwtExpiry(claims map[string]any) time.Time {
	if claims == nil {
		return time.Time{}
	}
	if value, ok := claims["exp"]; ok {
		return timeFromValue(value)
	}
	return time.Time{}
}

func timeFromValue(value any) time.Time {
	switch number := value.(type) {
	case json.Number:
		if seconds, err := number.Int64(); err == nil && seconds > 0 {
			return time.Unix(seconds, 0)
		}
	case float64:
		if number > 0 {
			return time.Unix(int64(number), 0)
		}
	case int64:
		if number > 0 {
			return time.Unix(number, 0)
		}
	case string:
		if timestamp, err := time.Parse(time.RFC3339, strings.TrimSpace(number)); err == nil {
			return timestamp
		}
	}
	return time.Time{}
}

// authFileIdentity 返回 CPA 传入文件在 auth-dir 内的文件名（basename），作为专用标记的
// 匹配身份。与外部刷新脚本的标记契约保持一致：区分大小写、不做空白归一，不使用 inode。
func authFileIdentity(name string) string {
	base := filepath.Base(name)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return ""
	}
	return base
}

// validDedicatedEntry 校验标记契约条目：非空裸文件名，任何位置都不含空白或控制字符，
// 不含路径分隔符。规则与外部刷新脚本 _load_marking 完全一致（Go 的 unicode.IsSpace ∪
// unicode.IsControl 与 Python 的 str.isspace() ∪ 类别 Cc 覆盖同一字符集）；不合法即
// 配置错误，不静默跳过（静默跳过会让一侧漏标，重新引入 native 刷新竞态）。
func validDedicatedEntry(entry string) bool {
	if entry == "" {
		return false
	}
	if strings.IndexFunc(entry, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return false
	}
	if strings.ContainsAny(entry, "/\\") {
		return false
	}
	return entry != "." && entry != ".." && filepath.Base(entry) == entry
}

func credentialID(fileName string) string {
	base := strings.TrimSpace(filepath.Base(fileName))
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if base == "" {
		base = "chatgpt"
	}
	var builder strings.Builder
	builder.WriteString("bp-")
	for _, character := range base {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			builder.WriteRune(character)
		} else {
			builder.WriteByte('-')
		}
	}
	return builder.String()
}

func authData(raw []byte, fileName string, c credential) map[string]any {
	id := credentialID(fileName)
	label := c.Email
	if label == "" {
		label = fileName
	}
	return map[string]any{
		"Provider":    Provider,
		"ID":          id,
		"FileName":    fileName,
		"Label":       label,
		"StorageJSON": raw,
		// 按凭据出口：CPA 以记录的 ProxyURL 构造 Auth.ProxyURL，并为执行上下文注入该出口的传输。
		"ProxyURL": c.ProxyURL,
		"Metadata": map[string]any{
			"type":       Provider,
			"auth_kind":  "oauth",
			"account_id": c.AccountID,
			"auth_mode":  c.AuthMode,
		},
		"Attributes": map[string]string{
			"auth_kind":  "oauth",
			"account_id": c.AccountID,
			"auth_mode":  c.AuthMode,
		},
	}
}

func nativeCodexAuthData(raw []byte, fileName string, c credential) (map[string]any, error) {
	// 在 Basis Points 虚拟记录旁保留原生 Codex 记录，使既有 Codex 模型
	// 继续走 CPA 原生执行器，同时为 oai-basispoints 模型提供独立认证。
	label := c.Email
	if label == "" {
		label = fileName
	}
	planType := codexPlanType(raw, c.AccessToken)
	// CPA 原生执行器直接读取 Metadata，必须保留源凭据字段。
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, fail(400, "invalid_auth", "OAuth credential is not valid JSON")
	}
	metadata["type"] = AuthProviderID
	metadata["auth_kind"] = "oauth"
	metadata["access_token"] = c.AccessToken
	if firstString(metadata, "account_id") == "" {
		metadata["account_id"] = c.AccountID
	}
	attributes := map[string]string{
		"auth_kind":  "oauth",
		"account_id": c.AccountID,
		"auth_mode":  c.AuthMode,
	}
	if planType != "" {
		metadata["plan_type"] = planType
		attributes["plan_type"] = planType
	}
	if priority, ok := metadata["priority"].(float64); ok {
		attributes["priority"] = strconv.Itoa(int(priority))
	} else if priority := strings.TrimSpace(stringValue(metadata["priority"])); priority != "" {
		if _, err := strconv.Atoi(priority); err == nil {
			attributes["priority"] = priority
		}
	}
	if note := strings.TrimSpace(stringValue(metadata["note"])); note != "" {
		attributes["note"] = note
	}
	return map[string]any{
		"Provider":    AuthProviderID,
		"ID":          fileName,
		"FileName":    fileName,
		"Label":       label,
		"StorageJSON": raw,
		// 与 CPA 原生加载 codex 文件一致：保留按凭据设置的出口。
		"ProxyURL":   c.ProxyURL,
		"Metadata":   metadata,
		"Attributes": attributes,
	}, nil
}

func codexPlanType(raw []byte, accessToken string) string {
	var root map[string]any
	if json.Unmarshal(raw, &root) == nil {
		if planType := firstString(root, "plan_type", "planType"); planType != "" {
			return planType
		}
	}
	// 与 CPA 原生解析一致，优先读取 id_token 套餐。
	idClaims := jwtPayload(stringValue(root["id_token"]))
	if auth, ok := idClaims["https://api.openai.com/auth"].(map[string]any); ok {
		if planType := firstString(auth, "chatgpt_plan_type", "plan_type"); planType != "" {
			return planType
		}
	}
	claims := jwtPayload(accessToken)
	if auth, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		return firstString(auth, "chatgpt_plan_type", "plan_type")
	}
	return ""
}

func authParse(raw []byte) (map[string]any, error) {
	return authParseWithDedicated(raw, nil)
}

// authParseWithDedicated 只接管 dedicated 集合中的（「标记」）codex 文件，以共享模式
// 返回 native codex 与 oai-basispoints 两条记录，二者都不携带 refresh_token、都是
// runtime_only，使 CPA 无法轮换或写回该凭据（前提：CPA 未以 Home 模式运行，见 CHANGELOG）；
// 未标记的文件返回 Handled:false，交还 CPA 原生加载器。
func authParseWithDedicated(raw []byte, dedicated map[string]bool) (map[string]any, error) {
	var request authParseRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(request.Provider))
	if provider != "" && provider != "codex" && provider != Provider && provider != "openai" {
		return map[string]any{"Handled": false}, nil
	}
	fileName := strings.TrimSpace(request.FileName)
	if fileName == "" {
		fileName = filepath.Base(strings.TrimSpace(request.Path))
	}
	c, err := parseCredential(request.RawJSON)
	if err != nil {
		if provider == Provider {
			return nil, err
		}
		return map[string]any{"Handled": false}, nil
	}
	virtual := authData(request.RawJSON, fileName, c)
	if provider == Provider {
		return map[string]any{"Handled": true, "Auth": virtual}, nil
	}
	// 未标记的文件不接管：交还 CPA 原生加载器（单条 native 记录，CPA 自己刷新并写回）。
	// 插件一旦把文件展开成多条记录，CPA 会把它们都标为 plugin_virtual 并跳过 persist，
	// 原生刷新结果就只留在内存里、文件中的旧 refresh_token 随即作废（v0.1.10 的隐患）。
	// 因此 Basis Points 只提供给由外部刷新脚本独占刷新的「标记」凭据。
	// 专用匹配使用 CPA 传入的原始文件名（不做 TrimSpace）：名为 " excel.json " 的文件
	// 不得命中标记 "excel.json"。
	rawName := request.FileName
	if rawName == "" {
		rawName = request.Path
	}
	if !dedicated[authFileIdentity(rawName)] {
		return map[string]any{"Handled": false}, nil
	}
	// 标记文件：共享模式——同时返回 native codex 与 oai-basispoints 两条记录，原生模型照常
	// 可用；但两条记录都**不携带 refresh_token**（Metadata 与 StorageJSON 均剔除）：
	//   - CPA 原生 Codex 刷新只从 Metadata 读取 refresh_token，读不到即原样返回、不轮换；
	//   - 401 后的补救刷新也只在 Metadata 有 refresh_token 时触发；
	// 于是 CPA 永远不会轮换该凭据，外部刷新脚本是唯一刷新方。脚本原子改写文件后，
	// watcher 重新解析，两条记录同时拿到新的 access token。
	// 不写回：返回多条记录时，CPA（watcher/synthesizer 与 filestore）会把它们都标为
	// plugin_virtual，Manager.persist 对 plugin_virtual 直接跳过，两条记录都不会写回文件。
	// 只有虚拟记录额外标记 runtime_only；native 记录不标——CPAMP 等管理面板会把
	// runtime_only 凭据整体视为只读（隐藏额度刷新、重置额度等），而 native 记录需要这些
	// 操作。native 记录不带 refresh_token，额度查询只用 access_token，不会触发轮换。
	stripped := stripRefreshTokens(request.RawJSON)
	virtual = authData(stripped, fileName, c)
	native, err := nativeCodexAuthData(stripped, fileName, c)
	if err != nil {
		return nil, err
	}
	markRuntimeOnly(virtual)
	return map[string]any{
		"Handled": true,
		"Auths": []any{
			native,
			virtual,
		},
	}, nil
}

// refreshTokenKeys 是 CPA 识别的 refresh token 字段（authHasRefreshCredential 与
// CodexExecutor.Refresh 读取的键）。
var refreshTokenKeys = []string{"refresh_token", "refreshToken"}

// stripRefreshTokens 返回去掉顶层 refresh token 字段的凭据 JSON 副本；无法解析时原样返回
// （parseCredential 已在前面校验过 JSON）。
func stripRefreshTokens(raw []byte) []byte {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil || root == nil {
		return raw
	}
	for _, key := range refreshTokenKeys {
		delete(root, key)
	}
	out, err := json.Marshal(root)
	if err != nil {
		return raw
	}
	return out
}

func markRuntimeOnly(record map[string]any) {
	attributes, _ := record["Attributes"].(map[string]string)
	cloned := make(map[string]string, len(attributes)+1)
	for key, value := range attributes {
		cloned[key] = value
	}
	cloned["runtime_only"] = "true"
	record["Attributes"] = cloned
}

func authRefresh(raw []byte) (map[string]any, error) {
	var request authRefreshRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	c, err := parseCredential(request.StorageJSON)
	if err != nil {
		return nil, err
	}
	if !c.ExpiresAt.IsZero() && !time.Now().Before(c.ExpiresAt) {
		return nil, fail(401, "auth_expired", "ChatGPT OAuth access token has expired")
	}
	fileName := request.AuthID
	if fileName == "" {
		fileName = "chatgpt.json"
	}
	if !strings.HasSuffix(fileName, ".json") {
		fileName += ".json"
	}
	next := time.Now().Add(10 * time.Minute)
	if !c.ExpiresAt.IsZero() {
		next = c.ExpiresAt.Add(-2 * time.Minute)
		if next.Before(time.Now().Add(time.Minute)) {
			next = time.Now().Add(time.Minute)
		}
	}
	return map[string]any{
		"Auth":             authData(request.StorageJSON, fileName, c),
		"NextRefreshAfter": next.UTC(),
	}, nil
}

func credentialFromExecutor(request ExecutorRequest) (credential, error) {
	if len(request.StorageJSON) > 0 {
		c, err := parseCredential(request.StorageJSON)
		if err != nil {
			return c, err
		}
		// 按凭据出口必须有效，否则失败关闭：CPA 对无效值构造不出传输（RoundTripperFor 返回
		// nil），全局 proxy-url 为空时宿主 http 客户端会退回默认传输——直连或读环境代理，
		// 绕过指定出口。因此在任何上游调用（含附件上传）之前拒绝。
		if err := validateProxyValue(c.ProxyURL); err != nil {
			return c, fail(500, "invalid_proxy", "credential proxy_url is invalid; refusing to send upstream traffic that would bypass the configured egress")
		}
		return c, nil
	}
	metadata := map[string]any{}
	for key, value := range request.AuthMetadata {
		metadata[key] = value
	}
	for key, value := range request.AuthAttributes {
		metadata[key] = value
	}
	if token := stringValue(metadata["access_token"]); token != "" {
		data := map[string]any{"access_token": token}
		if accountID := stringValue(metadata["account_id"]); accountID != "" {
			data["account_id"] = accountID
		}
		return parseCredential(jsonBytes(data))
	}
	return credential{}, fail(401, "missing_auth", "CPA did not provide a ChatGPT OAuth credential")
}

// redactTokenMessage 抹掉错误串中所有 bearer 形态的令牌：HTTP 的 "Bearer <tok>" 与
// WS 子协议里的 "openai-bearer.<tok>"（依赖库在子协议协商失败时可能把它写进错误）。
// 每个前缀的所有出现都会被替换，而不仅是第一处。
func redactTokenMessage(message string) string {
	for _, prefix := range []string{"Bearer ", "bearer ", "openai-bearer."} {
		var out strings.Builder
		rest := message
		for {
			index := strings.Index(rest, prefix)
			if index < 0 {
				out.WriteString(rest)
				break
			}
			end := index + len(prefix)
			for end < len(rest) && !strings.ContainsRune(" \t\r\n\"',;", rune(rest[end])) {
				end++
			}
			out.WriteString(rest[:index])
			out.WriteString(prefix)
			out.WriteString("[REDACTED]")
			rest = rest[end:]
		}
		message = out.String()
	}
	return message
}

// redactSecret 先按当前令牌精确脱敏，再做前缀形态脱敏。
func redactSecret(message, secret string) string {
	if secret != "" {
		message = strings.ReplaceAll(message, secret, "[REDACTED]")
	}
	return redactTokenMessage(message)
}
