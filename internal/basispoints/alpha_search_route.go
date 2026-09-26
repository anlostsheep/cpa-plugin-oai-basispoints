package basispoints

import (
	"encoding/json"
	"strings"
)

// Codex 的网页搜索（codex-tui 在 exec 中调用 tools.web__run）由客户端单独请求
// /v1/alpha/search，请求里的 model 是当前会话模型。CPA 7.3.17 只从原生 codex 凭据中按
// 模型挑选（server_routes.go codexAlphaSearch），Basis Points 模型只挂在本插件的虚拟
// 记录上，于是返回 auth_not_found（503），sub2api 随即冷却「账号 + 该模型」。
//
// CPA 在挑选凭据前会询问模型路由插件：返回 provider=codex 与一个原生模型名，CPA 就用
// 该模型挑中同一账号的原生记录，并经其 proxy_url 转发。OAuth 凭据转发时 CPA 不改写
// 请求体里的 model，因此 TargetModel 只决定挑哪个凭据；已实测 ChatGPT 搜索后端接受
// 请求体中的 Basis Points 模型名（2026-09-26）。
//
// 注意：声明 model_router 后，CPA 处理每个请求（含普通 /v1/responses）都会先调用
// model.route。因此只在配置了 alpha_search_model 时才声明该能力；判定只做字符串比较，
// 不解析请求体；任何异常都返回「不处理」，绝不返回错误。

// alphaSearchSourceFormat 是 CPA 7.3.17 alpha search 路由请求的 SourceFormat。
const alphaSearchSourceFormat = "codex-alpha-search"

// modelRouteRequest 只取判定所需的字段；其余字段（含 Body）不解码。
type modelRouteRequest struct {
	SourceFormat       string   `json:"SourceFormat"`
	RequestedModel     string   `json:"RequestedModel"`
	AvailableProviders []string `json:"AvailableProviders"`
}

func modelNotRouted() map[string]any {
	return map[string]any{"Handled": false}
}

// routeModel 处理 model.route：只把 Basis Points 模型的 alpha search 改道到原生 codex。
func (s *Service) routeModel(raw json.RawMessage) (result any) {
	defer func() {
		if recover() != nil {
			result = modelNotRouted()
		}
	}()
	var request modelRouteRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return modelNotRouted()
	}
	if !strings.EqualFold(strings.TrimSpace(request.SourceFormat), alphaSearchSourceFormat) {
		return modelNotRouted()
	}
	cfg := s.config()
	if cfg.AlphaSearchModel == "" || !isBasisPointsModel(request.RequestedModel, cfg) || !containsFold(request.AvailableProviders, "codex") {
		return modelNotRouted()
	}
	return map[string]any{
		"Handled":     true,
		"TargetKind":  "provider",
		"Target":      "codex",
		"TargetModel": cfg.AlphaSearchModel,
		"Reason":      "basis_points_alpha_search_via_native_codex",
	}
}

// isBasisPointsModel 判断客户端请求的模型是否由本插件提供：去掉 CPA 允许附加的
// 思考档位后缀（如 "(xhigh)"），再按别名与凭据前缀匹配。
func isBasisPointsModel(model string, cfg Config) bool {
	model = strings.TrimSpace(model)
	if strings.HasSuffix(model, ")") {
		model, _, _ = strings.Cut(model, "(")
		model = strings.TrimSpace(model)
	}
	if model == "" {
		return false
	}
	_, ok := catalogCanonicalSlug(model, cfg)
	return ok
}

func containsFold(values []string, want string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), want) {
			return true
		}
	}
	return false
}
