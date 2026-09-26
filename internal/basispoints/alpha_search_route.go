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
// 该模型在原生 codex 凭据中重新挑选，并经所选凭据的 proxy_url 转发。OAuth 凭据转发时
// CPA 不改写请求体里的 model，因此 TargetModel 只决定挑哪个凭据；已实测 ChatGPT 搜索
// 后端接受请求体中的 Basis Points 模型名（2026-09-26）。
//
// 账号归属：CPA 的路由契约无法指定凭据，只能按 provider + 模型 + 凭据策略挑选。只有当
// CPA 中能提供该模型的原生 codex 凭据恰好就是 Basis Points 凭据的同一账号时，搜索才
// 一定由同一账号执行；存在多个原生 codex 凭据（或获准 alpha search 的 codex API key）
// 时，搜索可能由其他凭据执行并消耗其额度——与原生模型自己的网页搜索相同。
//
// 目标可用性：插件看不到 CPA 的模型注册表与冷却状态，只能确认有 codex 提供方；目标
// 模型不可用时，CPA 的挑选失败，结果与未改道时相同（503）。
//
// 注意：声明 model_router 后，CPA 处理每个请求（含普通 /v1/responses）都会先调用
// model.route。因此只在配置了 alpha_search_model 时才声明该能力；判定只做字符串比较，
// 不解析请求体；解析失败或 panic 都返回「不处理」，绝不返回错误。

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
	if cfg.AlphaSearchModel == "" || !isUnprefixedBasisPointsModel(request.RequestedModel, cfg) || !containsFold(request.AvailableProviders, "codex") {
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

// stripThinkingSuffix 去掉 CPA 允许附加的思考档位后缀（如 "(xhigh)"），与 CPA 的解析
// 规则一致：模型名以 ")" 结尾时，从最后一个 "(" 截断。
func stripThinkingSuffix(model string) string {
	model = strings.TrimSpace(model)
	if strings.HasSuffix(model, ")") {
		if open := strings.LastIndex(model, "("); open > 0 {
			model = strings.TrimSpace(model[:open])
		}
	}
	return model
}

// isBasisPointsModel 判断模型名是否由本插件提供（别名，含凭据前缀形式）。
func isBasisPointsModel(model string, cfg Config) bool {
	model = stripThinkingSuffix(model)
	if model == "" {
		return false
	}
	_, ok := catalogCanonicalSlug(model, cfg)
	return ok
}

// isUnprefixedBasisPointsModel 只接受不带凭据前缀的本插件模型。带前缀的请求不改道：
// 固定的 TargetModel 会丢掉前缀而选错或选不到凭据，且 OAuth 转发会原样保留请求体中的
// 前缀，上游是否接受未经验证；保持 CPA 原有行为。
func isUnprefixedBasisPointsModel(model string, cfg Config) bool {
	model = stripThinkingSuffix(model)
	if model == "" || strings.Contains(model, "/") {
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
