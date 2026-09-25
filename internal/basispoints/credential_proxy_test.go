package basispoints

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func proxiedStorage(proxy string) []byte {
	return jsonBytes(map[string]any{"type": "codex", "access_token": "test-access", "account_id": "test-account", "proxy_url": proxy})
}

// 按凭据设置的出口必须随记录交给 CPA：专用（仅虚拟）与未标记（native + virtual）两种情况都要带上。
func TestCredentialProxyURLFlowsIntoRecords(t *testing.T) {
	const proxy = "http://127.0.0.1:10811"
	cfg := defaultConfig()
	cfg.DedicatedAuthFiles = []string{"excel.json"}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	dedicated, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "excel.json", RawJSON: proxiedStorage(proxy)}), cfg.dedicatedSet())
	if err != nil {
		t.Fatal(err)
	}
	records := dedicated["Auths"].([]any)
	if len(records) != 1 || objectValue(records[0])["ProxyURL"] != proxy {
		t.Fatalf("dedicated virtual record must carry the credential proxy: %#v", records)
	}
	other, err := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "other.json", RawJSON: proxiedStorage(proxy)}), cfg.dedicatedSet())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range other["Auths"].([]any) {
		if objectValue(record)["ProxyURL"] != proxy {
			t.Fatalf("native and virtual records must both carry the credential proxy: %#v", record)
		}
	}
	// 未设置时为空串，由 CPA 退回全局设置。
	plain, _ := authParseWithDedicated(jsonBytes(authParseRequest{Provider: AuthProviderID, FileName: "excel.json", RawJSON: codexStorage()}), cfg.dedicatedSet())
	if objectValue(plain["Auths"].([]any)[0])["ProxyURL"] != "" {
		t.Fatal("absent proxy_url must stay empty")
	}
}

func TestWSProxyPrecedenceAndSchemes(t *testing.T) {
	cfg := defaultConfig()
	cfg.ProxyURL = "http://127.0.0.1:1"
	if got := wsProxyURL(cfg, credential{ProxyURL: "socks5://127.0.0.1:10811"}); got != "socks5://127.0.0.1:10811" {
		t.Fatalf("credential proxy must win, got %q", got)
	}
	if got := wsProxyURL(cfg, credential{}); got != "http://127.0.0.1:1" {
		t.Fatalf("plugin proxy is the fallback, got %q", got)
	}
	for _, ok := range []string{"", "direct", "none", "http://127.0.0.1:10811", "https://p:1", "socks5://127.0.0.1:10811"} {
		if _, err := proxiedHTTPClient(ok); err != nil {
			t.Fatalf("%q should be accepted: %v", ok, err)
		}
	}
	for _, bad := range []string{"ftp://x:1", "://bad", "http://"} {
		if _, err := proxiedHTTPClient(bad); !isKind(err, "invalid_config") {
			t.Fatalf("%q should be rejected, got %v", bad, err)
		}
	}
	direct, _ := proxiedHTTPClient("direct")
	if tr, _ := direct.Transport.(*http.Transport); tr == nil || tr.Proxy != nil {
		t.Fatal("direct must not consult any (environment) proxy")
	}
}

// ws 拨号真实经过凭据的出口：代理记录到请求，上游收不到直连握手。
func TestWSDialUsesCredentialProxy(t *testing.T) {
	var proxyHits, upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		http.Error(w, "should not be reached directly", http.StatusTeapot)
	}))
	defer upstream.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		http.Error(w, "proxy refuses", http.StatusBadGateway)
	}))
	defer proxy.Close()

	svc := newWSService(t, upstream)
	_, err := svc.streamOverWS(context.Background(), ExecutorRequest{StreamID: "px"}, wsBody(), credential{AccessToken: "tok", AccountID: "a", ProxyURL: proxy.URL})
	if err == nil {
		t.Fatal("dial through a refusing proxy must fail")
	}
	if proxyHits.Load() == 0 {
		t.Fatal("ws dial did not go through the credential proxy")
	}
	if upstreamHits.Load() != 0 {
		t.Fatal("ws dial bypassed the credential proxy and reached upstream directly")
	}
}
