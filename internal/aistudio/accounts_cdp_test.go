package aistudio

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestAccountCDPOverridesAndValidation(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, accountConfigName)
	raw := `{
		"label": "acct-a@example.com",
		"enabled": true,
		"proxy": "",
		"locale": "en-US",
		"timezone": "UTC",
		"cdp_endpoint": "http://127.0.0.1:9222/cdp/profile-a",
		"cdp_headers": {
			"Authorization": "Bearer account-token",
			"X-Remove": ""
		}
	}`
	if err := os.WriteFile(cfgPath, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := readAccountConfig(cfgPath)
	if err != nil {
		t.Fatalf("readAccountConfig 失败: %v", err)
	}
	account := &Account{ID: "acct-a@example.com", Config: cfg}

	if got := account.EffectiveCDPEndpoint("http://127.0.0.1:9222/cdp/global"); got != "http://127.0.0.1:9222/cdp/profile-a" {
		t.Fatalf("EffectiveCDPEndpoint = %q", got)
	}

	fallbackAccount := &Account{ID: "acct-b@example.com", Config: AccountConfig{Label: "acct-b@example.com", Locale: "en-US", Timezone: "UTC"}}
	if got := fallbackAccount.EffectiveCDPEndpoint("http://127.0.0.1:9222/cdp/global"); got != "http://127.0.0.1:9222/cdp/global" {
		t.Fatalf("回退全局 EffectiveCDPEndpoint = %q", got)
	}

	globalHeaders := http.Header{
		"Authorization": []string{"Bearer global-token"},
		"X-Global":      []string{"keep-me"},
		"X-Remove":      []string{"drop-me"},
	}
	merged := account.EffectiveCDPHeaders(globalHeaders)
	if merged.Get("Authorization") != "Bearer account-token" {
		t.Fatalf("账号级 Authorization 应覆盖全局: got %q", merged.Get("Authorization"))
	}
	if merged.Get("X-Global") != "keep-me" {
		t.Fatalf("未覆盖的全局 header 应保留: got %q", merged.Get("X-Global"))
	}
	if merged.Get("X-Remove") != "" {
		t.Fatalf("空值覆盖应删除对应 header: got %q", merged.Get("X-Remove"))
	}

	invalid := cfg
	invalid.CDPEndpoint = "ftp://127.0.0.1:9222"
	if err := invalid.Validate(); err == nil {
		t.Fatalf("无效 cdp_endpoint 应当校验失败")
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("序列化 AccountConfig 失败: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("序列化结果为空")
	}
}
