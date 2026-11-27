package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAABackendCDPAndEnvRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	raw := "WAA_BACKEND=cdp\nCDP_ENDPOINT=http://127.0.0.1:9222/cdp/profile-a\nCDP_HEADERS='{\"Authorization\":\"Bearer example-token\",\"X-Profile\":\"profile-a\"}'\n"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if len(cfg.Invalid) != 0 {
		t.Fatalf("存在无效配置项: %+v", cfg.Invalid)
	}
	if cfg.WAABackend != WAABackendCDP {
		t.Fatalf("WAABackend = %q, 期望 %q", cfg.WAABackend, WAABackendCDP)
	}
	if cfg.CDPEndpoint != "http://127.0.0.1:9222/cdp/profile-a" {
		t.Fatalf("CDPEndpoint = %q", cfg.CDPEndpoint)
	}

	headers, err := ParseCDPHeaders(cfg.CDPHeaders)
	if err != nil {
		t.Fatalf("ParseCDPHeaders 失败: %v", err)
	}
	if got := headers.Get("Authorization"); got != "Bearer example-token" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := headers.Get("X-Profile"); got != "profile-a" {
		t.Fatalf("X-Profile = %q", got)
	}

	if err := cfg.Save(path); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}
	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("重新 Load 失败: %v", err)
	}
	if reloaded.WAABackend != WAABackendCDP || reloaded.CDPEndpoint != cfg.CDPEndpoint || reloaded.CDPHeaders != cfg.CDPHeaders {
		t.Fatalf("Save/Load 往返不一致: got=%+v want=%+v", reloaded, cfg)
	}
}

func TestValidateCDPEndpointAndParseCDPHeaders(t *testing.T) {
	validEndpoints := []string{
		"",
		"http://127.0.0.1:9222",
		"http://127.0.0.1:9222/cdp/profile-a",
		"https://cdp.example.com/proxy/profile-b",
		"ws://127.0.0.1:9222/devtools/browser/test-id",
		"wss://cdp.example.com/cdp/profile-a/devtools/browser/test-id",
	}
	for _, ep := range validEndpoints {
		if err := ValidateCDPEndpoint(ep); err != nil {
			t.Errorf("ValidateCDPEndpoint(%q) 不应失败: %v", ep, err)
		}
	}

	invalidEndpoints := []string{
		"ftp://127.0.0.1:9222",
		"127.0.0.1:9222",
		"http://",
		"://broken",
	}
	for _, ep := range invalidEndpoints {
		if err := ValidateCDPEndpoint(ep); err == nil {
			t.Errorf("ValidateCDPEndpoint(%q) 应当失败", ep)
		}
	}

	headers, err := ParseCDPHeaders("Authorization: Bearer example-token\nX-Tenant: profile-a\n")
	if err != nil {
		t.Fatalf("解析多行 Key: Value 失败: %v", err)
	}
	if headers.Get("Authorization") != "Bearer example-token" || headers.Get("X-Tenant") != "profile-a" {
		t.Fatalf("解析结果不符合预期: %+v", headers)
	}

	if _, err := ParseCDPHeaders(`{"Authorization": 123}`); err == nil {
		t.Fatalf("非字符串 JSON 值应当报错")
	}
	if _, err := ParseCDPHeaders("invalid-header-line"); err == nil {
		t.Fatalf("缺少冒号的 header 行应当报错")
	}
}
