package cdpnative

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
)

func TestFetchCookiesSuccessAndClosesTempTab(t *testing.T) {
	mock := newMockCDPServer(t, "/cdp/sync-profile")
	mock.cookies = []map[string]any{
		{"name": "SAPISID", "value": "fresh-sapisid", "domain": ".google.com", "path": "/", "expires": float64(time.Now().Add(24 * time.Hour).Unix()), "secure": true, "sameSite": "Lax"},
		{"name": "__Secure-1PAPISID", "value": "fresh-1p", "domain": ".google.com", "path": "/", "expires": float64(time.Now().Add(24 * time.Hour).Unix()), "secure": true, "sameSite": "Lax"},
	}

	headers := http.Header{"Authorization": []string{"Bearer test-token"}}
	raw, err := FetchCookies(context.Background(), CookieSyncOptions{
		Endpoint:   mock.endpoint(),
		Headers:    headers,
		Timeout:    10 * time.Second,
		HTTPClient: mock.server.Client(),
	})
	if err != nil {
		t.Fatalf("FetchCookies 失败: %v", err)
	}
	var cookies []storageCookie
	if err := json.Unmarshal(raw, &cookies); err != nil {
		t.Fatalf("解析返回 Cookie 失败: %v", err)
	}
	if len(cookies) != 2 {
		t.Fatalf("期望 2 条 Cookie，实际 %d", len(cookies))
	}
	mock.mu.Lock()
	created := len(mock.createdTargets)
	closed := len(mock.closedTargets)
	browserClosed := mock.browserClosed
	authHeader := mock.versionHeaders.Get("Authorization")
	mock.mu.Unlock()
	if created != 1 || closed != 1 {
		t.Fatalf("期望创建并关闭 1 个临时标签页，实际 created=%d closed=%d", created, closed)
	}
	if browserClosed {
		t.Fatal("FetchCookies 不应关闭外部浏览器进程")
	}
	if authHeader != "Bearer test-token" {
		t.Fatalf("未透传 CDP Authorization 请求头: %q", authHeader)
	}
}

func TestFetchCookiesDetectsAuthRedirect(t *testing.T) {
	mock := newMockCDPServer(t, "/cdp/sync-expired")
	mock.authRedirectURL = "https://accounts.google.com/ServiceLogin?continue=https://aistudio.google.com"

	_, err := FetchCookies(context.Background(), CookieSyncOptions{
		Endpoint:   mock.endpoint(),
		Timeout:    10 * time.Second,
		HTTPClient: mock.server.Client(),
	})
	if !errors.Is(err, camoufoxnative.ErrAuthenticationRequired) {
		t.Fatalf("期望 ErrAuthenticationRequired，实际: %v", err)
	}
	mock.mu.Lock()
	created := len(mock.createdTargets)
	closed := len(mock.closedTargets)
	mock.mu.Unlock()
	if created != 1 || closed != 1 {
		t.Fatalf("认证失败后仍应关闭临时标签页，实际 created=%d closed=%d", created, closed)
	}
}
