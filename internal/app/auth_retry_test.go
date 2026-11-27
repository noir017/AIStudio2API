package app

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/cdpnative"
)

// TestAuthRefreshKeepsAccountSource 验证没有 OAuth 续签材料的账户不可续签且认证状态原样保留，重新登录以新登录结果为准
func TestAuthRefreshKeepsAccountSource(t *testing.T) {
	directory := t.TempDir()
	storage := filepath.Join(directory, "storage-state.json")
	state := `{"cookies":[{"name":"SAPISID","value":"old","domain":".google.com","path":"/","secure":true,"sameSite":"Lax"}],"origins":[]}`
	if err := os.WriteFile(storage, []byte(state), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := aistudio.LoadStorageState(storage)
	if err != nil {
		t.Fatal(err)
	}
	account := &aistudio.Account{
		ID: "user@example.com", Directory: directory, StoragePath: storage,
		RuntimePath: filepath.Join(directory, "runtime-state.json"), StorageState: loaded,
		Config: aistudio.DefaultAccountConfig("user@example.com"),
	}
	pool := aistudio.NewAccountPool([]*aistudio.Account{account}, 1)
	lease, err := pool.AcquireAccount(t.Context(), account.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	var cookies []aistudio.StateCookie
	for _, name := range []string{"SAPISID", "__Secure-1PAPISID", "__Secure-3PAPISID"} {
		cookies = append(cookies, aistudio.StateCookie{Name: name, Value: "chrome", Domain: ".google.com", Path: "/", Expires: -1, Secure: true, SameSite: "Lax"})
	}
	refresher := &authRuntimeRefresher{
		pool: pool, requests: newRequestRegistry(t.Context()),
		importCurrent: func(_ context.Context, source aistudio.AuthSource, _ aistudio.ChromeOAuthMaterial, _ string) (aistudio.StorageState, error) {
			updated := aistudio.StorageState{Cookies: cookies}
			updated.SetAuthExtension(aistudio.AuthExtension{
				Source: source, OAuth: &aistudio.ChromeOAuthMaterial{GaiaID: "gaia", RefreshToken: "token"},
			})
			return updated, nil
		},
		reset: func(string) error { return nil },
	}
	ctx := aistudio.ContextWithAccountLease(t.Context(), lease)
	if refresher.Available(ctx) {
		t.Fatal("没有 OAuth 续签材料的账户被判定为可续签")
	}
	if err := refresher.Refresh(ctx, lease.AuthGeneration()); err == nil {
		t.Fatal("没有 OAuth 续签材料的账户续签成功")
	}
	if stored, err := os.ReadFile(storage); err != nil || string(stored) != state {
		t.Fatalf("续签失败后认证状态被改写: %s err=%v", stored, err)
	}

	chrome := aistudio.StorageState{Cookies: cookies}
	chrome.SetAuthExtension(aistudio.AuthExtension{
		Source: aistudio.AuthSource{Browser: "chrome", Email: account.ID},
		OAuth:  &aistudio.ChromeOAuthMaterial{GaiaID: "gaia", RefreshToken: "token"},
	})
	if err := lease.SaveStorageState(chrome); err != nil {
		t.Fatal(err)
	}
	login := aistudio.StorageState{Cookies: cookies}
	login.SetAuthExtension(aistudio.AuthExtension{Source: aistudio.AuthSource{Browser: "camoufox", Email: account.ID}})
	if err := replaceAccountState(lease, login, ""); err != nil {
		t.Fatal(err)
	}
	stored, err := aistudio.LoadStorageState(storage)
	if err != nil {
		t.Fatal(err)
	}
	if extension, _, err := stored.AuthExtension(); err != nil || extension.OAuth != nil || extension.Source.Browser != "camoufox" {
		t.Fatalf("重新登录后的认证扩展 %+v err=%v", extension, err)
	}
}

func TestAuthRefreshCDPFallbackAndCooldown(t *testing.T) {
	directory := t.TempDir()
	storage := filepath.Join(directory, "storage-state.json")
	initial := `{"cookies":[{"name":"SAPISID","value":"expired","domain":".google.com","path":"/","secure":true,"sameSite":"Lax"}],"origins":[]}`
	if err := os.WriteFile(storage, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := aistudio.LoadStorageState(storage)
	if err != nil {
		t.Fatal(err)
	}
	cfg := aistudio.DefaultAccountConfig("cdp@example.com")
	cfg.CDPEndpoint = "http://127.0.0.1:19877/cdp/profile-1"
	account := &aistudio.Account{
		ID: "cdp@example.com", Directory: directory, StoragePath: storage,
		RuntimePath: filepath.Join(directory, "runtime-state.json"), StorageState: loaded,
		Config: cfg,
	}
	pool := aistudio.NewAccountPool([]*aistudio.Account{account}, 1)
	lease, err := pool.AcquireAccount(t.Context(), account.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	var syncCalls int
	var gotEndpoint string
	var gotAuthHeader string
	var resetCalls int
	refresher := &authRuntimeRefresher{
		pool:        pool,
		requests:    newRequestRegistry(t.Context()),
		cdpHeaders:  http.Header{"Authorization": []string{"Bearer global-secret"}},
		cdpCooldown: 10 * time.Minute,
		now:         func() time.Time { return now },
		lastCDPSync: make(map[string]time.Time),
		refreshCDP: func(_ context.Context, opts cdpnative.CookieSyncOptions) ([]aistudio.StateCookie, error) {
			syncCalls++
			gotEndpoint = opts.Endpoint
			gotAuthHeader = opts.Headers.Get("Authorization")
			return []aistudio.StateCookie{
				{Name: "SAPISID", Value: "fresh-cdp", Domain: ".google.com", Path: "/", Expires: -1, Secure: true, SameSite: "Lax"},
				{Name: "__Secure-1PAPISID", Value: "fresh-1p", Domain: ".google.com", Path: "/", Expires: -1, Secure: true, SameSite: "Lax"},
				{Name: "__Secure-3PAPISID", Value: "fresh-3p", Domain: ".google.com", Path: "/", Expires: -1, Secure: true, SameSite: "Lax"},
			}, nil
		},
		reset: func(string) error {
			resetCalls++
			return nil
		},
		prepareWorker: func(*aistudio.Account, aistudio.AccountConfig) (*accountWorkerUpdate, error) {
			return nil, nil
		},
	}

	ctx := aistudio.ContextWithAccountLease(t.Context(), lease)
	if !refresher.Available(ctx) {
		t.Fatal("配置了 cdp_endpoint 的账户应可触发外部 CDP 续签")
	}
	if err := refresher.Refresh(ctx, lease.AuthGeneration()); err != nil {
		t.Fatalf("外部 CDP 续签失败: %v", err)
	}
	if syncCalls != 1 || resetCalls != 1 {
		t.Fatalf("期望 syncCalls=1 resetCalls=1，实际 %d %d", syncCalls, resetCalls)
	}
	if gotEndpoint != cfg.CDPEndpoint || gotAuthHeader != "Bearer global-secret" {
		t.Fatalf("CDP 参数传递异常: endpoint=%q auth=%q", gotEndpoint, gotAuthHeader)
	}
	stored, err := aistudio.LoadStorageState(storage)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Cookies) != 3 || stored.Cookies[0].Value != "fresh-cdp" {
		t.Fatalf("续签后的 Cookie 未正确保存: %+v", stored.Cookies)
	}

	// 冷却期内不可再次拉取
	if refresher.Available(ctx) {
		t.Fatal("冷却期内 Available 应返回 false")
	}
	if err := refresher.Refresh(ctx, lease.AuthGeneration()); err == nil {
		t.Fatal("冷却期内直接调用 Refresh 应返回冷却错误")
	}
	if syncCalls != 1 {
		t.Fatalf("冷却期内不应再次调用 refreshCDP，实际 %d", syncCalls)
	}

	// 冷却期结束后可再次触发，且失败时不改写 storage-state.json
	now = now.Add(11 * time.Minute)
	if !refresher.Available(ctx) {
		t.Fatal("冷却期过后 Available 应恢复为 true")
	}
	refresher.refreshCDP = func(context.Context, cdpnative.CookieSyncOptions) ([]aistudio.StateCookie, error) {
		syncCalls++
		return nil, errors.New("remote browser signed out")
	}
	if err := refresher.Refresh(ctx, lease.AuthGeneration()); err == nil {
		t.Fatal("外部 CDP 拉取失败时应返回错误")
	}
	if syncCalls != 2 {
		t.Fatalf("期望第二次触发 refreshCDP，实际 %d", syncCalls)
	}
	storedAfterFail, err := aistudio.LoadStorageState(storage)
	if err != nil {
		t.Fatal(err)
	}
	if len(storedAfterFail.Cookies) != 3 || storedAfterFail.Cookies[0].Value != "fresh-cdp" {
		t.Fatalf("失败后 storage-state.json 不应被破坏: %+v", storedAfterFail.Cookies)
	}
}
