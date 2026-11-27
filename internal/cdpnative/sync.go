package cdpnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
)

const defaultCookieSyncTimeout = 60 * time.Second

// CookieSyncOptions 配置通过外部 CDP 浏览器拉取最新 Cookie 的参数
type CookieSyncOptions struct {
	Endpoint   string
	Headers    http.Header
	Origin     string
	AuthUser   int
	Timeout    time.Duration
	HTTPClient *http.Client
}

// FetchCookies 连接外部 CDP 浏览器，新建临时标签页访问 AI Studio 触发静默会话刷新，
// 校验落点未跳回登录页或拒绝页后导出 StorageState 格式的 Cookie JSON，并在返回前关闭临时标签页。
func FetchCookies(ctx context.Context, options CookieSyncOptions) ([]byte, error) {
	endpoint := strings.TrimSpace(options.Endpoint)
	if endpoint == "" {
		return nil, errors.New("外部 CDP Cookie 同步缺少 CDP_ENDPOINT 或账户 cdp_endpoint 配置")
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultCookieSyncTimeout
	}
	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	wsURL, err := ResolveWebSocketURL(syncCtx, endpoint, options.Headers, options.HTTPClient)
	if err != nil {
		return nil, err
	}
	client, err := dialCDP(syncCtx, wsURL, options.Headers)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.close() }()

	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := client.call(syncCtx, "", "Target.createTarget", map[string]any{"url": "about:blank"}, &created); err != nil {
		return nil, fmt.Errorf("创建外部浏览器临时标签页: %w", err)
	}
	if created.TargetID == "" {
		return nil, errors.New("外部 CDP Target.createTarget 未返回有效 targetId")
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer closeCancel()
		_ = client.call(closeCtx, "", "Target.closeTarget", map[string]any{"targetId": created.TargetID}, nil)
	}()

	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := client.call(syncCtx, "", "Target.attachToTarget", map[string]any{
		"targetId": created.TargetID,
		"flatten":  true,
	}, &attached); err != nil {
		return nil, fmt.Errorf("附加外部浏览器临时标签页: %w", err)
	}
	if attached.SessionID == "" {
		return nil, errors.New("外部 CDP Target.attachToTarget 未返回有效 sessionId")
	}
	sessionID := attached.SessionID

	if err := client.call(syncCtx, sessionID, "Page.enable", nil, nil); err != nil {
		return nil, fmt.Errorf("启用外部浏览器 Page 域: %w", err)
	}
	if err := client.call(syncCtx, sessionID, "Runtime.enable", nil, nil); err != nil {
		return nil, fmt.Errorf("启用外部浏览器 Runtime 域: %w", err)
	}

	origin := strings.TrimRight(strings.TrimSpace(options.Origin), "/")
	if origin == "" {
		origin = defaultAIStudioOrigin
	}
	pagePath := "/prompts/new_chat"
	if options.AuthUser > 0 {
		pagePath = fmt.Sprintf("/u/%d/prompts/new_chat", options.AuthUser)
	}
	targetURL := origin + pagePath + "?temporary=true"

	originHost := defaultAIStudioOrigin
	if parsed, parseErr := url.Parse(origin); parseErr == nil && parsed.Host != "" {
		originHost = parsed.Host
	}
	client.startLoad(sessionID, originHost, defaultMakerSuiteHost)
	defer client.endLoad(sessionID)

	var navResp struct {
		ErrorText string `json:"errorText"`
	}
	if err := client.call(syncCtx, sessionID, "Page.navigate", map[string]any{"url": targetURL}, &navResp); err != nil {
		return nil, fmt.Errorf("导航 AI Studio: %w", err)
	}
	if navResp.ErrorText != "" && !strings.Contains(navResp.ErrorText, "ERR_ABORTED") {
		return nil, fmt.Errorf("导航 AI Studio 失败: %s", navResp.ErrorText)
	}

	if err := client.waitFor(syncCtx, sessionID, workerPageReadyExpression, timeout); err != nil {
		var denial *camoufoxnative.AccessDenial
		if errors.As(err, &denial) {
			return nil, err
		}
		pageURL, _ := client.evaluateString(syncCtx, sessionID, "location.href")
		return nil, fmt.Errorf("等待 AI Studio 页面就绪失败 url=%s: %w", pageURL, err)
	}

	pageURL, err := client.evaluateString(syncCtx, sessionID, "location.href")
	if err != nil {
		return nil, err
	}
	if location, parseErr := url.Parse(pageURL); parseErr == nil {
		if denial := camoufoxnative.PageDenial(location); denial != nil {
			return nil, denial
		}
		if location.Hostname() == "accounts.google.com" {
			return nil, fmt.Errorf("%w url=%s", camoufoxnative.ErrAuthenticationRequired, pageURL)
		}
	}

	cookies, err := client.getStorageCookies(syncCtx, sessionID)
	if err != nil {
		return nil, err
	}
	if !hasGoogleAuthCookies(cookies, time.Now()) {
		return nil, fmt.Errorf("%w: 外部浏览器缺少有效 Google 登录 Cookie", camoufoxnative.ErrAuthenticationRequired)
	}
	sort.SliceStable(cookies, func(left, right int) bool {
		if cookies[left].Domain != cookies[right].Domain {
			return cookies[left].Domain < cookies[right].Domain
		}
		if cookies[left].Name != cookies[right].Name {
			return cookies[left].Name < cookies[right].Name
		}
		return cookies[left].Path < cookies[right].Path
	})
	return json.Marshal(cookies)
}
