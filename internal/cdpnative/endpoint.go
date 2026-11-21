package cdpnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type versionResponse struct {
	Browser              string `json:"Browser"`
	WebSocketDebuggerURL string `json:"webSocketDebuggerUrl"`
}

// ResolveWebSocketURL 将 ws/wss 或 http/https（含反向代理路径前缀）调试地址解析为可直连的浏览器级 WebSocket 地址
func ResolveWebSocketURL(ctx context.Context, endpoint string, headers http.Header, httpClient *http.Client) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", errors.New("外部 CDP 地址不能为空")
	}
	lower := strings.ToLower(endpoint)
	if strings.HasPrefix(lower, "ws://") || strings.HasPrefix(lower, "wss://") {
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Host == "" {
			return "", fmt.Errorf("外部 CDP WebSocket 地址无效: %q", endpoint)
		}
		return parsed.String(), nil
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	baseURL, err := url.Parse(endpoint)
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return "", fmt.Errorf("外部 CDP 调试地址无效: %q", endpoint)
	}

	prefix := strings.TrimRight(baseURL.Path, "/")
	versionURL := *baseURL
	versionURL.Path = prefix + "/json/version"
	versionURL.RawPath = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, versionURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("构造 CDP /json/version 请求: %w", err)
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	client := httpClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("请求 CDP /json/version: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("读取 CDP /json/version 响应: %w", err)
	}
	var version versionResponse
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &version) != nil || strings.TrimSpace(version.WebSocketDebuggerURL) == "" {
		return "", fmt.Errorf("CDP /json/version 未返回有效 webSocketDebuggerUrl: HTTP %d", resp.StatusCode)
	}
	return rewriteDebuggerWSURL(baseURL, version.WebSocketDebuggerURL)
}

// rewriteDebuggerWSURL 基于原 HTTP(S) endpoint 的 scheme、host 与路径前缀重写 webSocketDebuggerUrl，兼容反向代理
func rewriteDebuggerWSURL(baseURL *url.URL, rawWS string) (string, error) {
	wsURL, err := url.Parse(strings.TrimSpace(rawWS))
	if err != nil {
		return "", fmt.Errorf("解析 webSocketDebuggerUrl 失败: %w", err)
	}
	targetScheme := "ws"
	if strings.EqualFold(baseURL.Scheme, "https") {
		targetScheme = "wss"
	}
	wsURL.Scheme = targetScheme
	wsURL.Host = baseURL.Host

	wsPath := wsURL.Path
	if wsPath == "" {
		wsPath = "/"
	} else if !strings.HasPrefix(wsPath, "/") {
		wsPath = "/" + wsPath
	}
	prefix := strings.TrimRight(baseURL.Path, "/")
	if prefix != "" && wsPath != prefix && !strings.HasPrefix(wsPath, prefix+"/") {
		wsPath = prefix + wsPath
	}
	wsURL.Path = wsPath
	wsURL.RawPath = ""
	return wsURL.String(), nil
}
