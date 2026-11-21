package cdpnative

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/gorilla/websocket"
)

type mockCDPServer struct {
	t      *testing.T
	server *httptest.Server

	mu                    sync.Mutex
	prefix                string
	versionWSURL          string
	versionHeaders        http.Header
	wsHandshakeHeaders    http.Header
	nextTargetSeq         int
	existingTargetID      string
	createdTargets        []string
	closedTargets         []string
	browserClosed         bool
	cookies               []map[string]any
	setCookiesCalls       int
	denialURL             string
	authRedirectURL       string
	proofNullAttempts     int
	proofCallCount        int
	proofValue            string
	protectedChunks       []string
	protectedHeadError    string
	protectedMidError     string
	protectedWaitForClose bool
	grantCount            int
	cancelCount           int
	pauseKeyEvents        []string
	navigations           map[string]int
}

func newMockCDPServer(t *testing.T, prefix string) *mockCDPServer {
	t.Helper()
	mock := &mockCDPServer{
		t:                t,
		prefix:           prefix,
		existingTargetID: "pre-existing-tab",
		proofValue:       "!mock-waa-proof-token",
		protectedChunks:  []string{"chunk-1", "chunk-2"},
		navigations:      make(map[string]int),
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	versionPath := strings.TrimRight(prefix, "/") + "/json/version"
	wsPath := strings.TrimRight(prefix, "/") + "/devtools/browser/mock-browser-id"

	mux.HandleFunc(versionPath, func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		mock.versionHeaders = r.Header.Clone()
		wsURL := mock.versionWSURL
		mock.mu.Unlock()
		if wsURL == "" {
			// 默认模拟上游 Chrome 返回 127.0.0.1 且缺失代理前缀的情况
			wsURL = "ws://127.0.0.1:9222/devtools/browser/mock-browser-id"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"Browser":              "Chrome/130.0.0.0",
			"webSocketDebuggerUrl": wsURL,
		})
	})

	mux.HandleFunc(wsPath, func(w http.ResponseWriter, r *http.Request) {
		mock.mu.Lock()
		mock.wsHandshakeHeaders = r.Header.Clone()
		mock.mu.Unlock()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		mock.serveConn(conn)
	})

	mock.server = httptest.NewServer(mux)
	t.Cleanup(mock.server.Close)
	return mock
}

func (m *mockCDPServer) endpoint() string {
	return m.server.URL + m.prefix
}

func (m *mockCDPServer) serveConn(conn *websocket.Conn) {
	defer conn.Close()
	var writeMu sync.Mutex
	sendJSON := func(v any) {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.WriteJSON(v)
	}
	sendResult := func(id int, sessionID string, result any) {
		msg := map[string]any{"id": id, "result": result}
		if sessionID != "" {
			msg["sessionId"] = sessionID
		}
		sendJSON(msg)
	}
	sendEvent := func(sessionID, method string, params any) {
		msg := map[string]any{"method": method, "params": params}
		if sessionID != "" {
			msg["sessionId"] = sessionID
		}
		sendJSON(msg)
	}

	var (
		stateMu       sync.Mutex
		bindings      = map[string]string{} // sessionID -> waaReadyBinding
		activeStreams = map[string]chan struct{}{}
	)

	for {
		var req struct {
			ID        int            `json:"id"`
			SessionID string         `json:"sessionId"`
			Method    string         `json:"method"`
			Params    map[string]any `json:"params"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		switch req.Method {
		case "Browser.close":
			m.mu.Lock()
			m.browserClosed = true
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Target.createTarget":
			m.mu.Lock()
			m.nextTargetSeq++
			targetID := fmt.Sprintf("target-%d", m.nextTargetSeq)
			m.createdTargets = append(m.createdTargets, targetID)
			m.mu.Unlock()
			sendResult(req.ID, "", map[string]any{"targetId": targetID})

		case "Target.attachToTarget":
			targetID, _ := req.Params["targetId"].(string)
			sessionID := "session-" + targetID
			sendResult(req.ID, "", map[string]any{"sessionId": sessionID})

		case "Target.closeTarget":
			targetID, _ := req.Params["targetId"].(string)
			m.mu.Lock()
			m.closedTargets = append(m.closedTargets, targetID)
			m.mu.Unlock()
			sendResult(req.ID, "", map[string]any{"success": true})

		case "Page.enable", "Runtime.enable", "Network.enable", "Page.removeScriptToEvaluateOnNewDocument", "Runtime.removeBinding":
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Page.addScriptToEvaluateOnNewDocument":
			sendResult(req.ID, req.SessionID, map[string]any{"identifier": "preload-1"})

		case "Runtime.addBinding":
			name, _ := req.Params["name"].(string)
			if strings.HasPrefix(name, "__cdp_waa_ready_") {
				stateMu.Lock()
				bindings[req.SessionID] = name
				stateMu.Unlock()
			}
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Network.setCookies":
			rawCookies, _ := req.Params["cookies"].([]any)
			m.mu.Lock()
			m.setCookiesCalls++
			for _, item := range rawCookies {
				if cookieMap, ok := item.(map[string]any); ok {
					// 合并同名同域同路径同分区的 cookie
					replaced := false
					for idx, existing := range m.cookies {
						if existing["name"] == cookieMap["name"] &&
							existing["domain"] == cookieMap["domain"] &&
							existing["path"] == cookieMap["path"] &&
							decodePartitionKey(existing["partitionKey"]) == decodePartitionKey(cookieMap["partitionKey"]) {
							m.cookies[idx] = cookieMap
							replaced = true
							break
						}
					}
					if !replaced {
						m.cookies = append(m.cookies, cookieMap)
					}
				}
			}
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Storage.getCookies":
			m.mu.Lock()
			copied := make([]map[string]any, len(m.cookies))
			copy(copied, m.cookies)
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{"cookies": copied})

		case "Page.navigate":
			m.mu.Lock()
			m.navigations[req.SessionID]++
			denialURL := m.denialURL
			authURL := m.authRedirectURL
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{"frameId": "main-frame"})

			sessionID := req.SessionID
			go func() {
				if denialURL != "" {
					sendEvent(sessionID, "Page.frameNavigated", map[string]any{
						"frame": map[string]any{"id": "main-frame", "url": denialURL},
					})
					return
				}
				if authURL != "" {
					sendEvent(sessionID, "Page.frameNavigated", map[string]any{
						"frame": map[string]any{"id": "main-frame", "url": authURL},
					})
					return
				}
				// 1) 前端主程序 bundle
				sendEvent(sessionID, "Network.responseReceived", map[string]any{
					"requestId": "req-bundle",
					"response": map[string]any{
						"url": "https://www.gstatic.com/_/mss/boq-makersuite/_/js/k=test/m=_b",
					},
				})
				// 2) 目录 RPC 与公共头
				rpcs := []struct {
					id   string
					name string
				}{
					{"req-logging", "GetLoggingContext"},
					{"req-tier", "GetAiStudioBenefitTier"},
					{"req-models", "ListModels"},
				}
				for _, rpc := range rpcs {
					sendEvent(sessionID, "Network.requestWillBeSent", map[string]any{
						"requestId": rpc.id,
						"request": map[string]any{
							"url":    "https://alkalimakersuite-pa.clients6.google.com/$rpc/google.internal.alkali.applications.makersuite.v1.MakerSuiteService/" + rpc.name,
							"method": "POST",
							"headers": map[string]any{
								"X-Goog-Api-Key":      "example-api-key",
								"X-Goog-AuthUser":     "0",
								"X-User-Agent":        "grpc-web-javascript/0.1",
								"X-AIStudio-Visit-Id": "visit-123",
							},
						},
					})
					sendEvent(sessionID, "Network.requestWillBeSentExtraInfo", map[string]any{
						"requestId": rpc.id,
						"headers": map[string]any{
							"User-Agent": "Mozilla/5.0 MockChrome/130.0",
						},
					})
					sendEvent(sessionID, "Network.loadingFinished", map[string]any{
						"requestId": rpc.id,
					})
				}
				// 3) WAA service 就绪通知
				stateMu.Lock()
				waaBinding := bindings[sessionID]
				stateMu.Unlock()
				if waaBinding != "" {
					sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
						"name":    waaBinding,
						"payload": "ready",
					})
				}
				// 4) WAA /generate_204 图片请求完成
				sendEvent(sessionID, "Network.requestWillBeSent", map[string]any{
					"requestId": "req-img",
					"request": map[string]any{
						"url":    "https://aistudio.google.com/generate_204",
						"method": "GET",
					},
				})
				sendEvent(sessionID, "Network.loadingFinished", map[string]any{
					"requestId": "req-img",
				})
			}()

		case "Network.getResponseBody":
			reqID, _ := req.Params["requestId"].(string)
			switch reqID {
			case "req-logging":
				sendResult(req.ID, req.SessionID, map[string]any{
					"body":          `[1,"logging-context"]`,
					"base64Encoded": false,
				})
			case "req-tier":
				sendResult(req.ID, req.SessionID, map[string]any{
					"body":          base64.StdEncoding.EncodeToString([]byte(`["free-tier"]`)),
					"base64Encoded": true,
				})
			case "req-models":
				sendResult(req.ID, req.SessionID, map[string]any{
					"body":          `[["models/gemini-2.5-flash"]]`,
					"base64Encoded": false,
				})
			default:
				sendResult(req.ID, req.SessionID, map[string]any{"body": "", "base64Encoded": false})
			}

		case "Runtime.evaluate":
			expr, _ := req.Params["expression"].(string)
			switch {
			case expr == "location.href":
				m.mu.Lock()
				authURL := m.authRedirectURL
				m.mu.Unlock()
				href := "https://aistudio.google.com/prompts/new_chat?model=gemini-2.5-flash&temporary=true"
				if authURL != "" {
					href = authURL
				}
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "string", "value": href},
				})
			case expr == "navigator.userAgent":
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "string", "value": "Mozilla/5.0 MockChrome/130.0"},
				})
			case strings.Contains(expr, "ms-prompt-box textarea") || strings.Contains(expr, "GetLoggingContext"):
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "boolean", "value": true},
				})
			case strings.Contains(expr, "ms-g1-welcome-dialog"):
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "number", "value": 0},
				})
			case strings.Contains(expr, "default_MakerSuite"):
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "object", "objectId": "page-obj-" + req.SessionID},
				})
			default:
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "undefined"},
				})
			}

		case "Runtime.callFunctionOn":
			fn, _ := req.Params["functionDeclaration"].(string)
			args, _ := req.Params["arguments"].([]any)
			switch {
			case strings.Contains(fn, "this.key"):
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "string", "value": "snapshotFn"},
				})
			case strings.Contains(fn, "page.snapshot"):
				m.mu.Lock()
				m.proofCallCount++
				nullRemaining := m.proofNullAttempts
				if m.proofNullAttempts > 0 {
					m.proofNullAttempts--
				}
				proofVal := m.proofValue
				m.mu.Unlock()
				if nullRemaining > 0 {
					sendResult(req.ID, req.SessionID, map[string]any{
						"result": map[string]any{"type": "object", "subtype": "null"},
					})
					continue
				}
				payload, _ := json.Marshal(map[string]any{
					"proof":   proofVal,
					"refresh": 1.5,
					"compute": 2.5,
				})
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "string", "value": string(payload)},
				})
			case strings.Contains(fn, "new XMLHttpRequest"):
				reqID := argString(args, 0)
				bindingName := argString(args, 5)
				cancelCh := make(chan struct{})
				stateMu.Lock()
				activeStreams[reqID] = cancelCh
				stateMu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "undefined"},
				})
				sessionID := req.SessionID
				go func() {
					m.mu.Lock()
					headErr := m.protectedHeadError
					midErr := m.protectedMidError
					chunks := slices.Clone(m.protectedChunks)
					waitForClose := m.protectedWaitForClose
					m.mu.Unlock()

					if headErr != "" {
						sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
							"name":    bindingName,
							"payload": fmt.Sprintf(`{"error":%q}`, headErr),
						})
						return
					}
					sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
						"name":    bindingName,
						"payload": `{"status":200,"headers":[["Content-Type","application/json+protobuf"],["X-Mock","yes"]]}`,
					})
					for _, chunk := range chunks {
						select {
						case <-cancelCh:
							return
						default:
						}
						sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
							"name":    bindingName,
							"payload": base64.StdEncoding.EncodeToString([]byte(chunk)),
						})
					}
					if midErr != "" {
						sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
							"name":    bindingName,
							"payload": fmt.Sprintf(`{"error":%q}`, midErr),
						})
						return
					}
					if waitForClose {
						<-cancelCh
						return
					}
					sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
						"name":    bindingName,
						"payload": `{"done":true}`,
					})
				}()
			case strings.Contains(fn, "request.credit +="):
				m.mu.Lock()
				m.grantCount++
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "undefined"},
				})
			case strings.Contains(fn, "request?.xhr?.abort()"):
				reqID := argString(args, 0)
				m.mu.Lock()
				m.cancelCount++
				m.mu.Unlock()
				stateMu.Lock()
				if ch, ok := activeStreams[reqID]; ok {
					delete(activeStreams, reqID)
					close(ch)
				}
				stateMu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "undefined"},
				})
			default:
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "undefined"},
				})
			}

		case "Input.dispatchKeyEvent":
			evType, _ := req.Params["type"].(string)
			key, _ := req.Params["key"].(string)
			m.mu.Lock()
			m.pauseKeyEvents = append(m.pauseKeyEvents, evType+":"+key)
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{})

		default:
			sendResult(req.ID, req.SessionID, map[string]any{})
		}
	}
}

func argString(args []any, idx int) string {
	if idx >= len(args) {
		return ""
	}
	item, _ := args[idx].(map[string]any)
	val, _ := item["value"].(string)
	return val
}

func TestResolveWebSocketURLAndProxyRewrite(t *testing.T) {
	cases := []struct {
		name     string
		base     string
		rawWS    string
		expected string
	}{
		{
			name:     "http_with_prefix_and_loopback_ws",
			base:     "http://127.0.0.1:9222/cdp/profile-a",
			rawWS:    "ws://127.0.0.1:9222/devtools/browser/abc",
			expected: "ws://127.0.0.1:9222/cdp/profile-a/devtools/browser/abc",
		},
		{
			name:     "https_proxy_with_prefix_and_loopback_ws",
			base:     "https://cdp.example.com/cdp/profile-a/",
			rawWS:    "ws://127.0.0.1:9222/devtools/browser/abc",
			expected: "wss://cdp.example.com/cdp/profile-a/devtools/browser/abc",
		},
		{
			name:     "proxy_already_rewrote_prefix_no_duplication",
			base:     "https://cdp.example.com/cdp/profile-a",
			rawWS:    "wss://cdp.example.com/cdp/profile-a/devtools/browser/abc",
			expected: "wss://cdp.example.com/cdp/profile-a/devtools/browser/abc",
		},
		{
			name:     "proxy_root_different_port",
			base:     "http://127.0.0.1:19877",
			rawWS:    "ws://127.0.0.1:9222/devtools/browser/abc",
			expected: "ws://127.0.0.1:19877/devtools/browser/abc",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsedBase, err := url.Parse(tc.base)
			if err != nil {
				t.Fatal(err)
			}
			got, err := rewriteDebuggerWSURL(parsedBase, tc.rawWS)
			if err != nil {
				t.Fatalf("rewriteDebuggerWSURL 失败: %v", err)
			}
			if got != tc.expected {
				t.Fatalf("got %q, want %q", got, tc.expected)
			}
		})
	}

	direct, err := ResolveWebSocketURL(context.Background(), "wss://cdp.example.com/cdp/profile-a/devtools/browser/direct", nil, nil)
	if err != nil || direct != "wss://cdp.example.com/cdp/profile-a/devtools/browser/direct" {
		t.Fatalf("直连 ws URL 解析失败: got=%q err=%v", direct, err)
	}
}

func TestWorkerBootstrapHeadersCatalogCookiesProofAndClose(t *testing.T) {
	mock := newMockCDPServer(t, "/cdp/profile-a")

	storagePath := filepath.Join(t.TempDir(), "storage-state.json")
	storageJSON := `{
		"cookies": [
			{"name":"SAPISID","value":"v1","domain":".google.com","path":"/","expires":1893456000,"httpOnly":false,"secure":true,"sameSite":"None"},
			{"name":"PART","value":"p1","domain":".google.com","path":"/","expires":1893456000,"httpOnly":true,"secure":true,"sameSite":"Lax","partitionKey":"https://aistudio.google.com"}
		],
		"origins": [{"origin":"https://aistudio.google.com","localStorage":[{"name":"theme","value":"dark"}]}]
	}`
	if err := os.WriteFile(storagePath, []byte(storageJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	customHeaders := http.Header{
		"Authorization": []string{"Bearer example-token"},
		"X-Profile":     []string{"profile-a"},
	}

	worker, err := Start(context.Background(), Options{
		Endpoint:         mock.endpoint(),
		Headers:          customHeaders,
		StorageStatePath: storagePath,
		Model:            "gemini-2.5-flash",
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	// 验证 /json/version HTTP 与 WebSocket 握手均携带自定义请求头
	mock.mu.Lock()
	verAuth := mock.versionHeaders.Get("Authorization")
	verProfile := mock.versionHeaders.Get("X-Profile")
	wsAuth := mock.wsHandshakeHeaders.Get("Authorization")
	wsProfile := mock.wsHandshakeHeaders.Get("X-Profile")
	mock.mu.Unlock()
	if verAuth != "Bearer example-token" || verProfile != "profile-a" {
		t.Fatalf("/json/version 未透传自定义头: auth=%q profile=%q", verAuth, verProfile)
	}
	if wsAuth != "Bearer example-token" || wsProfile != "profile-a" {
		t.Fatalf("WebSocket 握手未透传自定义头: auth=%q profile=%q", wsAuth, wsProfile)
	}

	// 验证 PageCatalog 与公共协议头
	catalog := worker.PageCatalog()
	if string(catalog.Logging) != `[1,"logging-context"]` ||
		string(catalog.Tier) != `["free-tier"]` ||
		string(catalog.Models) != `[["models/gemini-2.5-flash"]]` ||
		!strings.Contains(catalog.Bundle, "/boq-makersuite/_/js/") {
		t.Fatalf("PageCatalog 不符合预期: %+v", catalog)
	}
	headers, err := worker.ProtocolHeaders(context.Background())
	if err != nil {
		t.Fatalf("ProtocolHeaders 失败: %v", err)
	}
	if headers.Get("X-Goog-Api-Key") != "example-api-key" ||
		headers.Get("X-AIStudio-Visit-Id") != "visit-123" ||
		headers.Get("User-Agent") != "Mozilla/5.0 MockChrome/130.0" {
		t.Fatalf("公共协议头缺失: %+v", headers)
	}

	// 验证 Cookie 导出（含分区与未分区）与 SetCookies 合并
	exported, err := worker.StorageCookies(context.Background())
	if err != nil {
		t.Fatalf("StorageCookies 失败: %v", err)
	}
	var cookies []storageCookie
	if err := json.Unmarshal(exported, &cookies); err != nil {
		t.Fatalf("解析导出 Cookie 失败: %v", err)
	}
	if len(cookies) != 2 || cookies[0].Name != "SAPISID" || cookies[1].Name != "PART" || cookies[1].PartitionKey != "https://aistudio.google.com" {
		t.Fatalf("导出 Cookie 顺序或分区不符合预期: %+v", cookies)
	}

	extra, _ := json.Marshal([]storageCookie{{
		Name: "NID", Value: "n1", Domain: ".google.com", Path: "/", Expires: 1893456000, Secure: true, SameSite: "Lax",
	}})
	if err := worker.SetCookies(context.Background(), extra); err != nil {
		t.Fatalf("SetCookies 失败: %v", err)
	}
	exported, err = worker.StorageCookies(context.Background())
	if err != nil {
		t.Fatalf("二次 StorageCookies 失败: %v", err)
	}
	if err := json.Unmarshal(exported, &cookies); err != nil || len(cookies) != 3 || cookies[2].Name != "NID" {
		t.Fatalf("合并后 Cookie 不符合预期: %+v", cookies)
	}

	// 验证 Proof 重试（首次返回 null，第二次成功）
	mock.mu.Lock()
	mock.proofNullAttempts = 1
	mock.mu.Unlock()
	proof, err := worker.Proof(context.Background(), "deadbeef", "hello\r\nworld")
	if err != nil {
		t.Fatalf("Proof 失败: %v", err)
	}
	if proof != "!mock-waa-proof-token" {
		t.Fatalf("Proof = %q", proof)
	}
	mock.mu.Lock()
	proofCalls := mock.proofCallCount
	mock.mu.Unlock()
	if proofCalls != 2 {
		t.Fatalf("Proof 应重试一次，实际调用 %d 次", proofCalls)
	}

	// 验证 ActiveTab 扩展点
	if tab, ok := worker.ActiveTab(); !ok || tab.TargetID == "" || tab.SessionID == "" || tab.PageObjectID == "" {
		t.Fatalf("ActiveTab 扩展点未就绪: %+v ok=%v", tab, ok)
	}

	// 验证 Close 仅关闭自身创建的 Tab，绝不关闭外部浏览器或已有 Tab
	if err := worker.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	mock.mu.Lock()
	browserClosed := mock.browserClosed
	closedTargets := slices.Clone(mock.closedTargets)
	createdTargets := slices.Clone(mock.createdTargets)
	mock.mu.Unlock()
	if browserClosed {
		t.Fatal("Close 绝不能调用 Browser.close 关闭外部浏览器")
	}
	if !slices.Equal(closedTargets, createdTargets) || slices.Contains(closedTargets, mock.existingTargetID) {
		t.Fatalf("Close 应仅关闭自身创建的标签页: created=%v closed=%v", createdTargets, closedTargets)
	}
}

func TestWorkerPageDenialAndAuthRedirect(t *testing.T) {
	t.Run("region_not_supported", func(t *testing.T) {
		mock := newMockCDPServer(t, "")
		mock.denialURL = camoufoxnative.AvailableRegionsURL
		_, err := Start(context.Background(), Options{
			Endpoint: mock.endpoint(),
			Model:    "gemini-2.5-flash",
		})
		if !errors.Is(err, camoufoxnative.ErrRegionNotSupported) {
			t.Fatalf("期望 ErrRegionNotSupported，实际: %v", err)
		}
	})

	t.Run("auth_required", func(t *testing.T) {
		mock := newMockCDPServer(t, "")
		mock.authRedirectURL = "https://accounts.google.com/v3/signin/identifier"
		_, err := Start(context.Background(), Options{
			Endpoint: mock.endpoint(),
			Model:    "gemini-2.5-flash",
		})
		if !errors.Is(err, camoufoxnative.ErrAuthenticationRequired) {
			t.Fatalf("期望 ErrAuthenticationRequired，实际: %v", err)
		}
	})
}

func TestSendProtectedStreamingBackpressureCancelAndError(t *testing.T) {
	mock := newMockCDPServer(t, "")
	// 发送 6 个分块，超过 protectedGrantBatch (4)，触发背压额度授予
	mock.protectedChunks = []string{"p1-", "p2-", "p3-", "p4-", "p5-", "p6"}

	worker, err := Start(context.Background(), Options{
		Endpoint: mock.endpoint(),
		Model:    "gemini-2.5-flash",
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer worker.Close()

	reqHeaders := http.Header{
		"Content-Type": []string{"application/json+protobuf"},
		"Priority":     []string{"u=0"},
	}
	resp, err := worker.SendProtected(context.Background(), "https://alkalimakersuite-pa.clients6.google.com/$rpc/GenerateContent", reqHeaders, []byte(`["body"]`))
	if err != nil {
		t.Fatalf("SendProtected 失败: %v", err)
	}
	if resp.StatusCode != 200 || resp.Header.Get("X-Mock") != "yes" {
		t.Fatalf("响应头异常: status=%d header=%+v", resp.StatusCode, resp.Header)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取流式响应失败: %v", err)
	}
	_ = resp.Body.Close()
	if string(data) != "p1-p2-p3-p4-p5-p6" {
		t.Fatalf("流式拼装正文 = %q", string(data))
	}

	mock.mu.Lock()
	grants := mock.grantCount
	keys := slices.Clone(mock.pauseKeyEvents)
	mock.mu.Unlock()
	if grants < 1 {
		t.Fatalf("超过 %d 块时应触发背压 credit grant，实际 %d", protectedGrantBatch, grants)
	}
	if !slices.Equal(keys, []string{"rawKeyDown:Pause", "keyUp:Pause"}) {
		t.Fatalf("Priority: u=0 应派发 Pause 按键事件，实际 %v", keys)
	}

	// 测试中途中止（提前 Close）触发 protectedCancel
	mock.mu.Lock()
	mock.protectedChunks = []string{"first-part"}
	mock.protectedWaitForClose = true
	mock.mu.Unlock()

	resp2, err := worker.SendProtected(context.Background(), "https://alkalimakersuite-pa.clients6.google.com/$rpc/GenerateContent", http.Header{}, []byte(`["body"]`))
	if err != nil {
		t.Fatalf("第二次 SendProtected 失败: %v", err)
	}
	buf := make([]byte, 16)
	n, err := resp2.Body.Read(buf)
	if err != nil || string(buf[:n]) != "first-part" {
		t.Fatalf("读取首块失败: n=%d err=%v", n, err)
	}
	if err := resp2.Body.Close(); err != nil {
		t.Fatalf("提前 Close 失败: %v", err)
	}
	mock.mu.Lock()
	cancels := mock.cancelCount
	mock.mu.Unlock()
	if cancels != 1 {
		t.Fatalf("提前 Close 应触发 1 次 protectedCancel，实际 %d", cancels)
	}

	// 测试流式错误传播
	mock.mu.Lock()
	mock.protectedWaitForClose = false
	mock.protectedChunks = []string{"partial"}
	mock.protectedMidError = "NetworkError when attempting to fetch resource."
	mock.mu.Unlock()

	resp3, err := worker.SendProtected(context.Background(), "https://alkalimakersuite-pa.clients6.google.com/$rpc/GenerateContent", http.Header{}, []byte(`["body"]`))
	if err != nil {
		t.Fatalf("第三次 SendProtected 失败: %v", err)
	}
	_, readErr := io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
	if readErr == nil || !strings.Contains(readErr.Error(), "NetworkError") {
		t.Fatalf("期望流式错误传播，实际: %v", readErr)
	}
}

func TestReloadIdleAndBusyTabSwitching(t *testing.T) {
	mock := newMockCDPServer(t, "")
	worker, err := Start(context.Background(), Options{
		Endpoint: mock.endpoint(),
		Model:    "gemini-2.5-flash",
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer worker.Close()

	initialTab, _ := worker.ActiveTab()

	// 1) 空闲时原地重载：保持同一个 targetId
	if err := worker.Reload(context.Background()); err != nil {
		t.Fatalf("空闲 Reload 失败: %v", err)
	}
	afterIdleTab, _ := worker.ActiveTab()
	if afterIdleTab.TargetID != initialTab.TargetID {
		t.Fatalf("空闲 Reload 应原地复用标签页: before=%s after=%s", initialTab.TargetID, afterIdleTab.TargetID)
	}

	// 2) 有进行中受保护请求时重载：新开 Tab 并切换，旧 Tab 待请求结束后才关闭
	mock.mu.Lock()
	mock.protectedChunks = []string{"hold"}
	mock.protectedWaitForClose = true
	mock.mu.Unlock()

	resp, err := worker.SendProtected(context.Background(), "https://alkalimakersuite-pa.clients6.google.com/$rpc/GenerateContent", http.Header{}, []byte(`["body"]`))
	if err != nil {
		t.Fatalf("SendProtected 失败: %v", err)
	}

	if err := worker.Reload(context.Background()); err != nil {
		t.Fatalf("忙碌 Reload 失败: %v", err)
	}
	afterBusyTab, _ := worker.ActiveTab()
	if afterBusyTab.TargetID == initialTab.TargetID {
		t.Fatalf("忙碌 Reload 应新开标签页并切换，实际仍为 %s", afterBusyTab.TargetID)
	}

	mock.mu.Lock()
	closedBeforeDone := slices.Clone(mock.closedTargets)
	mock.mu.Unlock()
	if slices.Contains(closedBeforeDone, initialTab.TargetID) {
		t.Fatalf("进行中请求尚未结束前不应关闭旧标签页: %v", closedBeforeDone)
	}

	// 结束旧请求，旧标签页应自动关闭
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("关闭旧请求失败: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mock.mu.Lock()
		closed := slices.Contains(mock.closedTargets, initialTab.TargetID)
		mock.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("旧请求结束后旧标签页 %s 未被关闭", initialTab.TargetID)
}

func TestDisconnectWakesWaitersAndMarksErr(t *testing.T) {
	mock := newMockCDPServer(t, "")
	worker, err := Start(context.Background(), Options{
		Endpoint: mock.endpoint(),
		Model:    "gemini-2.5-flash",
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer worker.Close()

	// 1) 模拟外部浏览器重启/断开连接时，acquireChat 与 waitLoad 应立即返回 ErrDisconnected，不阻塞到超时
	worker.mu.Lock()
	worker.chats[0].busy = true
	worker.chats[0].resetting = true
	worker.mu.Unlock()

	acquireErrCh := make(chan error, 1)
	go func() {
		_, err := worker.acquireChat(context.Background())
		acquireErrCh <- err
	}()

	waitLoadErrCh := make(chan error, 1)
	worker.client.startLoad("session-wait", "aistudio.google.com", "alkalimakersuite-pa.clients6.google.com")
	go func() {
		waitLoadErrCh <- worker.client.waitLoad(context.Background(), "session-wait", 30*time.Second, "测试事件", func(*pageLoad) bool {
			return false
		})
	}()

	// 稍微等待两个 waiter 进入 select，随后强制关闭底层 WebSocket 模拟外部浏览器重启
	time.Sleep(30 * time.Millisecond)
	_ = worker.client.close()

	select {
	case <-worker.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket 断开后 Worker.Done() 未关闭")
	}
	if !errors.Is(worker.Err(), ErrDisconnected) {
		t.Fatalf("Worker.Err() = %v, 期望包含 ErrDisconnected", worker.Err())
	}
	if _, err := worker.ProtocolHeaders(context.Background()); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("断开后 ProtocolHeaders() = %v, 期望包含 ErrDisconnected", err)
	}

	select {
	case err := <-acquireErrCh:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("acquireChat err = %v, 期望包含 ErrDisconnected", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("acquireChat 在 WebSocket 断开后阻塞未返回")
	}

	select {
	case err := <-waitLoadErrCh:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("waitLoad err = %v, 期望包含 ErrDisconnected", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitLoad 在 WebSocket 断开后阻塞未返回")
	}
}
