package cdpnative

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/Mag1cFall/AIStudio2API/internal/waa/rhythm"
	"github.com/gorilla/websocket"
)

var fastTestRhythm = rhythm.Rhythm{
	Press:   0.1,
	Hold:    0.1,
	Proof:   0.1,
	Control: 0.1,
	Key:     0.1,
	Release: 0.1,
}

type armedCall struct {
	sessionID   string
	requestID   string
	digest      string
	head        string
	tail        string
	bindingName string
	sentBody    string
}

type generateMockCDP struct {
	t      *testing.T
	server *httptest.Server

	mu                sync.Mutex
	nextTargetSeq     int
	existingTargetID  string
	createdTargets    []string
	closedTargets     []string
	activatedTargets  []string
	browserClosed     bool
	insertedTexts     []string
	mouseEvents       []string
	keyEvents         []string
	clickedSelectors  []string
	armedCalls        []armedCall
	grantCount        int
	cancelCount       int
	stopClicked       int
	newChatClicked    int
	discardClicked    int
	rerunClicked      int
	runClicked        int
	submitClicked     int
	textareaValue     map[string]string
	promptFocused     map[string]bool
	running           map[string]bool
	lastTargetClicked map[string]string

	// 可配置的响应行为
	proofValue     string
	statusCode     int
	chunks         []string
	headError      string
	midError       string
	waitForClose   bool
	delaySent      time.Duration
	hangAfterStart bool
	runEnabled     bool
	onArmedEmit    func(sessionID, bindingName string)
}

func newGenerateMockCDP(t *testing.T) *generateMockCDP {
	t.Helper()
	m := &generateMockCDP{
		t:                 t,
		existingTargetID:  "pre-existing-tab",
		textareaValue:     make(map[string]string),
		promptFocused:     make(map[string]bool),
		running:           make(map[string]bool),
		lastTargetClicked: make(map[string]string),
		proofValue:        "!mock-page-proof",
		statusCode:        http.StatusOK,
		chunks:            []string{"hello-", "world"},
		runEnabled:        true,
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/json/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"Browser":              "Chrome/130.0.0.0",
			"webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/browser/mock-gen",
		})
	})
	mux.HandleFunc("/devtools/browser/mock-gen", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		m.serveConn(conn)
	})
	m.server = httptest.NewServer(mux)
	t.Cleanup(m.server.Close)
	return m
}

func (m *generateMockCDP) serveConn(conn *websocket.Conn) {
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
		waaBindings   = map[string]string{}
		pendingArmed  = map[string]armedCall{}
		activeStreams = map[string]chan struct{}{}
	)

	triggerArmed := func(sessionID string) {
		stateMu.Lock()
		armed, ok := pendingArmed[sessionID]
		if ok {
			delete(pendingArmed, sessionID)
		}
		stateMu.Unlock()
		if !ok {
			return
		}
		cancelCh := make(chan struct{})
		stateMu.Lock()
		activeStreams[armed.requestID] = cancelCh
		stateMu.Unlock()

		go func() {
			m.mu.Lock()
			delaySent := m.delaySent
			hangAfterStart := m.hangAfterStart
			proofVal := m.proofValue
			statusCode := m.statusCode
			chunks := slices.Clone(m.chunks)
			headErr := m.headError
			midErr := m.midError
			waitForClose := m.waitForClose
			m.running[sessionID] = true
			m.mu.Unlock()

			if delaySent > 0 {
				time.Sleep(delaySent)
			}
			if hangAfterStart {
				<-cancelCh
				return
			}

			encodedProof, _ := json.Marshal(proofVal)
			armed.sentBody = armed.head + string(encodedProof) + armed.tail
			m.mu.Lock()
			m.armedCalls = append(m.armedCalls, armed)
			m.mu.Unlock()

			sentPayload, _ := json.Marshal(map[string]any{
				"sent":    true,
				"proof":   proofVal,
				"refresh": 1.2,
				"compute": 2.3,
			})
			sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
				"name":    armed.bindingName,
				"payload": string(sentPayload),
			})

			if headErr != "" {
				sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
					"name":    armed.bindingName,
					"payload": fmt.Sprintf(`{"error":%q}`, headErr),
				})
				return
			}

			sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
				"name":    armed.bindingName,
				"payload": fmt.Sprintf(`{"status":%d,"headers":[["Content-Type","application/json+protobuf"]]}`, statusCode),
			})

			for _, chunk := range chunks {
				select {
				case <-cancelCh:
					return
				default:
				}
				sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
					"name":    armed.bindingName,
					"payload": base64.StdEncoding.EncodeToString([]byte(chunk)),
				})
			}

			if midErr != "" {
				sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
					"name":    armed.bindingName,
					"payload": fmt.Sprintf(`{"error":%q}`, midErr),
				})
				m.mu.Lock()
				m.running[sessionID] = false
				m.mu.Unlock()
				return
			}

			if waitForClose {
				<-cancelCh
				m.mu.Lock()
				m.running[sessionID] = false
				m.mu.Unlock()
				return
			}

			m.mu.Lock()
			m.running[sessionID] = false
			m.mu.Unlock()
			sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{
				"name":    armed.bindingName,
				"payload": `{"done":true}`,
			})
		}()
	}

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
			sendResult(req.ID, "", map[string]any{"sessionId": "session-" + targetID})

		case "Target.activateTarget":
			targetID, _ := req.Params["targetId"].(string)
			m.mu.Lock()
			m.activatedTargets = append(m.activatedTargets, targetID)
			m.mu.Unlock()
			sendResult(req.ID, "", map[string]any{})

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
				waaBindings[req.SessionID] = name
				stateMu.Unlock()
			}
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Page.navigate":
			sendResult(req.ID, req.SessionID, map[string]any{"frameId": "main-frame"})
			sessionID := req.SessionID
			go func() {
				sendEvent(sessionID, "Network.responseReceived", map[string]any{
					"requestId": "req-bundle",
					"response":  map[string]any{"url": "https://www.gstatic.com/_/mss/boq-makersuite/_/js/k=test/m=_b"},
				})
				for _, rpc := range []struct{ id, name string }{
					{"req-logging", "GetLoggingContext"},
					{"req-tier", "GetAiStudioBenefitTier"},
					{"req-models", "ListModels"},
				} {
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
								"User-Agent":          "Mozilla/5.0 MockChrome/130.0",
							},
						},
					})
					sendEvent(sessionID, "Network.loadingFinished", map[string]any{"requestId": rpc.id})
				}
				stateMu.Lock()
				waaBinding := waaBindings[sessionID]
				stateMu.Unlock()
				if waaBinding != "" {
					sendEvent(sessionID, "Runtime.bindingCalled", map[string]any{"name": waaBinding, "payload": "ready"})
				}
				sendEvent(sessionID, "Network.requestWillBeSent", map[string]any{
					"requestId": "req-img",
					"request":   map[string]any{"url": "https://aistudio.google.com/generate_204", "method": "GET"},
				})
				sendEvent(sessionID, "Network.loadingFinished", map[string]any{"requestId": "req-img"})
			}()

		case "Network.getResponseBody":
			reqID, _ := req.Params["requestId"].(string)
			switch reqID {
			case "req-logging":
				sendResult(req.ID, req.SessionID, map[string]any{"body": `[1,"logging"]`, "base64Encoded": false})
			case "req-tier":
				sendResult(req.ID, req.SessionID, map[string]any{"body": `["free"]`, "base64Encoded": false})
			case "req-models":
				sendResult(req.ID, req.SessionID, map[string]any{"body": `[["models/gemini-2.5-flash"]]`, "base64Encoded": false})
			default:
				sendResult(req.ID, req.SessionID, map[string]any{"body": "", "base64Encoded": false})
			}

		case "Input.insertText":
			text, _ := req.Params["text"].(string)
			m.mu.Lock()
			m.insertedTexts = append(m.insertedTexts, text)
			m.textareaValue[req.SessionID] = text
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Input.dispatchKeyEvent":
			evType, _ := req.Params["type"].(string)
			key, _ := req.Params["key"].(string)
			m.mu.Lock()
			m.keyEvents = append(m.keyEvents, evType+":"+key)
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{})

		case "Input.dispatchMouseEvent":
			evType, _ := req.Params["type"].(string)
			m.mu.Lock()
			m.mouseEvents = append(m.mouseEvents, evType)
			target := m.lastTargetClicked[req.SessionID]
			if evType == "mouseReleased" {
				switch target {
				case newChatButtonSelector:
					m.newChatClicked++
				case "button:" + discardButtonPattern:
					m.discardClicked++
					m.textareaValue[req.SessionID] = ""
				case promptBoxSelector:
					m.promptFocused[req.SessionID] = true
				case rerunButtonSelector:
					m.rerunClicked++
				case runButtonSelector:
					if m.running[req.SessionID] {
						m.stopClicked++
						m.running[req.SessionID] = false
					} else {
						m.runClicked++
					}
				case functionResultSubmitSelector:
					m.submitClicked++
				}
			}
			m.mu.Unlock()
			sendResult(req.ID, req.SessionID, map[string]any{})
			if evType == "mouseReleased" && (target == runButtonSelector || target == rerunButtonSelector || target == functionResultSubmitSelector) {
				// 如果是 Stop 按钮，关闭活跃流
				m.mu.Lock()
				stopped := target == runButtonSelector && !m.running[req.SessionID]
				m.mu.Unlock()
				if stopped {
					stateMu.Lock()
					for id, ch := range activeStreams {
						delete(activeStreams, id)
						close(ch)
					}
					stateMu.Unlock()
				}
				triggerArmed(req.SessionID)
			}

		case "Runtime.evaluate":
			expr, _ := req.Params["expression"].(string)
			switch {
			case expr == "location.href":
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "string", "value": "https://aistudio.google.com/prompts/new_chat?model=gemini-2.5-flash&temporary=true"},
				})
			case expr == "navigator.userAgent":
				sendResult(req.ID, req.SessionID, map[string]any{
					"result": map[string]any{"type": "string", "value": "Mozilla/5.0 MockChrome/130.0"},
				})
			case expr == `document.visibilityState === 'visible'` || expr == chatEmptyExpression || expr == temporaryChatExpression:
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": true}})
			case strings.Contains(expr, discardButtonPattern) && strings.Contains(expr, ".some("):
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": true}})
			case strings.Contains(expr, rerunButtonSelector) && strings.HasPrefix(expr, "Boolean("):
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": true}})
			case expr == `!`+generationRunningExpression:
				m.mu.Lock()
				running := m.running[req.SessionID]
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": !running}})
			case expr == generationRunningExpression:
				m.mu.Lock()
				running := m.running[req.SessionID]
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": running}})
			case expr == promptFocusedExpression:
				m.mu.Lock()
				focused := m.promptFocused[req.SessionID]
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": focused}})
			case strings.Contains(expr, "return textarea.value;"):
				m.mu.Lock()
				val := m.textareaValue[req.SessionID]
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "string", "value": val}})
			case strings.Contains(expr, "getBoundingClientRect"):
				// pointerTargetFunction 调用：记录目标选择器并返回模拟坐标
				targetKey := extractPointerTarget(expr)
				m.mu.Lock()
				m.lastTargetClicked[req.SessionID] = targetKey
				m.clickedSelectors = append(m.clickedSelectors, targetKey)
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "string", "value": `{"x":100,"y":200}`}})
			case strings.Contains(expr, "ms-prompt-box textarea") || strings.Contains(expr, "GetLoggingContext"):
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "boolean", "value": true}})
			case strings.Contains(expr, "ms-g1-welcome-dialog"):
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "number", "value": 0}})
			case strings.Contains(expr, "default_MakerSuite"):
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "object", "objectId": "page-obj-" + req.SessionID}})
			default:
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "undefined"}})
			}

		case "Runtime.callFunctionOn":
			fn, _ := req.Params["functionDeclaration"].(string)
			args, _ := req.Params["arguments"].([]any)
			switch {
			case strings.Contains(fn, "this.key"):
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "string", "value": "snapshotFn"}})
			case strings.Contains(fn, "boxTooltip"):
				m.mu.Lock()
				enabled := m.runEnabled
				running := m.running[req.SessionID]
				hang := m.hangAfterStart
				m.mu.Unlock()
				payload, _ := json.Marshal(runState{
					Found:   true,
					Enabled: enabled,
					Running: running,
					Started: hang,
					Prompt:  5,
				})
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "string", "value": string(payload)}})
			case strings.Contains(fn, "XMLHttpRequest.prototype.open"):
				call := armedCall{
					sessionID:   req.SessionID,
					requestID:   argString(args, 0),
					digest:      argString(args, 1),
					head:        argString(args, 2),
					tail:        argString(args, 3),
					bindingName: argString(args, 4),
				}
				stateMu.Lock()
				pendingArmed[req.SessionID] = call
				stateMu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "undefined"}})
			case strings.Contains(fn, "request.credit +="):
				m.mu.Lock()
				m.grantCount++
				m.mu.Unlock()
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "undefined"}})
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
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "undefined"}})
			default:
				sendResult(req.ID, req.SessionID, map[string]any{"result": map[string]any{"type": "undefined"}})
			}

		default:
			sendResult(req.ID, req.SessionID, map[string]any{})
		}
	}
}

func extractPointerTarget(expr string) string {
	unescaped := strings.ReplaceAll(expr, `\"`, `"`)
	switch {
	case strings.Contains(unescaped, newChatButtonSelector):
		return newChatButtonSelector
	case strings.Contains(unescaped, discardButtonPattern):
		return "button:" + discardButtonPattern
	case strings.Contains(unescaped, functionResultSubmitSelector):
		return functionResultSubmitSelector
	case strings.Contains(unescaped, functionResultInputSelector):
		return functionResultInputSelector
	case strings.Contains(unescaped, rerunButtonSelector):
		return rerunButtonSelector
	case strings.Contains(unescaped, lastTurnSelector):
		return lastTurnSelector
	case strings.Contains(unescaped, runButtonSelector):
		return runButtonSelector
	case strings.Contains(unescaped, promptBoxSelector):
		return promptBoxSelector
	default:
		return "unknown"
	}
}

func makeGenerateTurnBody(contentsJSON string) ([]byte, int, int) {
	body := []byte(fmt.Sprintf(`["models/gemini-2.5-flash",%s,null,null,null]`, contentsJSON))
	start := bytes.LastIndex(body, []byte("null"))
	return body, start, start + len("null")
}

func TestNextChatActionAndPromptHead(t *testing.T) {
	mustContents := func(raw string) []json.RawMessage {
		var out []json.RawMessage
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := `[[[[null,"alpha"]],"user"]]`
	continued := `[[[[null,"alpha"]],"user"],[[[null,"A"]],"model"],[[[null,"beta"]],"user"]]`

	cases := []struct {
		name     string
		previous string
		contents string
		want     chatAction
	}{
		{name: "empty_page", previous: "", contents: first, want: chatContinue},
		{name: "same_rerun", previous: first, contents: first, want: chatRerun},
		{name: "append_model_user", previous: first, contents: continued, want: chatContinue},
		{name: "only_user_appended", previous: first, contents: `[[[[null,"alpha"]],"user"],[[[null,"beta"]],"user"]]`, want: chatNew},
		{name: "unrelated_new", previous: continued, contents: `[[[[null,"delta"]],"user"]]`, want: chatNew},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var prev []json.RawMessage
			if tc.previous != "" {
				prev = mustContents(tc.previous)
			}
			if got := nextChatAction(prev, mustContents(tc.contents)); got != tc.want {
				t.Fatalf("nextChatAction = %d, want %d", got, tc.want)
			}
		})
	}

	const emoji = "\U0001F600"
	head := promptHead("a" + strings.Repeat(emoji, 20000))
	if !utf8.ValidString(head) || len(utf16.Encode([]rune(head))) != 24999 {
		t.Fatalf("promptHead UTF-16 截断异常: len=%d", len(utf16.Encode([]rune(head))))
	}
}

func TestGenerateActionsStepsProofReplacementAndBackpressure(t *testing.T) {
	mock := newGenerateMockCDP(t)
	// 发送 6 块触发背压 credit grant
	mock.chunks = []string{"c1-", "c2-", "c3-", "c4-", "c5-", "c6"}

	worker, err := Start(context.Background(), Options{
		Endpoint: mock.server.URL,
		Model:    "gemini-2.5-flash",
		Rhythm:   fastTestRhythm,
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer worker.Close()

	// 1) 空页面首次 chatContinue + 异步 StepWaitSent + proof 替换 + 流式背压
	mock.mu.Lock()
	mock.delaySent = 80 * time.Millisecond
	mock.mu.Unlock()

	turn1Contents := `[[[[null,"hello world"]],"user"]]`
	body1, start1, end1 := makeGenerateTurnBody(turn1Contents)
	var steps1 []camoufoxnative.GenerateStep
	resp1, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body:       body1,
		ProofStart: start1,
		ProofEnd:   end1,
		Digest:     "digest-1",
		Step:       func(s camoufoxnative.GenerateStep) { steps1 = append(steps1, s) },
	})
	if err != nil {
		t.Fatalf("Turn 1 Generate 失败: %v", err)
	}
	if resp1.Proof != "!mock-page-proof" || resp1.StatusCode != http.StatusOK {
		t.Fatalf("Turn 1 响应元信息异常: proof=%q status=%d", resp1.Proof, resp1.StatusCode)
	}
	data1, err := io.ReadAll(resp1.Body)
	if err != nil {
		t.Fatalf("读取 Turn 1 正文失败: %v", err)
	}
	if string(data1) != "c1-c2-c3-c4-c5-c6" {
		t.Fatalf("Turn 1 正文 = %q", string(data1))
	}
	_ = resp1.Body.Close()

	wantSteps1 := []camoufoxnative.GenerateStep{
		camoufoxnative.StepAcquireTab,
		camoufoxnative.StepLockInput,
		camoufoxnative.StepSelectTab,
		camoufoxnative.StepPastePrompt,
		camoufoxnative.StepWaitRunnable,
		camoufoxnative.StepClickRun,
		camoufoxnative.StepWaitSent,
		camoufoxnative.StepSent,
	}
	if !slices.Equal(steps1, wantSteps1) {
		t.Fatalf("Turn 1 步骤顺序异常: got=%v want=%v", steps1, wantSteps1)
	}

	mock.mu.Lock()
	grants := mock.grantCount
	armed1 := mock.armedCalls[0]
	mock.mu.Unlock()
	if grants < 1 {
		t.Fatalf("超过 %d 块应触发背压 credit grant，实际 %d", protectedGrantBatch, grants)
	}
	wantSentBody := fmt.Sprintf(`["models/gemini-2.5-flash",%s,null,null,"!mock-page-proof"]`, turn1Contents)
	if armed1.digest != "digest-1" || armed1.sentBody != wantSentBody {
		t.Fatalf("XHR 拦截与 proof 替换不符: digest=%q sentBody=%q want=%q", armed1.digest, armed1.sentBody, wantSentBody)
	}

	// 等待后台 resetChat 完成（清空 tab.contents 并置 runHovered=true）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		worker.mu.Lock()
		idle := !worker.chats[0].busy && !worker.chats[0].resetting && worker.chats[0].runHovered
		worker.mu.Unlock()
		if idle {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 2) chatRerun 分支：保留 tab.contents 为 turn1Contents 后再次发送相同 contents
	mock.mu.Lock()
	mock.delaySent = 0
	mock.chunks = []string{"rerun-ok"}
	mock.mu.Unlock()

	contents1, _ := generateContents(body1)
	worker.mu.Lock()
	worker.chats[0].contents = contents1
	worker.chats[0].unsent = false
	worker.mu.Unlock()

	var steps2 []camoufoxnative.GenerateStep
	resp2, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body:       body1,
		ProofStart: start1,
		ProofEnd:   end1,
		Digest:     "digest-rerun",
		Step:       func(s camoufoxnative.GenerateStep) { steps2 = append(steps2, s) },
	})
	if err != nil {
		t.Fatalf("Turn 2 (Rerun) 失败: %v", err)
	}
	data2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if string(data2) != "rerun-ok" {
		t.Fatalf("Turn 2 正文 = %q", string(data2))
	}
	if !slices.Contains(steps2, camoufoxnative.StepClickRerun) || slices.Contains(steps2, camoufoxnative.StepPastePrompt) || slices.Contains(steps2, camoufoxnative.StepNewChat) {
		t.Fatalf("Rerun 步骤顺序异常: %v", steps2)
	}

	// 等后台 resetChat 完成后再测试 chatNew 分支
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		worker.mu.Lock()
		idle := !worker.chats[0].busy && !worker.chats[0].resetting
		worker.mu.Unlock()
		if idle {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 3) chatNew 分支：tab.contents 为 turn1Contents，传入无关对话
	worker.mu.Lock()
	worker.chats[0].contents = contents1
	worker.chats[0].unsent = false
	worker.mu.Unlock()

	turn3Contents := `[[[[null,"brand new topic"]],"user"]]`
	body3, start3, end3 := makeGenerateTurnBody(turn3Contents)
	var steps3 []camoufoxnative.GenerateStep
	resp3, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body:       body3,
		ProofStart: start3,
		ProofEnd:   end3,
		Digest:     "digest-new",
		Step:       func(s camoufoxnative.GenerateStep) { steps3 = append(steps3, s) },
	})
	if err != nil {
		t.Fatalf("Turn 3 (NewChat) 失败: %v", err)
	}
	_, _ = io.ReadAll(resp3.Body)
	_ = resp3.Body.Close()
	if !slices.Contains(steps3, camoufoxnative.StepNewChat) || !slices.Contains(steps3, camoufoxnative.StepPastePrompt) || !slices.Contains(steps3, camoufoxnative.StepClickRun) {
		t.Fatalf("NewChat 步骤顺序异常: %v", steps3)
	}
}

func TestGenerateFunctionCallContinuationAndSubmit(t *testing.T) {
	mock := newGenerateMockCDP(t)
	// 第一轮返回包含 FunctionCallMarker 的正文，触发保留对话（不后台 resetChat）
	fnChunk := string(camoufoxnative.FunctionCallMarker) + `get_weather",[["city","Tokyo"]]]]`
	mock.chunks = []string{fnChunk}

	worker, err := Start(context.Background(), Options{
		Endpoint: mock.server.URL,
		Model:    "gemini-2.5-flash",
		Rhythm:   fastTestRhythm,
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer worker.Close()

	turn1Contents := `[[[[null,"weather in Tokyo"]],"user"]]`
	body1, start1, end1 := makeGenerateTurnBody(turn1Contents)
	resp1, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body:       body1,
		ProofStart: start1,
		ProofEnd:   end1,
		Digest:     "digest-fn-1",
	})
	if err != nil {
		t.Fatalf("首轮函数调用请求失败: %v", err)
	}
	_, _ = io.ReadAll(resp1.Body)
	_ = resp1.Body.Close()

	// 构造续接函数结果正文：[user1, model_fn_call, user_fn_result]
	followUpContents := `[[[[null,"weather in Tokyo"]],"user"],[[[null,null,null,null,null,null,null,null,null,null,["get_weather"]]],"model"],[[[null,null,null,null,null,null,null,null,null,null,null,["get_weather",[["result","22C"]]]]],"user"]]`
	followUpBody, fStart, fEnd := makeGenerateTurnBody(followUpContents)
	unrelatedBody, _, _ := makeGenerateTurnBody(`[[[[null,"other"]],"user"]]`)

	if !worker.ContinuesFunctionCall(followUpBody) {
		t.Fatal("ContinuesFunctionCall 应对匹配的函数结果续接返回 true")
	}
	if worker.ContinuesFunctionCall(unrelatedBody) {
		t.Fatal("ContinuesFunctionCall 应对无关对话返回 false")
	}

	// 提交函数结果
	mock.mu.Lock()
	mock.chunks = []string{"It is 22C in Tokyo."}
	mock.mu.Unlock()

	var steps []camoufoxnative.GenerateStep
	resp2, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body:       followUpBody,
		ProofStart: fStart,
		ProofEnd:   fEnd,
		Digest:     "digest-fn-2",
		Result:     `{"result":"22C"}`,
		Step:       func(s camoufoxnative.GenerateStep) { steps = append(steps, s) },
	})
	if err != nil {
		t.Fatalf("提交函数结果失败: %v", err)
	}
	data2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if string(data2) != "It is 22C in Tokyo." {
		t.Fatalf("函数结果续接正文 = %q", string(data2))
	}

	wantSubSteps := []camoufoxnative.GenerateStep{
		camoufoxnative.StepAcquireTab,
		camoufoxnative.StepLockInput,
		camoufoxnative.StepSelectTab,
		camoufoxnative.StepPasteResult,
		camoufoxnative.StepClickSubmit,
		camoufoxnative.StepSent,
	}
	if !slices.Equal(steps, wantSubSteps) {
		t.Fatalf("函数结果提交步骤异常: got=%v want=%v", steps, wantSubSteps)
	}

	mock.mu.Lock()
	submitCount := mock.submitClicked
	inserted := slices.Clone(mock.insertedTexts)
	mock.mu.Unlock()
	if submitCount != 1 || !slices.Contains(inserted, `{"result":"22C"}`) {
		t.Fatalf("函数结果未通过输入框提交: submitCount=%d inserted=%v", submitCount, inserted)
	}
}

func TestGenerateHeadFailureStreamInterruptionTimeoutAndUnsentRecovery(t *testing.T) {
	mock := newGenerateMockCDP(t)
	worker, err := Start(context.Background(), Options{
		Endpoint: mock.server.URL,
		Model:    "gemini-2.5-flash",
		Rhythm:   fastTestRhythm,
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	defer worker.Close()

	turnContents := `[[[[null,"first prompt"]],"user"]]`
	body, start, end := makeGenerateTurnBody(turnContents)

	// 1) 读响应头失败：应返回错误并清空 tab.contents（规避 issue #223）
	mock.mu.Lock()
	mock.headError = "NetworkError when attempting to fetch resource."
	mock.mu.Unlock()

	if _, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: body, ProofStart: start, ProofEnd: end, Digest: "d-head-err",
	}); err == nil || !strings.Contains(err.Error(), "NetworkError") {
		t.Fatalf("期望读头失败错误，实际: %v", err)
	}
	worker.mu.Lock()
	headErrContents := worker.chats[0].contents
	worker.mu.Unlock()
	if headErrContents != nil {
		t.Fatalf("读响应头失败后应清空 tab.contents，实际长度=%d", len(headErrContents))
	}

	// 2) 流中途报错：应传播错误并在 Close 后清空 tab.contents
	mock.mu.Lock()
	mock.headError = ""
	mock.chunks = []string{"partial"}
	mock.midError = "AbortError: stream reset"
	mock.mu.Unlock()

	respMid, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: body, ProofStart: start, ProofEnd: end, Digest: "d-mid-err",
	})
	if err != nil {
		t.Fatalf("流中途报错场景 Generate 失败: %v", err)
	}
	_, readErr := io.ReadAll(respMid.Body)
	_ = respMid.Body.Close()
	if readErr == nil || !strings.Contains(readErr.Error(), "AbortError") {
		t.Fatalf("期望流中途错误，实际: %v", readErr)
	}
	worker.mu.Lock()
	midErrContents := worker.chats[0].contents
	worker.mu.Unlock()
	if midErrContents != nil {
		t.Fatalf("流中途报错后应清空 tab.contents，实际长度=%d", len(midErrContents))
	}

	// 3) 流未读完提前 Close（中断）：应点击 Stop 并清空 tab.contents
	mock.mu.Lock()
	mock.midError = ""
	mock.chunks = []string{"first-chunk"}
	mock.waitForClose = true
	mock.mu.Unlock()

	respEarly, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: body, ProofStart: start, ProofEnd: end, Digest: "d-early-close",
	})
	if err != nil {
		t.Fatalf("流中断场景 Generate 失败: %v", err)
	}
	buf := make([]byte, 32)
	if _, err := respEarly.Body.Read(buf); err != nil {
		t.Fatalf("读取首块失败: %v", err)
	}
	_ = respEarly.Body.Close()

	worker.mu.Lock()
	earlyContents := worker.chats[0].contents
	worker.mu.Unlock()
	if earlyContents != nil {
		t.Fatalf("流中断提前 Close 后应清空 tab.contents，实际长度=%d", len(earlyContents))
	}
	mock.mu.Lock()
	stops := mock.stopClicked
	mock.mu.Unlock()
	if stops < 1 {
		t.Fatalf("流中断提前 Close 应点击 Stop 按钮，实际 %d", stops)
	}

	// 4) pageSendTimeout 超时验证 + 残留 unsent 触发全选清理
	oldTimeout := pageSendTimeout
	pageSendTimeout = 120 * time.Millisecond
	defer func() { pageSendTimeout = oldTimeout }()

	mock.mu.Lock()
	mock.waitForClose = false
	mock.hangAfterStart = true
	mock.mu.Unlock()

	if _, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: body, ProofStart: start, ProofEnd: end, Digest: "d-timeout",
	}); err == nil || !strings.Contains(err.Error(), string(camoufoxnative.StepWaitSent)) {
		t.Fatalf("期望 StepWaitSent 超时错误，实际: %v", err)
	}

	// 超时后 tab.unsent 仍为 true，下一轮应先执行 selectAll 再插入新提示词
	mock.mu.Lock()
	mock.hangAfterStart = false
	mock.chunks = []string{"recovered"}
	mock.keyEvents = nil
	mock.running["session-target-1"] = false
	mock.mu.Unlock()

	respRecover, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: body, ProofStart: start, ProofEnd: end, Digest: "d-recover",
	})
	if err != nil {
		t.Fatalf("unsent 恢复轮次失败: %v", err)
	}
	recovered, _ := io.ReadAll(respRecover.Body)
	_ = respRecover.Body.Close()
	if string(recovered) != "recovered" {
		t.Fatalf("恢复正文 = %q", string(recovered))
	}
	mock.mu.Lock()
	keys := slices.Clone(mock.keyEvents)
	mock.mu.Unlock()
	if !slices.Contains(keys, "rawKeyDown:a") {
		t.Fatalf("存在未发出残留输入时应先执行 Ctrl+A 全选，实际按键序列: %v", keys)
	}
}

func TestConcurrentMultiTabReuseAndCloseCleanup(t *testing.T) {
	mock := newGenerateMockCDP(t)
	mock.chunks = []string{"part-1"}
	mock.waitForClose = true

	worker, err := Start(context.Background(), Options{
		Endpoint: mock.server.URL,
		Model:    "gemini-2.5-flash",
		Rhythm:   fastTestRhythm,
	})
	if err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	bodyA, startA, endA := makeGenerateTurnBody(`[[[[null,"req A"]],"user"]]`)
	bodyB, startB, endB := makeGenerateTurnBody(`[[[[null,"req B"]],"user"]]`)

	// 请求 A 占用第一个标签页并保持流打开
	respA, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: bodyA, ProofStart: startA, ProofEnd: endA, Digest: "d-a",
	})
	if err != nil {
		t.Fatalf("请求 A 失败: %v", err)
	}
	bufA := make([]byte, 16)
	if _, err := respA.Body.Read(bufA); err != nil {
		t.Fatalf("读取请求 A 首块失败: %v", err)
	}

	// 请求 B 并发进入：应按需新开第二个标签页
	mock.mu.Lock()
	mock.waitForClose = false
	mock.chunks = []string{"resp-b"}
	mock.mu.Unlock()

	respB, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: bodyB, ProofStart: startB, ProofEnd: endB, Digest: "d-b",
	})
	if err != nil {
		t.Fatalf("并发请求 B 失败: %v", err)
	}
	dataB, _ := io.ReadAll(respB.Body)
	_ = respB.Body.Close()
	if string(dataB) != "resp-b" {
		t.Fatalf("请求 B 正文 = %q", string(dataB))
	}

	// 关闭请求 A，释放第一个标签页
	_ = respA.Body.Close()

	// 等待两个标签页均空闲
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		worker.mu.Lock()
		ready := len(worker.chats) == 2 && !worker.chats[0].busy && !worker.chats[1].busy
		worker.mu.Unlock()
		if ready {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 第三个串行请求应复用已有标签页，不再新建第 3 个标签页
	respC, err := worker.Generate(context.Background(), camoufoxnative.GenerateTurn{
		Body: bodyB, ProofStart: startB, ProofEnd: endB, Digest: "d-c",
	})
	if err != nil {
		t.Fatalf("复用标签页请求 C 失败: %v", err)
	}
	_, _ = io.ReadAll(respC.Body)
	_ = respC.Body.Close()

	mock.mu.Lock()
	createdBeforeClose := slices.Clone(mock.createdTargets)
	mock.mu.Unlock()
	if len(createdBeforeClose) != 2 {
		t.Fatalf("并发 2 路 + 复用 1 路应只创建 2 个标签页，实际创建: %v", createdBeforeClose)
	}

	// Close 必须关闭所有自建 Tab，绝不关闭外部浏览器或已有 Tab
	if err := worker.Close(); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	mock.mu.Lock()
	browserClosed := mock.browserClosed
	closedTargets := slices.Clone(mock.closedTargets)
	mock.mu.Unlock()
	if browserClosed {
		t.Fatal("Close 绝不能关闭外部浏览器进程")
	}
	for _, id := range createdBeforeClose {
		if !slices.Contains(closedTargets, id) {
			t.Fatalf("Close 未关闭自建标签页 %s: closed=%v", id, closedTargets)
		}
	}
	if slices.Contains(closedTargets, mock.existingTargetID) {
		t.Fatalf("Close 不应关闭已有标签页 %s: closed=%v", mock.existingTargetID, closedTargets)
	}
}
