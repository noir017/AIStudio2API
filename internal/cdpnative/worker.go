package cdpnative

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/Mag1cFall/AIStudio2API/internal/waa/prooftime"
	"github.com/Mag1cFall/AIStudio2API/internal/waa/rhythm"
)

const (
	pageImageTimeout   = 30 * time.Second
	pageReleaseTimeout = 10 * time.Second
)

type tabUse struct {
	targetID string
	requests int
	retired  bool
}

type loadedPage struct {
	pageObjectID string
	state        camoufoxnative.State
	catalog      camoufoxnative.PageCatalog
}

// Worker 维护单个账户到外部 CDP 浏览器的标签页与 WAA service；绝不启动或关闭外部浏览器进程
type Worker struct {
	mu           sync.Mutex
	inputMu      sync.Mutex
	inputWaiters atomic.Int32
	client       *cdpClient
	options      Options
	rhythm       rhythm.Rhythm
	targetID     string
	sessionID    string
	pageObjectID string
	state        camoufoxnative.State
	pageCatalog  camoufoxnative.PageCatalog
	cookieOrder  map[cookieKey]int
	// tabs 跟踪正在执行受保护请求或已被 Reload 替换的标签页，键为 sessionID
	tabs map[string]*tabUse
	// chats 是可运行界面对话的标签页池，第一个是当前主页面所在的标签页
	chats []*chatTab
	// chatsChanged 在界面对话标签页空出、新开结束或 Worker 关闭时关闭并换新
	chatsChanged chan struct{}
	// chatOpening 表示正在后台新开界面对话标签页，chatOpenErr 是最近一次新开失败的原因
	chatOpening bool
	chatOpenErr error
	// selected 是外部浏览器中当前激活的 targetID
	selected string
	// ownedTargets 记录本 Worker 创建的全部 targetID，Close 时仅关闭这些标签页
	ownedTargets map[string]struct{}
	closed       bool
}

// Start 连接外部 CDP 浏览器，仅通过 Target.createTarget 新建隔离标签页并完成 AI Studio WAA bootstrap
func Start(ctx context.Context, options Options) (*Worker, error) {
	if strings.TrimSpace(options.Endpoint) == "" {
		return nil, errors.New("外部 CDP 后端缺少 CDP_ENDPOINT 或账户 cdp_endpoint 配置")
	}
	if options.Model == "" {
		return nil, errors.New("WAA bootstrap 缺少实时目录聊天模型")
	}

	var storage storageState
	if strings.TrimSpace(options.StorageStatePath) != "" {
		loaded, err := loadStorageState(options.StorageStatePath)
		if err != nil {
			return nil, err
		}
		storage = loaded
	}

	options.reportStartup(camoufoxnative.StartupPreparingBrowser)
	wsURL, err := ResolveWebSocketURL(ctx, options.Endpoint, options.Headers, options.HTTPClient)
	if err != nil {
		return nil, err
	}

	options.reportStartup(camoufoxnative.StartupConnectingBiDi)
	client, err := dialCDP(ctx, wsURL, options.Headers)
	if err != nil {
		return nil, err
	}

	worker := &Worker{
		client:       client,
		options:      options,
		rhythm:       options.Rhythm,
		tabs:         make(map[string]*tabUse),
		ownedTargets: make(map[string]struct{}),
	}
	if worker.rhythm == (rhythm.Rhythm{}) {
		worker.rhythm = rhythm.New()
	}
	failed := true
	defer func() {
		if failed {
			_ = worker.Close()
		}
	}()

	targetID, sessionID, err := worker.createAndAttachTab(ctx)
	if err != nil {
		return nil, err
	}
	worker.targetID = targetID
	worker.sessionID = sessionID

	if err := worker.restoreStorage(ctx, sessionID, storage); err != nil {
		return nil, err
	}

	loaded, err := worker.openPage(ctx, options, sessionID)
	if err != nil {
		return nil, err
	}
	worker.pageObjectID = loaded.pageObjectID
	worker.state = loaded.state
	worker.pageCatalog = loaded.catalog
	worker.chats = []*chatTab{{targetID: targetID, sessionID: sessionID, pageObjectID: loaded.pageObjectID}}
	worker.selected = targetID

	failed = false
	return worker, nil
}

func (worker *Worker) createAndAttachTab(ctx context.Context) (string, string, error) {
	var created struct {
		TargetID string `json:"targetId"`
	}
	if err := worker.client.call(ctx, "", "Target.createTarget", map[string]any{"url": "about:blank"}, &created); err != nil {
		return "", "", fmt.Errorf("创建外部浏览器标签页: %w", err)
	}
	if created.TargetID == "" {
		return "", "", errors.New("外部 CDP Target.createTarget 未返回有效 targetId")
	}

	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		_ = worker.closeTarget(created.TargetID)
		return "", "", errors.New("外部 CDP runtime 已关闭")
	}
	worker.ownedTargets[created.TargetID] = struct{}{}
	worker.mu.Unlock()

	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := worker.client.call(ctx, "", "Target.attachToTarget", map[string]any{
		"targetId": created.TargetID,
		"flatten":  true,
	}, &attached); err != nil {
		_ = worker.closeTarget(created.TargetID)
		return "", "", fmt.Errorf("附加外部浏览器标签页: %w", err)
	}
	if attached.SessionID == "" {
		_ = worker.closeTarget(created.TargetID)
		return "", "", errors.New("外部 CDP Target.attachToTarget 未返回有效 sessionId")
	}
	return created.TargetID, attached.SessionID, nil
}

func (worker *Worker) closeTarget(targetID string) error {
	if targetID == "" {
		return nil
	}
	worker.mu.Lock()
	delete(worker.ownedTargets, targetID)
	client := worker.client
	worker.mu.Unlock()
	if client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.call(ctx, "", "Target.closeTarget", map[string]any{"targetId": targetID}, nil); err != nil {
		return fmt.Errorf("关闭外部浏览器标签页: %w", err)
	}
	return nil
}

func (worker *Worker) restoreStorage(ctx context.Context, sessionID string, storage storageState) error {
	if len(storage.Cookies) > 0 {
		worker.cookieOrder = make(map[cookieKey]int, len(storage.Cookies))
		for index, cookie := range storage.Cookies {
			worker.cookieOrder[cookie.key()] = index
		}
		if err := worker.client.installCookies(ctx, sessionID, storage.Cookies); err != nil {
			return err
		}
	}
	if len(storage.Origins) > 0 {
		script, err := buildLocalStoragePreloadScript(storage.Origins)
		if err != nil {
			return err
		}
		if err := worker.client.call(ctx, sessionID, "Page.addScriptToEvaluateOnNewDocument", map[string]any{"source": script}, nil); err != nil {
			return fmt.Errorf("安装 localStorage 预加载脚本: %w", err)
		}
	}
	return nil
}

func (worker *Worker) openPage(ctx context.Context, options Options, sessionID string) (loadedPage, error) {
	client := worker.client
	originParsed, err := url.Parse(options.origin())
	if err != nil || originParsed.Hostname() == "" {
		return loadedPage{}, fmt.Errorf("AI Studio Origin 无效: %q", options.origin())
	}

	bindingName := "__cdp_waa_ready_" + rand.Text()
	symbolKey := "__cdp_waa_service_" + rand.Text()
	services, err := client.openBinding(ctx, sessionID, bindingName)
	if err != nil {
		return loadedPage{}, err
	}
	defer client.closeBinding(sessionID, bindingName)

	client.startLoad(sessionID, originParsed.Hostname(), options.rpcHost())
	defer client.endLoad(sessionID)

	if err := client.call(ctx, sessionID, "Page.enable", nil, nil); err != nil {
		return loadedPage{}, fmt.Errorf("启用 CDP Page 域: %w", err)
	}
	if err := client.call(ctx, sessionID, "Runtime.enable", nil, nil); err != nil {
		return loadedPage{}, fmt.Errorf("启用 CDP Runtime 域: %w", err)
	}
	if err := client.call(ctx, sessionID, "Network.enable", nil, nil); err != nil {
		return loadedPage{}, fmt.Errorf("启用 CDP Network 域: %w", err)
	}

	var preload struct {
		Identifier string `json:"identifier"`
	}
	if err := client.call(ctx, sessionID, "Page.addScriptToEvaluateOnNewDocument", map[string]any{
		"source": buildServicePreloadScript(options.origin(), bindingName, symbolKey),
	}, &preload); err != nil {
		return loadedPage{}, fmt.Errorf("安装官网 WAA service 预加载脚本: %w", err)
	}

	released := false
	release := func() error {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pageReleaseTimeout)
		defer cancel()
		var errs []error
		if preload.Identifier != "" {
			if err := client.call(releaseCtx, sessionID, "Page.removeScriptToEvaluateOnNewDocument", map[string]any{"identifier": preload.Identifier}, nil); err != nil {
				errs = append(errs, fmt.Errorf("移除 WAA service 预加载脚本: %w", err))
			}
		}
		released = true
		return errors.Join(errs...)
	}
	defer func() {
		if !released {
			_ = release()
		}
	}()

	options.reportStartup(camoufoxnative.StartupLoadingAIStudio)
	pagePath := "/prompts/new_chat"
	if options.AuthUser > 0 {
		pagePath = fmt.Sprintf("/u/%d/prompts/new_chat", options.AuthUser)
	}
	targetURL := strings.TrimRight(options.origin(), "/") + pagePath + "?model=" + url.QueryEscape(options.Model) + "&temporary=true"

	var navResp struct {
		ErrorText string `json:"errorText"`
	}
	if err := client.call(ctx, sessionID, "Page.navigate", map[string]any{"url": targetURL}, &navResp); err != nil {
		return loadedPage{}, fmt.Errorf("导航 AI Studio: %w", err)
	}
	if navResp.ErrorText != "" && !strings.Contains(navResp.ErrorText, "ERR_ABORTED") {
		return loadedPage{}, fmt.Errorf("导航 AI Studio 失败: %s", navResp.ErrorText)
	}

	if err := client.waitFor(ctx, sessionID, workerPageReadyExpression, options.readyTimeout()); err != nil {
		var denial *camoufoxnative.AccessDenial
		if errors.As(err, &denial) {
			return loadedPage{}, err
		}
		pageURL, _ := client.evaluateString(ctx, sessionID, "location.href")
		return loadedPage{}, fmt.Errorf("AI Studio 输入框未就绪 url=%s: %w", pageURL, err)
	}

	pageURL, err := client.evaluateString(ctx, sessionID, "location.href")
	if err != nil {
		return loadedPage{}, err
	}
	if location, parseErr := url.Parse(pageURL); parseErr == nil {
		if denial := camoufoxnative.PageDenial(location); denial != nil {
			return loadedPage{}, denial
		}
		if location.Hostname() == "accounts.google.com" {
			return loadedPage{}, fmt.Errorf("%w url=%s", camoufoxnative.ErrAuthenticationRequired, pageURL)
		}
	}

	if err := client.waitFor(ctx, sessionID, loggingContextReadyExpression, 30*time.Second); err != nil {
		return loadedPage{}, fmt.Errorf("等待官网 GetLoggingContext 响应: %w", err)
	}

	catalog, err := client.readPageCatalog(ctx, sessionID, 30*time.Second)
	if err != nil {
		return loadedPage{}, err
	}

	options.reportStartup(camoufoxnative.StartupLocatingWAA)
	serviceCtx, cancelService := context.WithTimeout(ctx, 30*time.Second)
	_, err = services.next(serviceCtx)
	cancelService()
	if err != nil {
		return loadedPage{}, fmt.Errorf("等待官网 WAA service: %w", err)
	}

	if err := client.waitLoad(ctx, sessionID, 30*time.Second, "发出带 x-aistudio-visit-id 的 MakerSuite 请求", func(load *pageLoad) bool {
		return load.headers != nil
	}); err != nil {
		return loadedPage{}, err
	}

	options.reportStartup(camoufoxnative.StartupBootstrappingWAA)
	if err := client.waitLoad(ctx, sessionID, pageImageTimeout, "完成官方 WAA 程序的图片请求", func(load *pageLoad) bool {
		return load.image
	}); err != nil {
		if ctx.Err() != nil {
			return loadedPage{}, err
		}
		slog.Warn("官方 WAA 程序的图片请求没有结束，继续载入页面", "error", err)
	}

	client.mu.Lock()
	observed := maps.Clone(client.loads[sessionID].headers)
	client.mu.Unlock()

	if err := release(); err != nil {
		return loadedPage{}, err
	}
	if _, err := dismissKnownOverlays(ctx, client, sessionID); err != nil {
		return loadedPage{}, err
	}

	pageObj, err := client.evaluate(ctx, sessionID, buildPageObjectExpression(symbolKey), false, true)
	if err != nil {
		return loadedPage{}, fmt.Errorf("构造官网 WAA 页面对象: %w", err)
	}
	if pageObj.ObjectID == "" {
		return loadedPage{}, errors.New("官网高层 snapshot 函数不存在")
	}

	keyObj, err := client.callFunctionOn(ctx, sessionID, pageObj.ObjectID, `function() { return this.key; }`, nil, true, false)
	if err != nil {
		return loadedPage{}, fmt.Errorf("读取官网 snapshot 函数名: %w", err)
	}
	snapshotKey, _ := keyObj.Value.(string)

	if observed["user-agent"] == "" {
		if ua, uaErr := client.evaluateString(ctx, sessionID, "navigator.userAgent"); uaErr == nil && ua != "" {
			observed["user-agent"] = ua
		}
	}

	headers := make(http.Header, len(publicHeaderNames))
	for _, name := range publicHeaderNames {
		if value := observed[name]; value != "" {
			headers.Set(name, value)
		}
	}
	for _, name := range []string{"user-agent", "x-goog-api-key", "x-goog-authuser", "x-user-agent"} {
		if headers.Get(name) == "" {
			return loadedPage{}, fmt.Errorf("官网 MakerSuite 请求缺少必要公共头 %s", name)
		}
	}

	return loadedPage{
		pageObjectID: pageObj.ObjectID,
		catalog:      catalog,
		state: camoufoxnative.State{
			PID:         0,
			PageURL:     pageURL,
			UserAgent:   headers.Get("user-agent"),
			SnapshotKey: snapshotKey,
			Headers:     headers,
		},
	}, nil
}

func (client *cdpClient) readPageCatalog(ctx context.Context, sessionID string, timeout time.Duration) (camoufoxnative.PageCatalog, error) {
	if err := client.waitLoad(ctx, sessionID, timeout, "完成目录 RPC", func(load *pageLoad) bool {
		return len(load.responses) == len(pageCatalogRPCs)
	}); err != nil {
		client.mu.Lock()
		var completed []string
		if load := client.loads[sessionID]; load != nil {
			completed = slices.Sorted(maps.Keys(load.responses))
		}
		client.mu.Unlock()
		return camoufoxnative.PageCatalog{}, fmt.Errorf("%w，已完成 %v", err, completed)
	}

	client.mu.Lock()
	requests := maps.Clone(client.loads[sessionID].responses)
	bundle := client.loads[sessionID].bundle
	client.mu.Unlock()

	bodies := make(map[string][]byte, len(requests))
	for name, requestID := range requests {
		var bodyResp struct {
			Body          string `json:"body"`
			Base64Encoded bool   `json:"base64Encoded"`
		}
		if err := client.call(ctx, sessionID, "Network.getResponseBody", map[string]any{"requestId": requestID}, &bodyResp); err != nil {
			return camoufoxnative.PageCatalog{}, fmt.Errorf("读取页面 %s 响应: %w", name, err)
		}
		if bodyResp.Base64Encoded {
			decoded, err := base64.StdEncoding.DecodeString(bodyResp.Body)
			if err != nil {
				return camoufoxnative.PageCatalog{}, fmt.Errorf("解码页面 %s 响应: %w", name, err)
			}
			bodies[name] = decoded
		} else {
			bodies[name] = []byte(bodyResp.Body)
		}
	}

	return camoufoxnative.PageCatalog{
		Logging: bodies["GetLoggingContext"],
		Tier:    bodies["GetAiStudioBenefitTier"],
		Models:  bodies["ListModels"],
		Bundle:  bundle,
	}, nil
}

// PageCatalog 返回当前页面载入时 GetLoggingContext、GetAiStudioBenefitTier 与 ListModels 的响应正文
func (worker *Worker) PageCatalog() camoufoxnative.PageCatalog {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.pageCatalog
}

// ProtocolHeaders 返回页面捕获的公共协议头副本
func (worker *Worker) ProtocolHeaders(ctx context.Context) (http.Header, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if worker.closed {
		return nil, errors.New("外部 CDP runtime 已关闭")
	}
	if err := worker.client.err(); err != nil {
		return nil, err
	}
	return worker.state.Headers.Clone(), nil
}

// Done 在外部 CDP WebSocket 连接断开时关闭
func (worker *Worker) Done() <-chan struct{} {
	if worker == nil || worker.client == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return worker.client.done
}

// Err 返回外部 CDP 连接断开原因，连接正常时返回 nil
func (worker *Worker) Err() error {
	if worker == nil || worker.client == nil {
		return nil
	}
	return worker.client.err()
}

// Proof 写入官网 prompt 并通过页面内 WAA service 生成 fresh WAA proof
func (worker *Worker) Proof(ctx context.Context, digest string, prompt string) (string, error) {
	lockStarted := time.Now()
	worker.lockInput()
	defer worker.inputMu.Unlock()
	worker.mu.Lock()
	defer worker.mu.Unlock()
	locked := time.Now()
	prooftime.Observe(ctx, prooftime.Lock, locked.Sub(lockStarted))
	if worker.closed {
		return "", errors.New("外部 CDP runtime 已关闭")
	}
	prompt = normalizePromptNewlines(prompt)
	deadline := time.Now().Add(5 * time.Second)
	for {
		result, err := worker.client.callFunctionOn(ctx, worker.sessionID, worker.pageObjectID, proofFunction, []any{prompt, digest}, true, true)
		if err != nil {
			return "", fmt.Errorf("生成 fresh WAA proof: %w", err)
		}
		if result.Subtype != "null" && result.Value != nil {
			value, _ := result.Value.(string)
			var page struct {
				Proof string `json:"proof"`
				pageProofTiming
			}
			if err := json.Unmarshal([]byte(value), &page); err != nil {
				return "", fmt.Errorf("解析 fresh WAA proof: %w", err)
			}
			if !strings.HasPrefix(page.Proof, "!") {
				return "", errors.New("fresh WAA proof 前缀无效")
			}
			page.observe(ctx, locked)
			return page.Proof, nil
		}
		if time.Now().After(deadline) {
			return "", errors.New("官网 prompt 状态未同步")
		}
		if err := waitContext(ctx, 100*time.Millisecond); err != nil {
			return "", err
		}
	}
}

// Reload 空闲时原地重新载入当前标签页；有进行中受保护请求时新开标签页载入并切换，旧标签页待请求结束后关闭
func (worker *Worker) Reload(ctx context.Context) error {
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return errors.New("外部 CDP runtime 已关闭")
	}
	options := worker.options
	worker.mu.Unlock()
	options.StartupProgress = nil

	worker.lockInput()
	defer worker.inputMu.Unlock()

	worker.mu.Lock()
	targetID := worker.targetID
	sessionID := worker.sessionID
	busy := worker.tabs[sessionID] != nil
	if !busy && len(worker.chats) > 0 {
		reloading := worker.chats[0]
		reloading.busy, reloading.resetting = true, true
		defer func() {
			worker.mu.Lock()
			reloading.busy, reloading.resetting = false, false
			worker.notifyChats()
			worker.mu.Unlock()
		}()
	}
	worker.mu.Unlock()

	if busy {
		newTargetID, newSessionID, err := worker.createAndAttachTab(ctx)
		if err != nil {
			return err
		}
		if err := worker.activateTarget(ctx, newTargetID); err != nil {
			return errors.Join(err, worker.closeTarget(newTargetID))
		}
		targetID = newTargetID
		sessionID = newSessionID
	}

	loaded, err := worker.openPage(ctx, options, sessionID)
	if err != nil {
		if busy {
			return errors.Join(err, worker.closeTarget(targetID))
		}
		return err
	}
	return worker.switchTab(targetID, sessionID, loaded)
}

func (worker *Worker) switchTab(targetID, sessionID string, loaded loadedPage) error {
	worker.mu.Lock()
	previousTarget := worker.targetID
	previousSession := worker.sessionID
	worker.targetID = targetID
	worker.sessionID = sessionID
	worker.pageObjectID = loaded.pageObjectID
	worker.state = loaded.state
	worker.pageCatalog = loaded.catalog
	primary := &chatTab{targetID: targetID, sessionID: sessionID, pageObjectID: loaded.pageObjectID}
	if len(worker.chats) > 0 {
		worker.chats[0] = primary
	} else {
		worker.chats = []*chatTab{primary}
	}
	worker.selected = targetID
	if previousSession == sessionID {
		worker.mu.Unlock()
		return nil
	}
	use := worker.tabs[previousSession]
	if use != nil {
		use.retired = true
	}
	worker.mu.Unlock()
	if use == nil {
		return worker.closeTarget(previousTarget)
	}
	return nil
}

func (worker *Worker) beginRequest() (string, string) {
	worker.useTab(worker.sessionID, worker.targetID)
	return worker.sessionID, worker.pageObjectID
}

func (worker *Worker) useTab(sessionID, targetID string) {
	use := worker.tabs[sessionID]
	if use == nil {
		use = &tabUse{targetID: targetID}
		worker.tabs[sessionID] = use
	}
	use.requests++
}

// lockInput 为前台操作取得 inputMu
func (worker *Worker) lockInput() {
	worker.inputWaiters.Add(1)
	worker.inputMu.Lock()
	worker.inputWaiters.Add(-1)
}

// lockInputWithin 为前台操作取得 inputMu，ctx 结束或等满 limit 时放弃等待并返回错误，此后取得的 inputMu 立即释放
func (worker *Worker) lockInputWithin(ctx context.Context, limit time.Duration) error {
	worker.inputWaiters.Add(1)
	locked := make(chan struct{})
	go func() {
		worker.inputMu.Lock()
		close(locked)
	}()
	timeout := time.NewTimer(limit)
	defer timeout.Stop()
	var err error
	select {
	case <-locked:
	case <-ctx.Done():
		err = ctx.Err()
	case <-timeout.C:
		err = context.DeadlineExceeded
	}
	worker.inputWaiters.Add(-1)
	if err != nil {
		go func() {
			<-locked
			worker.inputMu.Unlock()
		}()
	}
	return err
}

// lockInputIdle 在没有前台操作等待时取得 inputMu，后台新开对话让界面运行先接收输入
func (worker *Worker) lockInputIdle() {
	for {
		worker.inputMu.Lock()
		if worker.inputWaiters.Load() == 0 {
			return
		}
		worker.inputMu.Unlock()
		runtime.Gosched()
	}
}

func (worker *Worker) activateTarget(ctx context.Context, targetID string) error {
	if err := worker.client.call(ctx, "", "Target.activateTarget", map[string]any{"targetId": targetID}, nil); err != nil {
		return fmt.Errorf("选中标签页: %w", err)
	}
	worker.mu.Lock()
	worker.selected = targetID
	worker.mu.Unlock()
	return nil
}

func (worker *Worker) finishRequest(sessionID string) {
	worker.mu.Lock()
	use := worker.tabs[sessionID]
	if use == nil {
		worker.mu.Unlock()
		return
	}
	use.requests--
	idle := use.requests == 0
	if idle {
		delete(worker.tabs, sessionID)
	}
	closing := idle && use.retired && !worker.closed
	targetID := use.targetID
	worker.mu.Unlock()
	if closing {
		if err := worker.closeTarget(targetID); err != nil {
			slog.Warn("关闭已替换的外部 CDP 标签页失败", "target", targetID, "error", err)
		}
	}
}

// State 返回 runtime 的不可变状态副本
func (worker *Worker) State() camoufoxnative.State {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	state := worker.state
	state.Headers = state.Headers.Clone()
	return state
}

// Close 仅关闭本 Worker 创建的标签页并断开 WebSocket 连接，绝不关闭外部浏览器进程
func (worker *Worker) Close() error {
	if worker == nil {
		return nil
	}
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return nil
	}
	worker.closed = true
	worker.notifyChats()
	client := worker.client
	targets := make([]string, 0, len(worker.ownedTargets))
	for id := range worker.ownedTargets {
		targets = append(targets, id)
	}
	worker.ownedTargets = map[string]struct{}{}
	worker.chats = nil
	worker.mu.Unlock()

	if client == nil {
		return nil
	}
	if client.err() == nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for _, targetID := range targets {
			_ = client.call(closeCtx, "", "Target.closeTarget", map[string]any{"targetId": targetID}, nil)
		}
		cancel()
	}
	return client.close()
}

// ActiveTab 返回当前活跃标签页句柄，供二期 pageGenerator 界面交互扩展使用
func (worker *Worker) ActiveTab() (TabHandle, bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed || worker.sessionID == "" {
		return TabHandle{}, false
	}
	return TabHandle{
		TargetID:     worker.targetID,
		SessionID:    worker.sessionID,
		PageObjectID: worker.pageObjectID,
	}, true
}

// DispatchKeyEvent 向指定标签页发送 CDP 键盘事件（二期 pageGenerator 扩展点）
func (worker *Worker) DispatchKeyEvent(ctx context.Context, sessionID string, params map[string]any) error {
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return errors.New("外部 CDP runtime 已关闭")
	}
	client := worker.client
	worker.mu.Unlock()
	return client.call(ctx, sessionID, "Input.dispatchKeyEvent", params, nil)
}

// DispatchMouseEvent 向指定标签页发送 CDP 鼠标事件（二期 pageGenerator 扩展点）
func (worker *Worker) DispatchMouseEvent(ctx context.Context, sessionID string, params map[string]any) error {
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return errors.New("外部 CDP runtime 已关闭")
	}
	client := worker.client
	worker.mu.Unlock()
	return client.call(ctx, sessionID, "Input.dispatchMouseEvent", params, nil)
}

func loadStorageState(path string) (storageState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return storageState{}, fmt.Errorf("读取 storage state: %w", err)
	}
	var state storageState
	if err := json.Unmarshal(data, &state); err != nil {
		return storageState{}, fmt.Errorf("解析 storage state: %w", err)
	}
	return state, nil
}
