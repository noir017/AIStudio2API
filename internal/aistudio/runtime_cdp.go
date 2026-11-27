package aistudio

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/Mag1cFall/AIStudio2API/internal/cdpnative"
)

// cdpRuntime 是外部 CDP 浏览器后端的 workerRuntime，实现基础 workerRuntime、pageGenerator 与 functionContinuer
type cdpRuntime struct {
	*cdpnative.Worker
}

var _ workerRuntime = cdpRuntime{}
var _ pageGenerator = cdpRuntime{}
var _ functionContinuer = cdpRuntime{}

// Reload 重新载入外部浏览器中的 AI Studio 页面，返回同一 runtime
func (runtime cdpRuntime) Reload(ctx context.Context) (workerRuntime, error) {
	return runtime, runtime.Worker.Reload(ctx)
}

// SendProtected 在外部浏览器页面中以 XMLHttpRequest 发出写入 proof 后的完整正文
func (runtime cdpRuntime) SendProtected(ctx context.Context, rawURL string, headers http.Header, body protectedBody) (*camoufoxnative.ProtectedResponse, error) {
	return runtime.Worker.SendProtected(ctx, rawURL, headers, body.Bytes())
}

// MergeSetCookieHeaders 把账户在页面之外取得的响应 Cookie 写入外部浏览器
func (runtime cdpRuntime) MergeSetCookieHeaders(ctx context.Context, headers []string, requestURL string) error {
	var state StorageState
	if err := state.MergeSetCookieHeaders(headers, requestURL, time.Now()); err != nil {
		return err
	}
	encoded, err := json.Marshal(state.Cookies)
	if err != nil {
		return err
	}
	return runtime.Worker.SetCookies(ctx, encoded)
}

// ProtocolHeaders 返回页面的公共协议头，并附带由 GetLoggingContext 响应编码的 X-Goog-Ext-519733851-Bin
func (runtime cdpRuntime) ProtocolHeaders(ctx context.Context) (http.Header, error) {
	headers, err := runtime.Worker.ProtocolHeaders(ctx)
	if err != nil {
		return nil, err
	}
	encoded, err := encodeJSPBFields(runtime.PageCatalog().Logging)
	if err != nil {
		return nil, fmt.Errorf("编码官网日志上下文: %w", err)
	}
	headers.Set("X-Goog-Ext-519733851-Bin", base64.StdEncoding.EncodeToString(encoded))
	return headers, nil
}

// NewCDPWorker 连接外部 CDP 浏览器并创建单个账户的 WAA runtime
func NewCDPWorker(ctx context.Context, accountID string, options cdpnative.Options) (*NativeWorker, error) {
	runtime, err := cdpnative.Start(ctx, options)
	if err != nil {
		return nil, err
	}
	runtimeState := runtime.State()
	worker := &NativeWorker{
		accountID: accountID,
		runtime:   cdpRuntime{runtime},
		state: WorkerState{
			AccountID: accountID,
			Phase:     WorkerReady,
			PID:       runtimeState.PID,
			RuntimeID: "external-cdp",
			PageURL:   runtimeState.PageURL,
		},
	}
	pageWorkers.Store(accountID, worker)
	go func() {
		<-runtime.Done()
		if runtime.Err() != nil {
			worker.setPhase(WorkerFailed)
		}
	}()
	return worker, nil
}
