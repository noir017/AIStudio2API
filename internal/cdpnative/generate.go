package cdpnative

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf16"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/Mag1cFall/AIStudio2API/internal/waa/prooftime"
)

// 界面运行各步骤的等待上限（与 camoufoxnative 对齐）
var (
	chatAcquireTimeout = 2 * time.Minute
	inputLockTimeout   = 2 * time.Minute
	turnInputTimeout   = time.Minute
	pageSendTimeout    = 30 * time.Second
)

func reportStep(turn camoufoxnative.GenerateTurn, step camoufoxnative.GenerateStep) {
	if turn.Step != nil {
		turn.Step(step)
	}
}

func stepTimeout(step camoufoxnative.GenerateStep, limit time.Duration) error {
	return fmt.Errorf("界面运行停在%s，%s 内没有完成", step, limit)
}

// chatTab 是运行界面对话的外部浏览器标签页
type chatTab struct {
	targetID     string
	sessionID    string
	pageObjectID string
	// busy 表示标签页正在运行一轮对话
	busy bool
	// contents 是标签页上一轮发出的 GenerateContent 的 contents，页面没有对话、上一轮出错或流中断时为空
	contents []json.RawMessage
	// resetting 表示一轮对话结束后正在后台新开对话，或页面正在原标签页重新载入
	resetting bool
	// runHovered 表示后台新开对话后指针停在 Run 按钮上
	runHovered bool
	// unsent 表示输入框或函数响应输入框中留有已粘贴、页面没有发出的输入，下一轮全选后粘贴，原本要 Rerun 时先开新对话
	unsent bool
}

// chatAction 是一轮界面对话在标签页上的操作
type chatAction int

const (
	// chatContinue 在页面当前对话后输入提示词并点击 Run
	chatContinue chatAction = iota
	// chatNew 先点 New chat 并丢弃当前对话，再输入提示词并点击 Run
	chatNew
	// chatRerun 点击最后一轮的 Rerun
	chatRerun
)

func generateContents(body []byte) ([]json.RawMessage, error) {
	var fields []json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || len(fields) < 2 {
		return nil, fmt.Errorf("GenerateContent 正文缺少 contents: %v", err)
	}
	var contents []json.RawMessage
	if err := json.Unmarshal(fields[1], &contents); err != nil {
		return nil, fmt.Errorf("解析 GenerateContent contents: %w", err)
	}
	return contents, nil
}

func contentRole(content json.RawMessage) string {
	var fields []json.RawMessage
	var role string
	if json.Unmarshal(content, &fields) == nil && len(fields) > 1 {
		_ = json.Unmarshal(fields[1], &role)
	}
	return role
}

// nextChatAction 按标签页上一轮的 contents 判断本轮的操作：contents 相同时 Rerun，在上一轮后追加 model 与 user 两轮时继续当前对话，上一轮 contents 为空时直接输入，其余情况先开新对话
func nextChatAction(previous, contents []json.RawMessage) chatAction {
	if previous == nil {
		return chatContinue
	}
	equal := func(left, right []json.RawMessage) bool {
		if len(left) != len(right) {
			return false
		}
		for index := range left {
			if !bytes.Equal(left[index], right[index]) {
				return false
			}
		}
		return true
	}
	if equal(previous, contents) {
		return chatRerun
	}
	if len(contents) == len(previous)+2 && equal(previous, contents[:len(previous)]) && contentRole(contents[len(previous)]) == "model" {
		return chatContinue
	}
	return chatNew
}

// generateArmFunction 在 Runtime.callFunctionOn 中以 page 为 this，为下一次 Run/Rerun/提交安装一次性钩子
// 参数依次为请求 ID、digest、head、tail 与 CDP binding 名；页面发送的正文换成 head + proof + tail
const generateArmFunction = `function(id, digest, head, tail, bindingName) {
  const page = this;
  const emit = payload => {
    if (typeof window[bindingName] === "function") window[bindingName](payload);
  };
  const makerSuite = window.default_MakerSuite;
  const open = XMLHttpRequest.prototype.open;
  const send = XMLHttpRequest.prototype.send;
  const request = {xhr: null, credit: 8, resume: null};
  (page.requests ||= new Map()).set(id, request);
  let loaded = false, failure = "", wake = null;
  const guarded = ["pointerdown", "mousedown", "pointerup", "mouseup", "click"];
  let canceled = false, blocking = false;
  const guard = event => {
    if (!event.isTrusted) return;
    if (event.type === "pointerdown") blocking = canceled;
    if (blocking) {
      event.preventDefault();
      event.stopImmediatePropagation();
    }
    if (event.type === "click") unguard();
  };
  const unguard = () => guarded.forEach(type => removeEventListener(type, guard, true));
  guarded.forEach(type => addEventListener(type, guard, true));
  request.cancel = () => canceled = true;
  const changed = () => {
    wake?.();
    wake = null;
  };
  const next = () => new Promise(resolve => wake = resolve);
  makerSuite[page.key] = function(service) {
    makerSuite[page.key] = page.original;
    request.started = true;
    const timing = (` + proofTimingFunction + `)(service);
    return page.original.call(this, service, digest).then(proof => {
      if (failure) return proof;
      const {refresh, compute} = timing();
      XMLHttpRequest.prototype.open = function(method, url) {
        if (String(url).endsWith("` + generateContentPath + `")) {
          XMLHttpRequest.prototype.open = open;
          const xhr = this;
          request.xhr = xhr;
          xhr.send = function() {
            delete xhr.send;
            xhr.addEventListener("readystatechange", changed);
            xhr.addEventListener("progress", changed);
            xhr.addEventListener("load", () => {
              loaded = true;
              changed();
            });
            xhr.addEventListener("loadend", () => {
              loaded = true;
              changed();
            });
            xhr.addEventListener("error", () => {
              failure = "NetworkError when attempting to fetch resource.";
              changed();
            });
            xhr.addEventListener("abort", () => {
              failure = "AbortError: The operation was aborted.";
              changed();
              request.resume?.();
            });
            emit(JSON.stringify({sent: true, proof, refresh, compute}));
            return send.call(xhr, head + JSON.stringify(proof) + tail);
          };
        }
        return open.apply(this, arguments);
      };
      return proof;
    });
  };
  request.release = () => {
    unguard();
    if (request.xhr) return;
    if (makerSuite[page.key] !== page.original) makerSuite[page.key] = page.original;
    if (XMLHttpRequest.prototype.open !== open) XMLHttpRequest.prototype.open = open;
    failure = "AbortError: The operation was aborted.";
    changed();
  };
` + protectedPump + `}`

// generateCancelFunction 让页面拦下界面运行还没有按下的点击
const generateCancelFunction = `function(id) {
  const page = this;
  page.requests?.get(id)?.cancel();
}`

// Generate 在外部 CDP 浏览器标签页界面运行一轮 GenerateContent
func (worker *Worker) Generate(ctx context.Context, turn camoufoxnative.GenerateTurn) (*camoufoxnative.ProtectedResponse, error) {
	contents, err := generateContents(turn.Body)
	if err != nil {
		return nil, err
	}
	prompt, ok := camoufoxnative.GeneratePrompt(turn.Body)
	if !ok && turn.Result == "" {
		return nil, errors.New("GenerateContent 最后一个轮次不是带文本或函数结果的 user 轮次")
	}
	lockStarted := time.Now()
	reportStep(turn, camoufoxnative.StepAcquireTab)
	acquireCtx, cancel := context.WithTimeout(ctx, chatAcquireTimeout)
	defer cancel()
	var tab *chatTab
	if turn.Result != "" {
		tab, err = worker.acquireFunctionChat(contents)
	} else {
		tab, err = worker.acquireChat(acquireCtx)
	}
	if err != nil {
		if ctx.Err() == nil && acquireCtx.Err() != nil {
			err = fmt.Errorf("%w: %w", stepTimeout(camoufoxnative.StepAcquireTab, chatAcquireTimeout), err)
		}
		return nil, err
	}
	prooftime.Observe(ctx, prooftime.Lock, time.Since(lockStarted))
	response, err := worker.generate(ctx, tab, turn, contents, prompt)
	if err != nil {
		worker.releaseChat(tab, false)
	}
	return response, err
}

func (worker *Worker) generate(ctx context.Context, tab *chatTab, turn camoufoxnative.GenerateTurn, contents []json.RawMessage, prompt string) (*camoufoxnative.ProtectedResponse, error) {
	client := worker.client
	requestID := rand.Text()
	bindingName := "__cdp_gen_" + requestID
	channel, err := client.openBinding(ctx, tab.sessionID, bindingName)
	if err != nil {
		return nil, fmt.Errorf("界面运行 GenerateContent: %w", err)
	}
	inputStarted := time.Now()
	reportStep(turn, camoufoxnative.StepLockInput)
	if err := worker.lockInputWithin(ctx, inputLockTimeout); err != nil {
		client.closeBinding(tab.sessionID, bindingName)
		if ctx.Err() == nil {
			err = stepTimeout(camoufoxnative.StepLockInput, inputLockTimeout)
		}
		return nil, err
	}
	locked := time.Now()
	prooftime.Observe(ctx, prooftime.Lock, locked.Sub(inputStarted))
	turned := make(chan error, 1)
	go func() {
		defer worker.inputMu.Unlock()
		turnCtx, cancel := context.WithTimeoutCause(ctx, turnInputTimeout, errTurnInputTimeout)
		current := camoufoxnative.StepSelectTab
		err := worker.startTurn(turnCtx, tab, turn, contents, prompt, requestID, bindingName, func(step camoufoxnative.GenerateStep) {
			current = step
			reportStep(turn, step)
		})
		if err != nil && context.Cause(turnCtx) == errTurnInputTimeout {
			err = fmt.Errorf("%w: %w", stepTimeout(current, turnInputTimeout), err)
		}
		cancel()
		turned <- err
	}()
	sent, err := worker.waitSent(ctx, tab, turn, requestID, channel, turned)
	if err != nil {
		prooftime.Observe(ctx, prooftime.Interaction, time.Since(locked))
		return nil, worker.abandonProtectedRequest(ctx, tab.sessionID, tab.pageObjectID, requestID, bindingName, err)
	}
	reportStep(turn, camoufoxnative.StepSent)
	sent.observe(ctx, locked)
	worker.mu.Lock()
	tab.contents = contents
	tab.unsent = false
	worker.mu.Unlock()
	status, header, err := readResponseHead(ctx, channel)
	if err != nil {
		// 读响应头失败时及时清空 tab.contents，避免下一轮误判为可 Rerun 或可续接函数调用
		worker.mu.Lock()
		tab.contents = nil
		worker.mu.Unlock()
		client.closeBinding(tab.sessionID, bindingName)
		return nil, errors.Join(err, worker.stopTurn(tab))
	}
	if status != http.StatusOK {
		worker.mu.Lock()
		tab.contents = nil
		worker.mu.Unlock()
	}
	bodyCtx, cancelBody := context.WithCancel(ctx)
	return &camoufoxnative.ProtectedResponse{
		StatusCode: status,
		Header:     header,
		Proof:      sent.Proof,
		Body: &protectedResponseBody{
			ctx:          bodyCtx,
			cancel:       cancelBody,
			worker:       worker,
			sessionID:    tab.sessionID,
			pageObjectID: tab.pageObjectID,
			requestID:    requestID,
			bindingName:  bindingName,
			channel:      channel,
			stop:         func() error { return worker.stopTurn(tab) },
			release: func(completed, call bool) {
				if !completed {
					// 流中断或出错时及时清空 tab.contents，避免残留状态污染下一轮
					worker.mu.Lock()
					tab.contents = nil
					worker.mu.Unlock()
				}
				worker.releaseChat(tab, completed && status == http.StatusOK && !call)
			},
		},
	}, nil
}

var errTurnInputTimeout = errors.New("界面输入超时")

func (worker *Worker) startTurn(ctx context.Context, tab *chatTab, turn camoufoxnative.GenerateTurn, contents []json.RawMessage, prompt string, requestID, bindingName string, step func(camoufoxnative.GenerateStep)) error {
	client := worker.client
	input := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		input, cancel = context.WithDeadline(input, deadline)
		defer cancel()
	}
	step(camoufoxnative.StepSelectTab)
	if err := worker.selectChat(input, tab); err != nil {
		return err
	}
	if err := client.waitFor(ctx, tab.sessionID, `!`+generationRunningExpression, 15*time.Second); err != nil {
		return fmt.Errorf("等待上一轮生成结束: %w", err)
	}
	if _, err := dismissKnownOverlays(input, client, tab.sessionID); err != nil {
		return err
	}
	worker.mu.Lock()
	action := nextChatAction(tab.contents, contents)
	hovered := tab.runHovered
	tab.runHovered = false
	if tab.unsent && action == chatRerun {
		action = chatNew
	}
	worker.mu.Unlock()
	if turn.Result != "" {
		return worker.submitResult(ctx, input, tab, turn, requestID, bindingName, step)
	}
	if action == chatNew {
		hovered = false
		if err := ctx.Err(); err != nil {
			return err
		}
		step(camoufoxnative.StepNewChat)
		if err := worker.newChat(input, tab); err != nil {
			return err
		}
	}
	if action != chatRerun {
		if err := ctx.Err(); err != nil {
			return err
		}
		step(camoufoxnative.StepPastePrompt)
		focused, err := client.evaluateBool(input, tab.sessionID, promptFocusedExpression)
		if err != nil {
			return fmt.Errorf("检查官网输入框焦点: %w", err)
		}
		if !focused {
			hovered = false
			if err := worker.click(input, tab.sessionID, promptBoxSelector, "", "官网输入框", pointerMoveDuration); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		worker.mu.Lock()
		unsent := tab.unsent
		tab.unsent = true
		worker.mu.Unlock()
		if unsent {
			if err := worker.selectAll(input, tab.sessionID); err != nil {
				return fmt.Errorf("全选官网输入框中未发出的输入: %w", err)
			}
		}
		if err := worker.pastePrompt(input, tab.sessionID, promptHead(prompt)); err != nil {
			return err
		}
		step(camoufoxnative.StepWaitRunnable)
		if err := worker.waitRunnable(ctx, tab); err != nil {
			return fmt.Errorf("等待官网 Run 按钮启用: %w", err)
		}
	}
	if err := worker.arm(ctx, tab, turn, requestID, bindingName); err != nil {
		return err
	}
	if action == chatRerun {
		step(camoufoxnative.StepClickRerun)
		if _, err := worker.pointer(input, tab.sessionID, lastTurnSelector, "", false, pointerMoveDuration); err != nil {
			return fmt.Errorf("指针移到最后一轮: %w", err)
		}
		if err := client.waitFor(ctx, tab.sessionID, `Boolean(document.querySelector('`+rerunButtonSelector+`'))`, 5*time.Second); err != nil {
			return fmt.Errorf("等待官网 Rerun 按钮: %w", err)
		}
		return worker.clickArmed(ctx, tab, requestID, rerunButtonSelector, "官网 Rerun 按钮", pointerMoveDuration)
	}
	move := pointerMoveDuration
	if hovered {
		move = 0
	}
	step(camoufoxnative.StepClickRun)
	return worker.clickArmed(ctx, tab, requestID, runButtonSelector, "官网 Run 按钮", move)
}

func (worker *Worker) clickArmed(ctx context.Context, tab *chatTab, requestID, selector, name string, move int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var cancelErr error
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(canceled)
		cancelCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := worker.client.callFunctionOn(cancelCtx, tab.sessionID, tab.pageObjectID, generateCancelFunction, []any{requestID}, true, false); err != nil {
			cancelErr = fmt.Errorf("让页面拦下点击%s: %w", name, err)
		}
	})
	err := worker.click(context.WithoutCancel(ctx), tab.sessionID, selector, "", name, move)
	if !stop() {
		<-canceled
		return errors.Join(err, ctx.Err(), cancelErr)
	}
	return err
}

func (worker *Worker) arm(ctx context.Context, tab *chatTab, turn camoufoxnative.GenerateTurn, requestID, bindingName string) error {
	head, tail := string(turn.Body[:turn.ProofStart]), string(turn.Body[turn.ProofEnd:])
	if _, err := worker.client.callFunctionOn(ctx, tab.sessionID, tab.pageObjectID, generateArmFunction, []any{
		requestID, turn.Digest, head, tail, bindingName,
	}, true, false); err != nil {
		return fmt.Errorf("安装界面运行钩子: %w", err)
	}
	return nil
}

// promptFillLimit 是界面运行写入官网输入框的最多 UTF-16 码元数，与官网把更长的粘贴转为文本附件的长度相同
const promptFillLimit = 25000

func promptHead(prompt string) string {
	units := 0
	for index, char := range prompt {
		units += utf16.RuneLen(char)
		if units > promptFillLimit {
			return prompt[:index]
		}
	}
	return prompt
}

func (worker *Worker) waitRunnable(ctx context.Context, tab *chatTab) error {
	for checks := 1; ; checks++ {
		state, err := readRunState(ctx, worker.client, tab.sessionID, tab.pageObjectID, "")
		if err != nil {
			return err
		}
		if state.Enabled {
			return nil
		}
		if len(state.Dialogs) > 0 || state.Cover != "" {
			clicked, err := dismissKnownOverlays(ctx, worker.client, tab.sessionID)
			if err != nil {
				return err
			}
			if clicked > 0 {
				checks = 0
			}
		}
		if checks == runSettleChecks {
			return fmt.Errorf("页面空闲后仍不可点: %s", state.reason())
		}
		if err := waitContext(ctx, pagePollInterval); err != nil {
			return err
		}
	}
}

var errInputEnded = errors.New("界面输入已结束")

func (worker *Worker) waitSent(ctx context.Context, tab *chatTab, turn camoufoxnative.GenerateTurn, requestID string, channel *cdpChannel, turned <-chan error) (protectedMessage, error) {
	inputCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var inputErr error
	ended := make(chan struct{})
	go func() {
		inputErr = <-turned
		close(ended)
		cancel(errInputEnded)
	}()
	message, err := readProtectedMessage(inputCtx, channel)
	if err == nil {
		sent, err := sentProof(message)
		if err != nil {
			<-ended
			return protectedMessage{}, fmt.Errorf("等待页面应用发出 GenerateContent: %w", err)
		}
		return sent, nil
	}
	<-ended
	if inputErr != nil {
		return protectedMessage{}, inputErr
	}
	if context.Cause(inputCtx) != errInputEnded {
		return protectedMessage{}, fmt.Errorf("等待页面应用发出 GenerateContent: %w", err)
	}
	reportStep(turn, camoufoxnative.StepWaitSent)
	sent, err := worker.waitStarted(ctx, tab, requestID, channel)
	if err != nil {
		return protectedMessage{}, fmt.Errorf("等待页面应用发出 GenerateContent: %w", err)
	}
	return sent, nil
}

func (worker *Worker) waitStarted(ctx context.Context, tab *chatTab, requestID string, channel *cdpChannel) (protectedMessage, error) {
	for checks := 1; ; checks++ {
		pollCtx, cancel := context.WithTimeout(ctx, pagePollInterval)
		message, err := readProtectedMessage(pollCtx, channel)
		cancel()
		if err != nil && (ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded)) {
			return protectedMessage{}, err
		}
		if err == nil {
			return sentProof(message)
		}
		state, err := readRunState(ctx, worker.client, tab.sessionID, tab.pageObjectID, requestID)
		if err != nil {
			return protectedMessage{}, err
		}
		if state.Started || state.Running {
			sendCtx, cancel := context.WithTimeout(ctx, pageSendTimeout)
			message, err := readProtectedMessage(sendCtx, channel)
			cancel()
			if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return protectedMessage{}, fmt.Errorf("%w: 页面已开始运行，官网取 proof 的函数已调用=%t", stepTimeout(camoufoxnative.StepWaitSent, pageSendTimeout), state.Started)
			}
			if err != nil {
				return protectedMessage{}, err
			}
			return sentProof(message)
		}
		if len(state.Dialogs) > 0 || checks == runSettleChecks {
			return protectedMessage{}, fmt.Errorf("点击 Run 后页面没有开始运行: %s", state.reason())
		}
	}
}

func sentProof(message protectedMessage) (protectedMessage, error) {
	if message.Error != "" {
		return protectedMessage{}, errors.New(message.Error)
	}
	if !message.Sent {
		return protectedMessage{}, errors.New("页面应用没有发出 GenerateContent")
	}
	return message, nil
}

func (worker *Worker) newChat(ctx context.Context, tab *chatTab) error {
	client := worker.client
	if err := worker.click(ctx, tab.sessionID, newChatButtonSelector, "", "官网 New chat 按钮", pointerMoveDuration); err != nil {
		return err
	}
	discard := fmt.Sprintf(`[...document.querySelectorAll('button')].some(node => node.checkVisibility({visibilityProperty: true}) && new RegExp(%q, 'i').test(node.innerText.trim()))`, discardButtonPattern)
	if err := client.waitFor(ctx, tab.sessionID, discard, 5*time.Second); err != nil {
		return fmt.Errorf("等待官网新对话确认: %w", err)
	}
	if err := worker.click(ctx, tab.sessionID, "button", discardButtonPattern, "官网 Discard and continue 按钮", pointerMoveDuration); err != nil {
		return err
	}
	if err := client.waitFor(ctx, tab.sessionID, chatEmptyExpression, 10*time.Second); err != nil {
		return fmt.Errorf("等待官网新对话: %w", err)
	}
	if err := client.waitFor(ctx, tab.sessionID, temporaryChatExpression, 10*time.Second); err != nil {
		return fmt.Errorf("等待官网临时对话: %w", err)
	}
	worker.mu.Lock()
	tab.contents = nil
	tab.unsent = false
	worker.mu.Unlock()
	return nil
}

func (worker *Worker) stopTurn(tab *chatTab) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	worker.lockInput()
	defer worker.inputMu.Unlock()
	if err := worker.selectChat(ctx, tab); err != nil {
		return err
	}
	running, err := worker.client.evaluateBool(ctx, tab.sessionID, generationRunningExpression)
	if err != nil || !running {
		return err
	}
	return worker.click(ctx, tab.sessionID, runButtonSelector, "", "官网 Stop 按钮", pointerMoveDuration)
}

func (worker *Worker) click(ctx context.Context, sessionID, selector, pattern, name string, move int) error {
	found, err := worker.pointer(ctx, sessionID, selector, pattern, true, move)
	if err != nil {
		return fmt.Errorf("点击%s: %w", name, err)
	}
	if !found {
		return fmt.Errorf("%s不存在", name)
	}
	return nil
}

func (worker *Worker) acquireChat(ctx context.Context) (*chatTab, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	waited := false
	for {
		if worker.closed {
			return nil, errors.New("外部 CDP runtime 已关闭")
		}
		if err := worker.client.err(); err != nil {
			return nil, err
		}
		resetting := false
		for _, tab := range worker.chats {
			if !tab.busy {
				tab.busy = true
				worker.useTab(tab.sessionID, tab.targetID)
				return tab, nil
			}
			resetting = resetting || tab.resetting
		}
		if !resetting && !worker.chatOpening {
			if waited && worker.chatOpenErr != nil {
				return nil, worker.chatOpenErr
			}
			worker.openChat()
		}
		if worker.chatsChanged == nil {
			worker.chatsChanged = make(chan struct{})
		}
		changed := worker.chatsChanged
		done := worker.client.done
		worker.mu.Unlock()
		select {
		case <-changed:
		case <-done:
			worker.mu.Lock()
			if err := worker.client.err(); err != nil {
				return nil, err
			}
			return nil, ErrDisconnected
		case <-ctx.Done():
			worker.mu.Lock()
			return nil, ctx.Err()
		}
		worker.mu.Lock()
		waited = true
	}
}

func (worker *Worker) notifyChats() {
	if worker.chatsChanged != nil {
		close(worker.chatsChanged)
		worker.chatsChanged = nil
	}
}

func (worker *Worker) openChat() {
	worker.chatOpening, worker.chatOpenErr = true, nil
	options := worker.options
	options.StartupProgress = nil
	go func() {
		tab, err := worker.loadChat(context.Background(), options)
		worker.mu.Lock()
		worker.chatOpening, worker.chatOpenErr = false, err
		closed := worker.closed
		if err == nil && !closed {
			worker.chats = append(worker.chats, tab)
		}
		worker.notifyChats()
		worker.mu.Unlock()
		if err == nil && closed {
			_ = worker.closeTarget(tab.targetID)
		}
	}()
}

func (worker *Worker) loadChat(ctx context.Context, options Options) (*chatTab, error) {
	targetID, sessionID, err := worker.createAndAttachTab(ctx)
	if err != nil {
		return nil, err
	}
	worker.lockInput()
	err = worker.activateTarget(ctx, targetID)
	worker.inputMu.Unlock()
	if err != nil {
		return nil, errors.Join(err, worker.closeTarget(targetID))
	}
	loaded, err := worker.openPage(ctx, options, sessionID)
	if err != nil {
		return nil, errors.Join(err, worker.closeTarget(targetID))
	}
	return &chatTab{targetID: targetID, sessionID: sessionID, pageObjectID: loaded.pageObjectID}, nil
}

func (worker *Worker) releaseChat(tab *chatTab, completed bool) {
	worker.mu.Lock()
	reset := completed && !worker.closed
	if reset {
		tab.resetting = true
	} else {
		tab.busy = false
		worker.notifyChats()
	}
	worker.mu.Unlock()
	if !reset {
		worker.finishRequest(tab.sessionID)
		return
	}
	go worker.resetChat(tab)
}

func (worker *Worker) resetChat(tab *chatTab) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := worker.clearChat(ctx, tab)
	cancel()
	worker.mu.Lock()
	closed := worker.closed
	tab.busy, tab.resetting = false, false
	worker.notifyChats()
	worker.mu.Unlock()
	worker.finishRequest(tab.sessionID)
	if err != nil && !closed {
		slog.Warn("外部 CDP 标签页新开对话失败", "target", tab.targetID, "error", err)
	}
}

func (worker *Worker) clearChat(ctx context.Context, tab *chatTab) error {
	worker.lockInputIdle()
	defer worker.inputMu.Unlock()
	worker.mu.Lock()
	closed := worker.closed
	worker.mu.Unlock()
	if closed {
		return errors.New("外部 CDP runtime 已关闭")
	}
	if err := worker.selectChat(ctx, tab); err != nil {
		return err
	}
	if err := worker.client.waitFor(ctx, tab.sessionID, `!`+generationRunningExpression, 15*time.Second); err != nil {
		return fmt.Errorf("等待上一轮生成结束: %w", err)
	}
	if _, err := dismissKnownOverlays(ctx, worker.client, tab.sessionID); err != nil {
		return err
	}
	if err := worker.newChat(ctx, tab); err != nil {
		return err
	}
	if _, err := worker.pointer(ctx, tab.sessionID, runButtonSelector, "", false, pointerMoveDuration); err != nil {
		return fmt.Errorf("指针移到官网 Run 按钮: %w", err)
	}
	worker.mu.Lock()
	tab.runHovered = true
	worker.mu.Unlock()
	return nil
}

func (worker *Worker) selectChat(ctx context.Context, tab *chatTab) error {
	worker.mu.Lock()
	selected := worker.selected
	worker.mu.Unlock()
	if selected == tab.targetID {
		return nil
	}
	if err := worker.client.call(ctx, "", "Target.activateTarget", map[string]any{"targetId": tab.targetID}, nil); err != nil {
		return fmt.Errorf("选中标签页: %w", err)
	}
	if err := worker.client.waitFor(ctx, tab.sessionID, `document.visibilityState === 'visible'`, 10*time.Second); err != nil {
		return fmt.Errorf("等待标签页可见: %w", err)
	}
	worker.mu.Lock()
	worker.selected = tab.targetID
	worker.mu.Unlock()
	return nil
}
