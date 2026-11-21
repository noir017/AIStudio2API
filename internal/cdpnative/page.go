package cdpnative

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/waa/prooftime"
	"github.com/Mag1cFall/AIStudio2API/internal/waa/rhythm"
)

const pageDOMHelpers = `
  const visible = element => element.checkVisibility({visibilityProperty: true});
  const uniqueVisible = (selector, label) => {
    const items = [...document.querySelectorAll(selector)].filter(visible);
    if (items.length > 1) throw new Error(label + ' 匹配多个可见目标');
    return items[0];
  };
  const buttonEnabled = button => !button.matches(':disabled') && !button.closest('[aria-disabled="true"]');
`

const promptReadyExpression = `(() => {` + pageDOMHelpers + `
  return Boolean(uniqueVisible('ms-prompt-box textarea', '提示词输入框'));
})()`

const workerPageReadyExpression = `(location.hostname === 'accounts.google.com' || ` + promptReadyExpression + `)`

const loggingContextReadyExpression = `performance.getEntriesByType('resource').some(entry => entry.name.endsWith('/GetLoggingContext'))`

const dismissOverlaysExpression = `(() => {` + pageDOMHelpers + `
  const selectors = [
    'ms-g1-welcome-dialog button',
    'button.xap-tour-exit-button',
    '#glue-cookie-notification-bar-1 .glue-cookie-notification-bar__reject',
    '#glue-cookie-notification-bar-1:not(:has(.glue-cookie-notification-bar__reject)) .glue-cookie-notification-bar__accept'
  ];
  let clicked = 0;
  for (const selector of selectors) {
    const button = uniqueVisible(selector, '启动弹层按钮');
    if (button && buttonEnabled(button)) {
      button.click();
      clicked++;
    }
  }
  return clicked;
})()`

const fillPromptStatements = pageDOMHelpers + `
  const textarea = uniqueVisible('ms-prompt-box textarea', '提示词输入框');
  if (!textarea) throw new Error('提示词输入框不存在');
  const setValue = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value').set;
  setValue.call(textarea, prompt);
  textarea.dispatchEvent(new InputEvent('input', {bubbles: true, inputType: 'insertText', data: prompt}));
  textarea.dispatchEvent(new Event('change', {bubbles: true}));
  const synced = textarea.value === prompt;
`

const proofTimingFunction = `service => {
  const started = performance.now();
  let refreshed = started;
  const done = () => { refreshed = performance.now(); };
  const vm = Object.values(service).find(value => typeof value?.snapshot === 'function');
  for (const wait of [...Object.values(service), ...Object.values(vm ?? {}).map(value => value?.promise)]) {
    if (typeof wait?.then === 'function') wait.then(done, done);
  }
  return () => ({refresh: refreshed - started, compute: performance.now() - refreshed});
}`

// proofFunction 在 Runtime.callFunctionOn 中以 page 对象为 this，写入提示词并调用官网 snapshot 函数
const proofFunction = `async function(prompt, digest) {
  const page = this;
  if (!page.service) throw new Error('官方 WAA service 尚未就绪');` + fillPromptStatements + `  try {
    if (!synced) return null;
    const timing = (` + proofTimingFunction + `)(page.service);
    const proof = await page.snapshot(digest);
    return JSON.stringify({proof, ...timing()});
  } finally {
    setValue.call(textarea, '');
    textarea.dispatchEvent(new InputEvent('input', {bubbles: true, inputType: 'deleteContent'}));
  }
}`

type pageProofTiming struct {
	Refresh float64 `json:"refresh"`
	Compute float64 `json:"compute"`
}

func (timing pageProofTiming) observe(ctx context.Context, started time.Time) {
	refresh := time.Duration(timing.Refresh * float64(time.Millisecond))
	compute := time.Duration(timing.Compute * float64(time.Millisecond))
	prooftime.Observe(ctx, prooftime.Refresh, refresh)
	prooftime.Observe(ctx, prooftime.Compute, compute)
	prooftime.Observe(ctx, prooftime.Interaction, time.Since(started)-refresh-compute)
}

func normalizePromptNewlines(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "\r\n", "\n"), "\r", "\n")
}

// buildServicePreloadScript 构造在页面脚本之前执行的预加载脚本：
// 首次调用 WAA service 的 getMetadata 时恢复原方法，将实例暂存到一次性 Symbol 并经 CDP binding 通知 Go 侧提取对象句柄
func buildServicePreloadScript(origin, bindingName, symbolKey string) string {
	encodedOrigin, _ := json.Marshal(origin)
	encodedBinding, _ := json.Marshal(bindingName)
	encodedSymbol, _ := json.Marshal(symbolKey)
	return fmt.Sprintf(`(() => {
  if (window !== window.top || location.origin !== %s || document.readyState !== 'loading') return;
  const bindingName = %s;
  const sym = Symbol.for(%s);
  Object.defineProperty(window, 'default_MakerSuite', {configurable: true, enumerable: true, get: () => undefined, set: namespace => {
    Object.defineProperty(window, 'default_MakerSuite', {value: namespace, writable: true, enumerable: true, configurable: true});
    queueMicrotask(() => {
      for (const key of Object.keys(namespace)) {
        let prototype, original;
        try {
          prototype = typeof namespace[key] === 'function' ? namespace[key].prototype : null;
          original = prototype && Object.getOwnPropertyDescriptor(prototype, 'getMetadata')?.value;
          if (typeof original !== 'function' || typeof prototype.initialize !== 'function' || !String(original).includes('X-Goog-Api-Key')) continue;
        } catch (_) {
          continue;
        }
        prototype.getMetadata = function() {
          prototype.getMetadata = original;
          window[sym] = this;
          if (typeof window[bindingName] === 'function') window[bindingName]('ready');
          return original.apply(this, arguments);
        };
        return;
      }
    });
  }});
})();`, encodedOrigin, encodedBinding, encodedSymbol)
}

// buildPageObjectExpression 从一次性 Symbol 取出 WAA service 实例、立即清理 Symbol，并构造 Worker 持有的页面对象
func buildPageObjectExpression(symbolKey string) string {
	encodedSymbol, _ := json.Marshal(symbolKey)
	return fmt.Sprintf(`(() => {
  const sym = Symbol.for(%s);
  const service = window[sym];
  delete window[sym];
  if (!service) return null;
  const makerSuite = window.default_MakerSuite;
  if (!makerSuite) return null;
  for (const key of Object.keys(makerSuite)) {
    let original;
    try {
      original = makerSuite[key];
      if (typeof original !== 'function') continue;
      const source = original.toString();
      if (!source.includes('.snapshot({') || !source.includes('content') || !source.includes('yield')) continue;
    } catch (_) {
      continue;
    }
    return {key, original, service, snapshot: digest => original.call(makerSuite, service, digest)};
  }
  return null;
})()`, encodedSymbol)
}

func buildLocalStoragePreloadScript(origins []storageOrigin) (string, error) {
	values := make(map[string]map[string]string, len(origins))
	for _, origin := range origins {
		items := make(map[string]string, len(origin.LocalStorage))
		for _, item := range origin.LocalStorage {
			items[item.Name] = item.Value
		}
		values[origin.Origin] = items
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`(() => {
  const all = %s;
  const current = all[location.origin];
  if (!current) return;
  for (const [name, value] of Object.entries(current)) localStorage.setItem(name, value);
})();`, encoded), nil
}

func dismissKnownOverlays(ctx context.Context, client *cdpClient, sessionID string) (int, error) {
	obj, err := client.evaluate(ctx, sessionID, dismissOverlaysExpression, true, true)
	if err != nil {
		return 0, fmt.Errorf("处理 AI Studio 启动覆盖层: %w", err)
	}
	if count, ok := obj.Value.(float64); ok {
		return int(count), nil
	}
	return 0, nil
}

const (
	// promptBoxSelector 是提示词输入框
	promptBoxSelector = "ms-prompt-box textarea"
	// runButtonSelector 是生成前显示 Run、生成中显示 Stop 的按钮
	runButtonSelector = "ms-run-button button"
	// newChatButtonSelector 是顶栏的 New chat 按钮
	newChatButtonSelector = `ms-playground-toolbar button[aria-label="New chat"]`
	// discardButtonPattern 是 New chat 确认弹窗中丢弃当前对话的按钮文字
	discardButtonPattern = "^Discard and continue$"
	// lastTurnSelector 是最后一个对话轮次，指针悬停其上时显示 Rerun 按钮
	lastTurnSelector = "ms-chat-turn:last-of-type"
	// rerunButtonSelector 是最后一个对话轮次的 Rerun 按钮
	rerunButtonSelector = `ms-chat-turn:last-of-type button[aria-label^="Rerun" i]`
)

// generationRunningExpression 检查官网 Run 按钮已切换为 Stop（按钮内出现 .spin）
const generationRunningExpression = `(() => {` + pageDOMHelpers + `
  const button = uniqueVisible('ms-run-button button', '官网 Run 按钮');
  return Boolean(button && button.querySelector('.spin'));
})()`

// promptFocusedExpression 检查输入框已获得焦点，官网新开对话后焦点在输入框中
const promptFocusedExpression = `Boolean(document.activeElement && document.activeElement.matches('` + promptBoxSelector + `'))`

// chatEmptyExpression 检查页面没有对话轮次
const chatEmptyExpression = `document.querySelectorAll('ms-chat-turn').length === 0`

// temporaryChatExpression 检查页面显示临时对话标识
const temporaryChatExpression = `[...document.querySelectorAll('ms-incognito-mode-indicator')].some(node => node.checkVisibility({visibilityProperty: true}))`

// promptValueFunction 返回可见提示框的当前值
const promptValueFunction = `() => {` + pageDOMHelpers + `
  const textarea = uniqueVisible('ms-prompt-box textarea', '提示词输入框');
  if (!textarea) throw new Error('提示词输入框不存在');
  return textarea.value;
}`

// pointerTargetFunction 把 selector 匹配且文字或 aria-label 匹配 pattern 的第一个可见元素滚动到视口内，返回其中心坐标 JSON，没有时返回空字符串
const pointerTargetFunction = `(selector, pattern) => {
  const expression = new RegExp(pattern, 'i');
  const item = [...document.querySelectorAll(selector)].find(node => node.checkVisibility({visibilityProperty: true}) && expression.test((node.innerText || node.getAttribute('aria-label') || '').trim()));
  if (!item) return '';
  item.scrollIntoView({block: 'nearest', inline: 'nearest'});
  const box = item.getBoundingClientRect();
  return JSON.stringify({x: Math.round(box.left + box.width / 2), y: Math.round(box.top + box.height / 2)});
}`

// runStateFunction 在 Runtime.callFunctionOn 中以 page 为 this，等待已排队任务后读取官网 Run 按钮状态
const runStateFunction = `async function(id) {
  const page = this;` + pageDOMHelpers + `
  await new Promise(resolve => setTimeout(resolve));
  const describe = node => (node?.getAttribute('aria-describedby') || '').split(' ').map(name => document.getElementById(name)?.textContent.trim()).filter(Boolean).join(' ');
  const texts = selector => [...document.querySelectorAll(selector)].filter(visible).map(node => node.innerText.trim()).filter(Boolean);
  const button = uniqueVisible('ms-run-button button', '官网 Run 按钮');
  const box = document.querySelector('ms-prompt-box .prompt-box-container');
  let cover = '';
  if (button) {
    const rect = button.getBoundingClientRect();
    const top = document.elementFromPoint(rect.left + rect.width / 2, rect.top + rect.height / 2);
    if (top && !button.contains(top)) cover = (top.innerText || top.getAttribute('aria-label') || top.tagName).trim();
  }
  return JSON.stringify({
    found: Boolean(button), enabled: Boolean(button && buttonEnabled(button)) && !cover, running: Boolean(button?.querySelector('.spin')),
    started: Boolean(page.requests?.get(id)?.started),
    tooltip: describe(button), boxTooltip: box?.classList.contains('disabled') ? describe(box) : '',
    dialogs: texts('mat-dialog-container'), notices: texts('mat-snack-bar-container'), cover,
    prompt: document.querySelector('ms-prompt-box textarea')?.value.length ?? -1
  });
}`

type runState struct {
	Found      bool     `json:"found"`
	Enabled    bool     `json:"enabled"`
	Running    bool     `json:"running"`
	Started    bool     `json:"started"`
	Tooltip    string   `json:"tooltip"`
	BoxTooltip string   `json:"boxTooltip"`
	Dialogs    []string `json:"dialogs"`
	Notices    []string `json:"notices"`
	Cover      string   `json:"cover"`
	Prompt     int      `json:"prompt"`
}

func readRunState(ctx context.Context, client *cdpClient, sessionID, pageObjectID, id string) (runState, error) {
	var state runState
	value, err := client.callFunctionOnString(ctx, sessionID, pageObjectID, runStateFunction, []any{id}, true)
	if err != nil {
		return state, fmt.Errorf("读取官网 Run 按钮状态: %w", err)
	}
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return state, fmt.Errorf("解析官网 Run 按钮状态: %w", err)
	}
	return state, nil
}

func (state runState) reason() string {
	var parts []string
	if !state.Found {
		parts = append(parts, "Run 按钮不存在")
	}
	for _, dialog := range state.Dialogs {
		parts = append(parts, fmt.Sprintf("对话框 %q", dialog))
	}
	if state.Cover != "" {
		parts = append(parts, fmt.Sprintf("遮挡 Run 按钮的元素 %q", state.Cover))
	}
	if state.BoxTooltip != "" {
		parts = append(parts, fmt.Sprintf("输入框提示 %q", state.BoxTooltip))
	}
	if state.Tooltip != "" {
		parts = append(parts, fmt.Sprintf("Run 按钮提示 %q", state.Tooltip))
	}
	for _, notice := range state.Notices {
		parts = append(parts, fmt.Sprintf("提示条 %q", notice))
	}
	parts = append(parts, fmt.Sprintf("输入框 %d 字", state.Prompt))
	return strings.Join(parts, " | ")
}

// runSettleChecks 是页面空闲时 Run 按钮持续不可点、点击后仍没有开始运行时判定失败所需的连续检查次数
var runSettleChecks = 40

// pointerMoveDuration 是可信指针从别处移到目标元素所用的毫秒数
const pointerMoveDuration = 120

// pointer 以 CDP Input.dispatchMouseEvent 移到匹配元素中心，click 为真时按账户节律停顿并按下/抬起左键
func (worker *Worker) pointer(ctx context.Context, sessionID, selector, pattern string, click bool, move int) (bool, error) {
	encodedSelector, _ := json.Marshal(selector)
	encodedPattern, _ := json.Marshal(pattern)
	expr := fmt.Sprintf("(%s)(%s, %s)", pointerTargetFunction, encodedSelector, encodedPattern)
	located, err := worker.client.evaluateString(ctx, sessionID, expr)
	if err != nil || located == "" {
		return false, err
	}
	var target struct {
		X float64 `json:"x"`
		Y float64 `json:"y"`
	}
	if err := json.Unmarshal([]byte(located), &target); err != nil {
		return false, err
	}
	if err := worker.client.call(ctx, sessionID, "Input.dispatchMouseEvent", map[string]any{
		"type": "mouseMoved",
		"x":    target.X,
		"y":    target.Y,
	}, nil); err != nil {
		return true, err
	}
	if !click {
		return true, nil
	}
	if delay := rhythm.Duration(worker.rhythm.Press); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return true, err
		}
	}
	if err := worker.client.call(ctx, sessionID, "Input.dispatchMouseEvent", map[string]any{
		"type":       "mousePressed",
		"x":          target.X,
		"y":          target.Y,
		"button":     "left",
		"buttons":    1,
		"clickCount": 1,
	}, nil); err != nil {
		return true, err
	}
	if delay := rhythm.Duration(worker.rhythm.Hold); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return true, err
		}
	}
	if err := worker.client.call(ctx, sessionID, "Input.dispatchMouseEvent", map[string]any{
		"type":       "mouseReleased",
		"x":          target.X,
		"y":          target.Y,
		"button":     "left",
		"buttons":    0,
		"clickCount": 1,
	}, nil); err != nil {
		return true, err
	}
	return true, nil
}

// selectAll 通过 CDP Input.dispatchKeyEvent 执行可信 Ctrl+A（含 selectAll 编辑命令，跨平台生效）
func (worker *Worker) selectAll(ctx context.Context, sessionID string) error {
	if err := worker.client.call(ctx, sessionID, "Input.dispatchKeyEvent", map[string]any{
		"type":                  "rawKeyDown",
		"key":                   "Control",
		"code":                  "ControlLeft",
		"windowsVirtualKeyCode": 17,
		"modifiers":             2,
	}, nil); err != nil {
		return err
	}
	if delay := rhythm.Duration(worker.rhythm.Control); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return err
		}
	}
	if err := worker.client.call(ctx, sessionID, "Input.dispatchKeyEvent", map[string]any{
		"type":                  "rawKeyDown",
		"key":                   "a",
		"code":                  "KeyA",
		"windowsVirtualKeyCode": 65,
		"modifiers":             2,
		"commands":              []string{"selectAll"},
	}, nil); err != nil {
		return err
	}
	if delay := rhythm.Duration(worker.rhythm.Key); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return err
		}
	}
	if err := worker.client.call(ctx, sessionID, "Input.dispatchKeyEvent", map[string]any{
		"type":                  "keyUp",
		"key":                   "a",
		"code":                  "KeyA",
		"windowsVirtualKeyCode": 65,
		"modifiers":             2,
	}, nil); err != nil {
		return err
	}
	if delay := rhythm.Duration(worker.rhythm.Release); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return err
		}
	}
	return worker.client.call(ctx, sessionID, "Input.dispatchKeyEvent", map[string]any{
		"type":                  "keyUp",
		"key":                   "Control",
		"code":                  "ControlLeft",
		"windowsVirtualKeyCode": 17,
		"modifiers":             0,
	}, nil)
}

// insertText 通过 CDP Input.insertText 将文本写入当前获得焦点的输入框，并按账户节律抖动
func (worker *Worker) insertText(ctx context.Context, sessionID, text string) error {
	if delay := rhythm.Duration(worker.rhythm.Control); delay > 0 {
		if err := waitContext(ctx, delay); err != nil {
			return err
		}
	}
	return worker.client.call(ctx, sessionID, "Input.insertText", map[string]any{"text": text}, nil)
}

// pastePrompt 将按 LF 规范化的提示词经 CDP Input.insertText 写入有焦点的输入框并校验页面值一致
func (worker *Worker) pastePrompt(ctx context.Context, sessionID, prompt string) error {
	prompt = normalizePromptNewlines(prompt)
	if err := worker.insertText(ctx, sessionID, prompt); err != nil {
		return fmt.Errorf("粘贴提示词: %w", err)
	}
	value, err := worker.client.evaluateString(ctx, sessionID, `(`+promptValueFunction+`)()`)
	if err != nil {
		return fmt.Errorf("读取提示词: %w", err)
	}
	if value != prompt {
		return fmt.Errorf("粘贴提示词后页面值不同 prompt=%q", prompt)
	}
	return nil
}
