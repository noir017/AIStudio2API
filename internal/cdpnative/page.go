package cdpnative

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/waa/prooftime"
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
