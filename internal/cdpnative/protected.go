package cdpnative

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
)

const protectedGrantBatch = 4

type protectedMessage struct {
	Sent  bool   `json:"sent"`
	Proof string `json:"proof"`
	pageProofTiming
	Status  int         `json:"status"`
	Headers [][2]string `json:"headers"`
	Done    bool        `json:"done"`
	Error   string      `json:"error"`
}

const protectedPump = `
  (async () => {
    try {
      while ((!request.xhr || request.xhr.readyState < 2) && !failure) await next();
      if (failure) throw new Error(failure);
      const xhr = request.xhr;
      const lines = xhr.getAllResponseHeaders().split(/\r\n/).filter(Boolean);
      emit(JSON.stringify({status: xhr.status, headers: lines.map(line => [line.slice(0, line.indexOf(":")), line.slice(line.indexOf(":") + 1).trim()])}));
      const encoder = new TextEncoder();
      for (let sent = 0;;) {
        const text = xhr.responseText;
        if (sent < text.length) {
          while (request.credit <= 0 && !failure) await new Promise(resolve => request.resume = resolve);
          if (failure) throw new Error(failure);
          let end = Math.min(text.length, sent + 65536);
          if (end < text.length && text.charCodeAt(end - 1) >= 0xd800 && text.charCodeAt(end - 1) <= 0xdbff) end--;
          const block = encoder.encode(text.slice(sent, end));
          let binary = "";
          for (let start = 0; start < block.length; start += 32768) {
            binary += String.fromCharCode(...block.subarray(start, start + 32768));
          }
          emit(btoa(binary));
          request.credit--;
          sent = end;
          continue;
        }
        if (failure) throw new Error(failure);
        if (loaded || xhr.readyState === 4) break;
        await next();
      }
      emit(JSON.stringify({done: true}));
    } catch (error) {
      emit(JSON.stringify({error: String(error)}));
    } finally {
      page.requests.delete(id);
    }
  })();
`

// protectedSend 在 Runtime.callFunctionOn 中以 page 对象为 this，使用 XMLHttpRequest 流式发送并通过 CDP binding 回传分块
const protectedSend = `function(id, url, headers, body, activate, bindingName) {
  const page = this;
  const emit = payload => {
    if (typeof window[bindingName] === "function") window[bindingName](payload);
  };
  const xhr = new XMLHttpRequest();
  const request = {xhr, credit: 8, resume: null};
  (page.requests ||= new Map()).set(id, request);
  let loaded = false, failure = "", wake = null;
  const changed = () => {
    wake?.();
    wake = null;
  };
  const next = () => new Promise(resolve => {
    const timer = setTimeout(() => {
      wake = null;
      resolve();
    }, 250);
    wake = () => {
      clearTimeout(timer);
      resolve();
    };
  });
  const send = () => {
    xhr.open("POST", url);
    xhr.withCredentials = true;
    for (const [name, value] of JSON.parse(headers)) xhr.setRequestHeader(name, value);
    xhr.onreadystatechange = xhr.onprogress = changed;
    xhr.onload = () => {
      loaded = true;
      changed();
    };
    xhr.onloadend = () => {
      loaded = true;
      changed();
    };
    xhr.onerror = () => {
      failure = "NetworkError when attempting to fetch resource.";
      changed();
    };
    xhr.onabort = () => {
      failure = "AbortError: The operation was aborted.";
      changed();
      request.resume?.();
    };
    xhr.send(body);
  };
  const keydown = event => {
    if (!event.isTrusted) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    removeEventListener("keydown", keydown, true);
    send();
  };
  const keyup = event => {
    if (!event.isTrusted) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    removeEventListener("keyup", keyup, true);
  };
  request.release = () => {
    removeEventListener("keydown", keydown, true);
    removeEventListener("keyup", keyup, true);
    if (xhr.readyState === 0) {
      failure = "AbortError: The operation was aborted.";
      changed();
    }
  };
  if (activate) {
    addEventListener("keydown", keydown, true);
    addEventListener("keyup", keyup, true);
  } else {
    send();
  }
` + protectedPump + `}`

const protectedCancel = `function(id) {
  const page = this;
  const request = page.requests?.get(id);
  request?.release();
  request?.xhr?.abort();
  request?.resume?.();
  page.requests?.delete(id);
}`

const protectedGrant = `function(id, count) {
  const page = this;
  const request = page.requests?.get(id);
  if (!request) return;
  request.credit += Number(count);
  request.resume?.();
  request.resume = null;
}`

// SendProtected 通过外部 CDP 浏览器页面内 XMLHttpRequest 流式发送受保护请求，支持分块、背压与中止
func (worker *Worker) SendProtected(ctx context.Context, rawURL string, headers http.Header, body []byte) (*camoufoxnative.ProtectedResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	worker.lockInput()
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		worker.inputMu.Unlock()
		return nil, errors.New("外部 CDP runtime 已关闭")
	}
	client := worker.client
	sessionID, pageObjectID := worker.beginRequest()
	worker.mu.Unlock()
	worker.inputMu.Unlock()

	response, err := worker.sendProtected(ctx, client, sessionID, pageObjectID, rawURL, headers, body)
	if err != nil {
		worker.finishRequest(sessionID)
	}
	return response, err
}

func (worker *Worker) sendProtected(ctx context.Context, client *cdpClient, sessionID, pageObjectID, rawURL string, headers http.Header, body []byte) (*camoufoxnative.ProtectedResponse, error) {
	encodedHeaders, err := json.Marshal(browserRequestHeaders(headers))
	if err != nil {
		return nil, fmt.Errorf("编码浏览器请求头: %w", err)
	}
	requestID := rand.Text()
	bindingName := "__cdp_stream_" + requestID
	channel, err := client.openBinding(ctx, sessionID, bindingName)
	if err != nil {
		return nil, fmt.Errorf("外部浏览器发送受保护请求: %w", err)
	}

	activate := ""
	if headers.Get("Priority") == "u=0" {
		activate = "keydown"
	}
	startCtx, cancelStart := context.WithTimeout(ctx, 5*time.Second)
	_, err = client.callFunctionOn(startCtx, sessionID, pageObjectID, protectedSend, []any{
		requestID, rawURL, string(encodedHeaders), string(body), activate, bindingName,
	}, true, false)
	if err == nil && activate != "" {
		err = dispatchPauseKey(startCtx, client, sessionID)
	}
	cancelStart()
	if err != nil {
		return nil, worker.abandonProtectedRequest(ctx, sessionID, pageObjectID, requestID, bindingName, fmt.Errorf("外部浏览器发送受保护请求: %w", err))
	}

	status, header, err := readResponseHead(ctx, channel)
	if err != nil {
		return nil, worker.abandonProtectedRequest(ctx, sessionID, pageObjectID, requestID, bindingName, err)
	}

	bodyCtx, cancelBody := context.WithCancel(ctx)
	return &camoufoxnative.ProtectedResponse{
		StatusCode: status,
		Header:     header,
		Body: &protectedResponseBody{
			ctx:          bodyCtx,
			cancel:       cancelBody,
			worker:       worker,
			sessionID:    sessionID,
			pageObjectID: pageObjectID,
			requestID:    requestID,
			bindingName:  bindingName,
			channel:      channel,
		},
	}, nil
}

func dispatchPauseKey(ctx context.Context, client *cdpClient, sessionID string) error {
	if err := client.call(ctx, sessionID, "Input.dispatchKeyEvent", map[string]any{
		"type":                  "rawKeyDown",
		"key":                   "Pause",
		"code":                  "Pause",
		"windowsVirtualKeyCode": 19,
	}, nil); err != nil {
		return err
	}
	return client.call(ctx, sessionID, "Input.dispatchKeyEvent", map[string]any{
		"type":                  "keyUp",
		"key":                   "Pause",
		"code":                  "Pause",
		"windowsVirtualKeyCode": 19,
	}, nil)
}

func readResponseHead(ctx context.Context, channel *cdpChannel) (int, http.Header, error) {
	metadata, err := readProtectedMessage(ctx, channel)
	if err == nil && metadata.Error != "" {
		err = errors.New(metadata.Error)
	}
	if err != nil {
		return 0, nil, err
	}
	header := make(http.Header, len(metadata.Headers)+1)
	for _, pair := range metadata.Headers {
		header.Add(pair[0], pair[1])
	}
	if header.Get("Content-Type") == "" {
		header.Set("Content-Type", "application/json+protobuf")
	}
	return metadata.Status, header, nil
}

func readProtectedMessage(ctx context.Context, channel *cdpChannel) (protectedMessage, error) {
	value, err := channel.next(ctx)
	if err != nil {
		return protectedMessage{}, err
	}
	var message protectedMessage
	if err := json.Unmarshal([]byte(value), &message); err != nil {
		return message, fmt.Errorf("解析外部浏览器响应消息: %w", err)
	}
	return message, nil
}

func browserRequestHeaders(headers http.Header) [][2]string {
	order := []string{
		"X-AIStudio-G1-Tier",
		"X-AIStudio-Visit-Id",
		"X-Goog-Ext-519733851-bin",
		"X-Goog-Api-Key",
		"X-Goog-AuthUser",
		"Authorization",
		"Content-Type",
		"X-User-Agent",
	}
	result := make([][2]string, 0, len(order))
	for _, name := range order {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			result = append(result, [2]string{name, value})
		}
	}
	return result
}

type protectedResponseBody struct {
	ctx          context.Context
	cancel       context.CancelFunc
	worker       *Worker
	sessionID    string
	pageObjectID string
	requestID    string
	bindingName  string
	channel      *cdpChannel
	// stop 在正文读完前关闭时中止请求，为空时在页面中止 XHR
	stop func() error
	// release 在关闭时结束请求所在的标签页使用，参数表示正文完整无错读完与正文中出现了函数调用；为空时按受保护请求结束
	release func(done, call bool)

	call     bool
	tail     []byte
	mu       sync.Mutex
	buffer   []byte
	consumed int
	done     bool
	failed   bool
	closed   bool
}

func (body *protectedResponseBody) Read(target []byte) (int, error) {
	body.mu.Lock()
	defer body.mu.Unlock()
	if body.closed {
		return 0, io.ErrClosedPipe
	}
	for len(body.buffer) == 0 && !body.done {
		value, err := body.channel.next(body.ctx)
		if err := body.ctx.Err(); err != nil {
			return 0, err
		}
		if err != nil {
			body.done = true
			body.failed = true
			return 0, errors.Join(err, body.worker.cancelProtectedRequest(body.sessionID, body.pageObjectID, body.requestID, body.bindingName))
		}
		if !strings.HasPrefix(value, "{") {
			start := len(body.buffer)
			body.buffer, err = base64.StdEncoding.AppendDecode(body.buffer, []byte(value))
			if err != nil {
				return 0, fmt.Errorf("解码外部浏览器响应块: %w", err)
			}
			if !body.call {
				window := append(body.tail, body.buffer[start:]...)
				body.call = bytes.Contains(window, camoufoxnative.FunctionCallMarker)
				body.tail = window[max(len(window)-len(camoufoxnative.FunctionCallMarker)+1, 0):]
			}
			if body.consumed++; body.consumed == protectedGrantBatch {
				body.consumed = 0
				if _, err := body.worker.client.callFunctionOn(body.ctx, body.sessionID, body.pageObjectID, protectedGrant, []any{
					body.requestID, protectedGrantBatch,
				}, true, false); err != nil {
					return 0, fmt.Errorf("外部浏览器继续读取受保护响应: %w", err)
				}
			}
			continue
		}
		var message protectedMessage
		if err := json.Unmarshal([]byte(value), &message); err != nil {
			return 0, fmt.Errorf("解析外部浏览器响应消息: %w", err)
		}
		if message.Error != "" || message.Done {
			body.done = true
			body.worker.client.closeBinding(body.sessionID, body.bindingName)
		}
		if message.Error != "" {
			body.failed = true
			return 0, errors.New(message.Error)
		}
	}
	if len(body.buffer) == 0 {
		return 0, io.EOF
	}
	n := copy(target, body.buffer)
	body.buffer = body.buffer[n:]
	return n, nil
}

func (body *protectedResponseBody) Close() error {
	body.cancel()
	body.mu.Lock()
	if body.closed {
		body.mu.Unlock()
		return nil
	}
	body.closed = true
	done, failed, call := body.done, body.failed, body.call
	body.mu.Unlock()
	if body.release != nil {
		defer body.release(done && !failed, call)
	} else {
		defer body.worker.finishRequest(body.sessionID)
	}
	if done {
		return nil
	}
	if body.stop != nil {
		body.worker.client.closeBinding(body.sessionID, body.bindingName)
		return body.stop()
	}
	return body.worker.cancelProtectedRequest(body.sessionID, body.pageObjectID, body.requestID, body.bindingName)
}

func (worker *Worker) abandonProtectedRequest(ctx context.Context, sessionID, pageObjectID, requestID, bindingName string, err error) error {
	if ctx.Err() != nil {
		go func() { _ = worker.cancelProtectedRequest(sessionID, pageObjectID, requestID, bindingName) }()
		return err
	}
	return errors.Join(err, worker.cancelProtectedRequest(sessionID, pageObjectID, requestID, bindingName))
}

func (worker *Worker) cancelProtectedRequest(sessionID, pageObjectID, requestID, bindingName string) error {
	worker.client.closeBinding(sessionID, bindingName)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := worker.client.callFunctionOn(ctx, sessionID, pageObjectID, protectedCancel, []any{requestID}, true, false)
	return err
}
