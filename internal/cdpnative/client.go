package cdpnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/gorilla/websocket"
)

const (
	cdpCommandTimeout = 150 * time.Second
	pagePollInterval  = 50 * time.Millisecond
)

// ErrDisconnected 表示外部 CDP WebSocket 连接已断开
var ErrDisconnected = errors.New("外部 CDP 连接已断开")

type cdpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

type cdpCommandError struct {
	method  string
	code    int
	message string
}

func (err *cdpCommandError) Error() string {
	return fmt.Sprintf("CDP %s 失败 (%d): %s", err.method, err.code, err.message)
}

type cdpEnvelope struct {
	ID        *int            `json:"id,omitempty"`
	SessionID string          `json:"sessionId,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *cdpError       `json:"error,omitempty"`
}

type cdpPending struct {
	method   string
	response chan cdpResponse
}

type cdpResponse struct {
	result json.RawMessage
	err    error
}

type trackedRequest struct {
	url     string
	method  string
	headers map[string]string
}

type pageLoad struct {
	originHost string
	rpcHost    string
	requests   map[string]*trackedRequest
	responses  map[string]string
	headers    map[string]string
	image      bool
	bundle     string
	denial     *camoufoxnative.AccessDenial
}

type cdpChannel struct {
	mu     sync.Mutex
	queue  []string
	err    error
	notify chan struct{}
}

func (channel *cdpChannel) push(value string) {
	channel.mu.Lock()
	channel.queue = append(channel.queue, value)
	channel.mu.Unlock()
	select {
	case channel.notify <- struct{}{}:
	default:
	}
}

func (channel *cdpChannel) finish(err error) {
	channel.mu.Lock()
	if channel.err == nil {
		channel.err = err
	}
	channel.mu.Unlock()
	select {
	case channel.notify <- struct{}{}:
	default:
	}
}

func (channel *cdpChannel) next(ctx context.Context) (string, error) {
	for {
		channel.mu.Lock()
		if len(channel.queue) > 0 {
			value := channel.queue[0]
			channel.queue = channel.queue[1:]
			channel.mu.Unlock()
			return value, nil
		}
		err := channel.err
		channel.mu.Unlock()
		if err != nil {
			return "", err
		}
		select {
		case <-channel.notify:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

type cdpClient struct {
	conn      *websocket.Conn
	writeLock sync.Mutex
	mu        sync.Mutex
	nextID    int
	pending   map[int]cdpPending
	channels  map[string]*cdpChannel
	loads     map[string]*pageLoad
	failure   error
	writeErr  error
	done      chan struct{}
}

func dialCDP(ctx context.Context, wsURL string, headers http.Header) (*cdpClient, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	var reqHeaders http.Header
	if len(headers) > 0 {
		reqHeaders = headers.Clone()
	}
	conn, _, err := dialer.DialContext(ctx, wsURL, reqHeaders)
	if err != nil {
		return nil, fmt.Errorf("连接外部 CDP WebSocket: %w", err)
	}
	client := &cdpClient{
		conn:     conn,
		pending:  make(map[int]cdpPending),
		channels: make(map[string]*cdpChannel),
		loads:    make(map[string]*pageLoad),
		done:     make(chan struct{}),
	}
	go client.read()
	return client, nil
}

func (client *cdpClient) close() error {
	if client == nil || client.conn == nil {
		return nil
	}
	return client.conn.Close()
}

func (client *cdpClient) err() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.failure
}

func bindingKey(sessionID, name string) string {
	return sessionID + "\x00" + name
}

func (client *cdpClient) call(ctx context.Context, sessionID, method string, params any, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, cdpCommandTimeout)
	defer cancel()

	respCh := make(chan cdpResponse, 1)
	client.mu.Lock()
	if sessionID != "" {
		if load := client.loads[sessionID]; load != nil && load.denial != nil {
			denial := load.denial
			client.mu.Unlock()
			return denial
		}
	}
	if client.failure != nil {
		failure := client.failure
		client.mu.Unlock()
		return failure
	}
	client.nextID++
	id := client.nextID
	client.pending[id] = cdpPending{method: method, response: respCh}
	client.mu.Unlock()

	msg := map[string]any{
		"id":     id,
		"method": method,
	}
	if params != nil {
		msg["params"] = params
	}
	if sessionID != "" {
		msg["sessionId"] = sessionID
	}
	if err := client.write(msg); err != nil {
		client.mu.Lock()
		if client.writeErr == nil {
			client.writeErr = err
		}
		client.mu.Unlock()
		_ = client.conn.Close()
		select {
		case res := <-respCh:
			return res.err
		case <-ctx.Done():
			client.forget(id)
			return err
		}
	}

	select {
	case res := <-respCh:
		if res.err != nil {
			return res.err
		}
		if out != nil && len(res.result) > 0 {
			if err := json.Unmarshal(res.result, out); err != nil {
				return fmt.Errorf("解码 CDP %s 响应: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		client.forget(id)
		return ctx.Err()
	}
}

func (client *cdpClient) write(msg map[string]any) error {
	client.writeLock.Lock()
	defer client.writeLock.Unlock()
	if err := client.conn.SetWriteDeadline(time.Now().Add(cdpCommandTimeout)); err != nil {
		return err
	}
	return client.conn.WriteJSON(msg)
}

func (client *cdpClient) forget(id int) {
	client.mu.Lock()
	delete(client.pending, id)
	client.mu.Unlock()
}

func (client *cdpClient) read() {
	for {
		var env cdpEnvelope
		if err := client.conn.ReadJSON(&env); err != nil {
			client.fail(err)
			return
		}
		if env.ID != nil {
			client.respond(*env.ID, env)
			continue
		}
		if env.Method != "" {
			client.observe(env.SessionID, env.Method, env.Params)
		}
	}
}

func (client *cdpClient) respond(id int, env cdpEnvelope) {
	client.mu.Lock()
	pending, ok := client.pending[id]
	delete(client.pending, id)
	client.mu.Unlock()
	if !ok {
		return
	}
	if env.Error != nil {
		pending.response <- cdpResponse{err: &cdpCommandError{
			method:  pending.method,
			code:    env.Error.Code,
			message: env.Error.Message,
		}}
		return
	}
	pending.response <- cdpResponse{result: env.Result}
}

func (client *cdpClient) fail(err error) {
	client.mu.Lock()
	if client.writeErr != nil {
		err = client.writeErr
	}
	failure := fmt.Errorf("%w: %w", ErrDisconnected, err)
	client.failure = failure
	pending := client.pending
	channels := client.channels
	client.pending = map[int]cdpPending{}
	client.channels = map[string]*cdpChannel{}
	client.mu.Unlock()

	close(client.done)
	for _, item := range pending {
		item.response <- cdpResponse{err: failure}
	}
	for _, channel := range channels {
		channel.finish(failure)
	}
}

func (client *cdpClient) openBinding(ctx context.Context, sessionID, name string) (*cdpChannel, error) {
	channel := &cdpChannel{notify: make(chan struct{}, 1)}
	key := bindingKey(sessionID, name)
	client.mu.Lock()
	if client.failure != nil {
		failure := client.failure
		client.mu.Unlock()
		return nil, failure
	}
	client.channels[key] = channel
	client.mu.Unlock()

	if err := client.call(ctx, sessionID, "Runtime.addBinding", map[string]any{"name": name}, nil); err != nil {
		client.mu.Lock()
		delete(client.channels, key)
		client.mu.Unlock()
		return nil, fmt.Errorf("注册 CDP binding %s: %w", name, err)
	}
	return channel, nil
}

func (client *cdpClient) closeBinding(sessionID, name string) {
	key := bindingKey(sessionID, name)
	client.mu.Lock()
	delete(client.channels, key)
	failed := client.failure != nil
	client.mu.Unlock()
	if failed {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.call(ctx, sessionID, "Runtime.removeBinding", map[string]any{"name": name}, nil)
}

func (client *cdpClient) startLoad(sessionID, originHost, rpcHost string) {
	client.mu.Lock()
	client.loads[sessionID] = &pageLoad{
		originHost: originHost,
		rpcHost:    rpcHost,
		requests:   make(map[string]*trackedRequest),
		responses:  make(map[string]string),
	}
	client.mu.Unlock()
}

func (client *cdpClient) endLoad(sessionID string) {
	client.mu.Lock()
	delete(client.loads, sessionID)
	client.mu.Unlock()
}

func (client *cdpClient) loadDenial(sessionID string) *camoufoxnative.AccessDenial {
	client.mu.Lock()
	defer client.mu.Unlock()
	if load := client.loads[sessionID]; load != nil {
		return load.denial
	}
	return nil
}

func (client *cdpClient) observe(sessionID, method string, rawParams json.RawMessage) {
	switch method {
	case "Runtime.bindingCalled":
		var params struct {
			Name    string `json:"name"`
			Payload string `json:"payload"`
		}
		if json.Unmarshal(rawParams, &params) != nil {
			return
		}
		client.mu.Lock()
		channel := client.channels[bindingKey(sessionID, params.Name)]
		client.mu.Unlock()
		if channel != nil {
			channel.push(params.Payload)
		}
	case "Page.frameNavigated":
		var params struct {
			Frame struct {
				ParentID string `json:"parentId"`
				URL      string `json:"url"`
			} `json:"frame"`
		}
		if json.Unmarshal(rawParams, &params) != nil || params.Frame.ParentID != "" {
			return
		}
		client.checkDenial(sessionID, params.Frame.URL)
	case "Page.navigatedWithinDocument":
		var params struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(rawParams, &params) != nil {
			return
		}
		client.checkDenial(sessionID, params.URL)
	case "Network.requestWillBeSent":
		var params struct {
			RequestID string `json:"requestId"`
			Request   struct {
				URL     string         `json:"url"`
				Method  string         `json:"method"`
				Headers map[string]any `json:"headers"`
			} `json:"request"`
		}
		if json.Unmarshal(rawParams, &params) != nil || params.RequestID == "" {
			return
		}
		headers := normalizeHeaderMap(params.Request.Headers)
		client.mu.Lock()
		load := client.loads[sessionID]
		if load != nil {
			req := &trackedRequest{
				url:     params.Request.URL,
				method:  params.Request.Method,
				headers: headers,
			}
			load.requests[params.RequestID] = req
			maybeCapturePublicHeaders(load, req)
		}
		client.mu.Unlock()
	case "Network.requestWillBeSentExtraInfo":
		var params struct {
			RequestID string         `json:"requestId"`
			Headers   map[string]any `json:"headers"`
		}
		if json.Unmarshal(rawParams, &params) != nil || params.RequestID == "" {
			return
		}
		extra := normalizeHeaderMap(params.Headers)
		client.mu.Lock()
		if load := client.loads[sessionID]; load != nil {
			if req := load.requests[params.RequestID]; req != nil {
				for k, v := range extra {
					if v != "" && req.headers[k] == "" {
						req.headers[k] = v
					}
				}
				maybeCapturePublicHeaders(load, req)
			}
		}
		client.mu.Unlock()
	case "Network.responseReceived":
		var params struct {
			RequestID string `json:"requestId"`
			Response  struct {
				URL string `json:"url"`
			} `json:"response"`
		}
		if json.Unmarshal(rawParams, &params) != nil || params.Response.URL == "" {
			return
		}
		parsed, err := url.Parse(params.Response.URL)
		if err != nil {
			return
		}
		client.mu.Lock()
		if load := client.loads[sessionID]; load != nil && load.bundle == "" && camoufoxnative.FrontendBundle(parsed) {
			load.bundle = params.Response.URL
		}
		client.mu.Unlock()
	case "Network.loadingFinished":
		var params struct {
			RequestID string `json:"requestId"`
		}
		if json.Unmarshal(rawParams, &params) != nil || params.RequestID == "" {
			return
		}
		client.mu.Lock()
		if load := client.loads[sessionID]; load != nil {
			if req := load.requests[params.RequestID]; req != nil {
				if parsed, err := url.Parse(req.url); err == nil {
					if strings.EqualFold(parsed.Hostname(), load.originHost) && path.Base(parsed.Path) == "generate_204" {
						load.image = true
					}
					if load.bundle == "" && camoufoxnative.FrontendBundle(parsed) {
						load.bundle = req.url
					}
					if strings.EqualFold(parsed.Hostname(), load.rpcHost) && strings.EqualFold(req.method, http.MethodPost) {
						if name := path.Base(parsed.Path); slices.Contains(pageCatalogRPCs, name) {
							load.responses[name] = params.RequestID
						}
					}
				}
			}
		}
		client.mu.Unlock()
	case "Network.loadingFailed":
		var params struct {
			RequestID string `json:"requestId"`
		}
		if json.Unmarshal(rawParams, &params) != nil || params.RequestID == "" {
			return
		}
		client.mu.Lock()
		if load := client.loads[sessionID]; load != nil {
			if req := load.requests[params.RequestID]; req != nil {
				if parsed, err := url.Parse(req.url); err == nil && strings.EqualFold(parsed.Hostname(), load.originHost) && path.Base(parsed.Path) == "generate_204" {
					load.image = true
				}
			}
		}
		client.mu.Unlock()
	}
}

func (client *cdpClient) checkDenial(sessionID, rawURL string) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return
	}
	denial := camoufoxnative.PageDenial(parsed)
	if denial == nil {
		return
	}
	client.mu.Lock()
	if load := client.loads[sessionID]; load != nil && load.denial == nil {
		load.denial = denial
	}
	client.mu.Unlock()
}

func normalizeHeaderMap(raw map[string]any) map[string]string {
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		if text, ok := value.(string); ok {
			out[strings.ToLower(strings.TrimSpace(key))] = text
		}
	}
	return out
}

func maybeCapturePublicHeaders(load *pageLoad, req *trackedRequest) {
	if load.headers != nil {
		return
	}
	if !strings.EqualFold(req.method, http.MethodPost) {
		return
	}
	parsed, err := url.Parse(req.url)
	if err != nil || !strings.EqualFold(parsed.Hostname(), load.rpcHost) {
		return
	}
	if req.headers["x-aistudio-visit-id"] == "" {
		return
	}
	captured := make(map[string]string, len(req.headers))
	for k, v := range req.headers {
		captured[k] = v
	}
	load.headers = captured
}

type remoteObject struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype,omitempty"`
	Value       any    `json:"value,omitempty"`
	ObjectID    string `json:"objectId,omitempty"`
	Description string `json:"description,omitempty"`
}

type exceptionDetails struct {
	Text      string        `json:"text"`
	Exception *remoteObject `json:"exception,omitempty"`
}

func (e *exceptionDetails) errorString() string {
	if e == nil {
		return ""
	}
	if e.Exception != nil && e.Exception.Description != "" {
		return e.Exception.Description
	}
	if e.Text != "" {
		return e.Text
	}
	return "unknown javascript exception"
}

func (client *cdpClient) evaluate(ctx context.Context, sessionID, expression string, returnByValue, awaitPromise bool) (remoteObject, error) {
	var resp struct {
		Result           remoteObject      `json:"result"`
		ExceptionDetails *exceptionDetails `json:"exceptionDetails"`
	}
	err := client.call(ctx, sessionID, "Runtime.evaluate", map[string]any{
		"expression":    expression,
		"returnByValue": returnByValue,
		"awaitPromise":  awaitPromise,
	}, &resp)
	if err != nil {
		return remoteObject{}, err
	}
	if resp.ExceptionDetails != nil {
		return remoteObject{}, fmt.Errorf("页面表达式异常: %s", resp.ExceptionDetails.errorString())
	}
	return resp.Result, nil
}

func (client *cdpClient) evaluateString(ctx context.Context, sessionID, expression string) (string, error) {
	obj, err := client.evaluate(ctx, sessionID, expression, true, true)
	if err != nil {
		return "", err
	}
	text, _ := obj.Value.(string)
	return text, nil
}

func (client *cdpClient) evaluateBool(ctx context.Context, sessionID, expression string) (bool, error) {
	obj, err := client.evaluate(ctx, sessionID, expression, true, true)
	if err != nil {
		return false, err
	}
	flag, _ := obj.Value.(bool)
	return flag, nil
}

func (client *cdpClient) callFunctionOn(ctx context.Context, sessionID, objectID, declaration string, args []any, returnByValue, awaitPromise bool) (remoteObject, error) {
	arguments := make([]map[string]any, len(args))
	for i, arg := range args {
		arguments[i] = map[string]any{"value": arg}
	}
	var resp struct {
		Result           remoteObject      `json:"result"`
		ExceptionDetails *exceptionDetails `json:"exceptionDetails"`
	}
	err := client.call(ctx, sessionID, "Runtime.callFunctionOn", map[string]any{
		"objectId":            objectID,
		"functionDeclaration": declaration,
		"arguments":           arguments,
		"returnByValue":       returnByValue,
		"awaitPromise":        awaitPromise,
	}, &resp)
	if err != nil {
		return remoteObject{}, err
	}
	if resp.ExceptionDetails != nil {
		return remoteObject{}, fmt.Errorf("页面函数异常: %s", resp.ExceptionDetails.errorString())
	}
	return resp.Result, nil
}

func (client *cdpClient) callFunctionOnString(ctx context.Context, sessionID, objectID, declaration string, args []any, awaitPromise bool) (string, error) {
	obj, err := client.callFunctionOn(ctx, sessionID, objectID, declaration, args, true, awaitPromise)
	if err != nil {
		return "", err
	}
	text, _ := obj.Value.(string)
	return text, nil
}

func (client *cdpClient) waitFor(ctx context.Context, sessionID, expression string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for time.Now().Before(deadline) {
		if denial := client.loadDenial(sessionID); denial != nil {
			return denial
		}
		if err := waitCtx.Err(); err != nil {
			return err
		}
		ready, err := client.evaluateBool(waitCtx, sessionID, expression)
		if err != nil && !retryableEvaluationError(err) {
			return err
		}
		if err == nil && ready {
			return nil
		}
		if err := waitContext(waitCtx, pagePollInterval); err != nil {
			return err
		}
	}
	return fmt.Errorf("等待页面条件超时: %s", expression)
}

func retryableEvaluationError(err error) bool {
	var cmdErr *cdpCommandError
	if !errors.As(err, &cmdErr) {
		return false
	}
	msg := strings.ToLower(cmdErr.message)
	return strings.Contains(msg, "execution context was destroyed") ||
		strings.Contains(msg, "cannot find context") ||
		strings.Contains(msg, "inspected target navigated")
}

func (client *cdpClient) waitLoad(ctx context.Context, sessionID string, timeout time.Duration, what string, ready func(*pageLoad) bool) error {
	deadline := time.Now().Add(timeout)
	for {
		client.mu.Lock()
		load := client.loads[sessionID]
		failure := client.failure
		var denial *camoufoxnative.AccessDenial
		var done bool
		if load != nil {
			denial = load.denial
			done = ready(load)
		}
		client.mu.Unlock()
		if denial != nil {
			return denial
		}
		if done {
			return nil
		}
		if failure != nil {
			return failure
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("页面载入 %s 内没有%s", timeout, what)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("等待页面载入%s: %w", what, ctx.Err())
		case <-client.done:
			timer.Stop()
			if err := client.err(); err != nil {
				return err
			}
			return ErrDisconnected
		case <-timer.C:
		}
	}
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
