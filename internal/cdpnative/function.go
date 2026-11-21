package cdpnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
)

const (
	// functionResultInputSelector 是最后一轮函数调用块中的函数响应输入框
	functionResultInputSelector = "ms-chat-turn:last-of-type ms-function-call-chunk input"
	// functionResultSubmitSelector 是最后一轮函数调用块中提交函数响应的按钮
	functionResultSubmitSelector = "ms-chat-turn:last-of-type ms-function-call-chunk button.ms-button-primary"
)

// functionTab 返回最后一轮是 contents 所回应的函数调用、输入框中没有未发出输入的空闲标签页；调用方持有 mu
func (worker *Worker) functionTab(contents []json.RawMessage) *chatTab {
	for _, tab := range worker.chats {
		if !tab.busy && !tab.unsent && tab.contents != nil && nextChatAction(tab.contents, contents) == chatContinue {
			return tab
		}
	}
	return nil
}

// ContinuesFunctionCall 判断函数结果正文 body 是否回应某个空闲标签页最后一轮的函数调用
func (worker *Worker) ContinuesFunctionCall(body []byte) bool {
	contents, err := generateContents(body)
	if err != nil {
		return false
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.functionTab(contents) != nil
}

func (worker *Worker) acquireFunctionChat(contents []json.RawMessage) (*chatTab, error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed {
		return nil, errors.New("外部 CDP runtime 已关闭")
	}
	tab := worker.functionTab(contents)
	if tab == nil {
		return nil, errors.New("没有最后一轮是本请求回应的函数调用的标签页")
	}
	tab.busy = true
	worker.useTab(tab.sessionID, tab.targetID)
	return tab, nil
}

func (worker *Worker) submitResult(ctx, input context.Context, tab *chatTab, turn camoufoxnative.GenerateTurn, requestID, bindingName string, step func(camoufoxnative.GenerateStep)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	step(camoufoxnative.StepPasteResult)
	if err := worker.click(input, tab.sessionID, functionResultInputSelector, "", "官网函数响应输入框", pointerMoveDuration); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	worker.mu.Lock()
	tab.unsent = true
	worker.mu.Unlock()
	if err := worker.insertText(input, tab.sessionID, turn.Result); err != nil {
		return fmt.Errorf("粘贴函数结果: %w", err)
	}
	if err := worker.arm(ctx, tab, turn, requestID, bindingName); err != nil {
		return err
	}
	step(camoufoxnative.StepClickSubmit)
	return worker.clickArmed(ctx, tab, requestID, functionResultSubmitSelector, "官网函数响应提交按钮", pointerMoveDuration)
}
