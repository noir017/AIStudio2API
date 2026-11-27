package cdpnative

import (
	"io"
	"net/http"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/camoufoxnative"
	"github.com/Mag1cFall/AIStudio2API/internal/waa/rhythm"
)

const (
	defaultAIStudioOrigin = "https://aistudio.google.com"
	defaultMakerSuiteHost = "alkalimakersuite-pa.clients6.google.com"
	generateContentPath   = "/$rpc/google.internal.alkali.applications.makersuite.v1.MakerSuiteService/GenerateContent"
)

// publicHeaderNames 是页面 MakerSuite 请求共用的公共头，x-goog-ext-519733851-bin 由页面 GetLoggingContext 响应得出
var publicHeaderNames = []string{
	"x-goog-api-key",
	"x-goog-authuser",
	"x-user-agent",
	"x-aistudio-visit-id",
	"user-agent",
}

// pageCatalogRPCs 是官网页面载入时读取条款状态、权益与模型目录的 RPC
var pageCatalogRPCs = []string{
	"GetLoggingContext",
	"GetAiStudioBenefitTier",
	"ListModels",
}

// Options 定义单个 AI Studio 账户连接外部 CDP 浏览器的启动选项
type Options struct {
	Endpoint         string
	Headers          http.Header
	StorageStatePath string
	Model            string
	AuthUser         int
	ReadyTimeout     time.Duration
	Log              io.Writer
	StartupProgress  func(camoufoxnative.StartupStage)
	// HTTPClient 可选，用于请求 /json/version；为空时使用带默认超时的客户端
	HTTPClient *http.Client
	// Origin 可选，默认 https://aistudio.google.com
	Origin string
	// RPCHost 可选，默认 alkalimakersuite-pa.clients6.google.com
	RPCHost string
	// Rhythm 可选，点击与输入事件节律；为零值时按账户随机生成
	Rhythm rhythm.Rhythm
}

func (options Options) reportStartup(stage camoufoxnative.StartupStage) {
	if options.StartupProgress != nil {
		options.StartupProgress(stage)
	}
}

func (options Options) origin() string {
	if options.Origin != "" {
		return options.Origin
	}
	return defaultAIStudioOrigin
}

func (options Options) rpcHost() string {
	if options.RPCHost != "" {
		return options.RPCHost
	}
	return defaultMakerSuiteHost
}

func (options Options) readyTimeout() time.Duration {
	if options.ReadyTimeout > 0 {
		return options.ReadyTimeout
	}
	return 120 * time.Second
}

// TabHandle 为二期界面交互（pageGenerator）预留的标签页与页面对象扩展点
type TabHandle struct {
	TargetID     string
	SessionID    string
	PageObjectID string
}

type storageState struct {
	Cookies []storageCookie `json:"cookies"`
	Origins []storageOrigin `json:"origins"`
}

type storageCookie struct {
	Name         string  `json:"name"`
	Value        string  `json:"value"`
	Domain       string  `json:"domain"`
	Path         string  `json:"path"`
	Expires      float64 `json:"expires"`
	HTTPOnly     bool    `json:"httpOnly"`
	Secure       bool    `json:"secure"`
	SameSite     string  `json:"sameSite"`
	PartitionKey string  `json:"partitionKey,omitempty"`
}

type cookieKey struct {
	name, domain, path, partition string
}

func (cookie storageCookie) key() cookieKey {
	return cookieKey{cookie.Name, cookie.Domain, cookie.Path, cookie.PartitionKey}
}

type storageOrigin struct {
	Origin       string             `json:"origin"`
	LocalStorage []localStorageItem `json:"localStorage"`
}

type localStorageItem struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
