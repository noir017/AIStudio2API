package cdpnative

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

// SetCookies 按 storage state 的 Cookie 格式通过 Network.setCookies 写入外部浏览器
func (worker *Worker) SetCookies(ctx context.Context, encoded []byte) error {
	var cookies []storageCookie
	if err := json.Unmarshal(encoded, &cookies); err != nil {
		return fmt.Errorf("解析 Cookie: %w", err)
	}
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return errors.New("外部 CDP runtime 已关闭")
	}
	client := worker.client
	sessionID := worker.sessionID
	worker.mu.Unlock()
	return client.installCookies(ctx, sessionID, cookies)
}

// StorageCookies 通过 Storage.getCookies 导出当前浏览器 Cookie，保留启动写入的顺序，之后出现的 Cookie 排在其后
func (worker *Worker) StorageCookies(ctx context.Context) ([]byte, error) {
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return nil, errors.New("外部 CDP runtime 已关闭")
	}
	client := worker.client
	sessionID := worker.sessionID
	order := worker.cookieOrder
	worker.mu.Unlock()

	var stored struct {
		Cookies []map[string]any `json:"cookies"`
	}
	if err := client.call(ctx, sessionID, "Storage.getCookies", nil, &stored); err != nil {
		return nil, fmt.Errorf("导出外部浏览器 Cookie: %w", err)
	}
	cookies := make([]storageCookie, 0, len(stored.Cookies))
	for _, raw := range stored.Cookies {
		if cookie, ok := decodeCDPCookie(raw); ok {
			cookies = append(cookies, cookie)
		}
	}
	rank := func(cookie storageCookie) int {
		if index, installed := order[cookie.key()]; installed {
			return index
		}
		return len(order)
	}
	sort.SliceStable(cookies, func(left, right int) bool {
		if leftRank, rightRank := rank(cookies[left]), rank(cookies[right]); leftRank != rightRank {
			return leftRank < rightRank
		}
		if cookies[left].Domain != cookies[right].Domain {
			return cookies[left].Domain < cookies[right].Domain
		}
		if cookies[left].Name != cookies[right].Name {
			return cookies[left].Name < cookies[right].Name
		}
		return cookies[left].Path < cookies[right].Path
	})
	return json.Marshal(cookies)
}

func (client *cdpClient) installCookies(ctx context.Context, sessionID string, cookies []storageCookie) error {
	if len(cookies) == 0 {
		return nil
	}
	params := make([]map[string]any, 0, len(cookies))
	for _, item := range cookies {
		if item.Name == "" || item.Domain == "" {
			continue
		}
		pathValue := item.Path
		if pathValue == "" {
			pathValue = "/"
		}
		cookie := map[string]any{
			"name":     item.Name,
			"value":    item.Value,
			"domain":   item.Domain,
			"path":     pathValue,
			"httpOnly": item.HTTPOnly,
			"secure":   item.Secure,
		}
		if item.Expires > 0 {
			cookie["expires"] = math.Floor(item.Expires)
		}
		switch strings.ToLower(strings.TrimSpace(item.SameSite)) {
		case "strict":
			cookie["sameSite"] = "Strict"
		case "lax":
			cookie["sameSite"] = "Lax"
		case "none":
			if item.Secure {
				cookie["sameSite"] = "None"
			}
		}
		if item.PartitionKey != "" {
			cookie["partitionKey"] = map[string]any{
				"topLevelSite":         item.PartitionKey,
				"hasCrossSiteAncestor": false,
			}
		}
		params = append(params, cookie)
	}
	if len(params) == 0 {
		return nil
	}
	if err := client.call(ctx, sessionID, "Network.setCookies", map[string]any{"cookies": params}, nil); err != nil {
		return fmt.Errorf("写入外部浏览器 Cookie: %w", err)
	}
	return nil
}

func decodeCDPCookie(raw map[string]any) (storageCookie, bool) {
	name, _ := raw["name"].(string)
	domain, _ := raw["domain"].(string)
	pathValue, _ := raw["path"].(string)
	if name == "" || domain == "" || pathValue == "" {
		return storageCookie{}, false
	}
	value, _ := raw["value"].(string)
	expires := float64(-1)
	if rawExpires, ok := raw["expires"].(float64); ok && rawExpires > 0 {
		expires = rawExpires
	}
	sameSite, _ := raw["sameSite"].(string)
	sameSite = strings.ToLower(strings.TrimSpace(sameSite))
	switch sameSite {
	case "none":
		sameSite = "None"
	case "lax":
		sameSite = "Lax"
	case "strict":
		sameSite = "Strict"
	default:
		sameSite = ""
	}
	partitionKey := decodePartitionKey(raw["partitionKey"])
	httpOnly, _ := raw["httpOnly"].(bool)
	secure, _ := raw["secure"].(bool)
	return storageCookie{
		Name:         name,
		Value:        value,
		Domain:       domain,
		Path:         pathValue,
		Expires:      expires,
		HTTPOnly:     httpOnly,
		Secure:       secure,
		SameSite:     sameSite,
		PartitionKey: partitionKey,
	}, true
}

func decodePartitionKey(raw any) string {
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case map[string]any:
		top, _ := value["topLevelSite"].(string)
		return strings.TrimSpace(top)
	default:
		return ""
	}
}
