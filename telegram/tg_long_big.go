package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type MessageLB struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

// SavedMessage 代表保存的已发送消息（含时间）
type SavedMessageLB struct {
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	savedMessagesLB = struct {
		sync.RWMutex
		messages []SavedMessageLB
		maxSize  int
	}{
		messages: make([]SavedMessageLB, 0, 100),
		maxSize:  100,
	}
)

func SendMessageLB(botToken, chatID, text string) error {
	proxy := "http://127.0.0.1:10809"
	proxyURL, err := url.Parse(proxy)
	if err != nil {
		return fmt.Errorf("解析代理地址失败: %w", err)
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}
	url := fmt.Sprintf("%s%s/sendMessage", telegramAPIURL, botToken)

	message := MessageLB{
		ChatID: chatID,
		Text:   text,
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonMessage))
		if err != nil {
			lastErr = fmt.Errorf("failed to send message: %w", err)
		} else {
			if resp.StatusCode != http.StatusOK {
				lastErr = fmt.Errorf("received non-200 response (attempt %d): %s", attempt, resp.Status)
			} else {

				AddMessageLB(SavedMessageLB{
					Text:      text,
					Timestamp: time.Now(),
				})
				resp.Body.Close()
				return nil
			}
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("多次发送失败: %w", lastErr)
}

// AddMessage 添加一条消息，超出maxSize自动删除最早的
func AddMessageLB(msg SavedMessageLB) {
	savedMessagesLB.Lock()
	defer savedMessagesLB.Unlock()

	if len(savedMessagesLB.messages) >= savedMessagesLB.maxSize {
		// 删除最早的一条，保持长度不变
		savedMessagesLB.messages = savedMessagesLB.messages[1:]
	}
	savedMessagesLB.messages = append(savedMessagesLB.messages, msg)
}

// GetLatestMessages 返回最新n条，倒序
func GetLatestMessagesLB(n int) []SavedMessageLB {
	savedMessagesLB.RLock()
	defer savedMessagesLB.RUnlock()

	total := len(savedMessagesLB.messages)
	if total == 0 {
		return nil
	}

	if n > total {
		n = total
	}

	res := make([]SavedMessageLB, n)
	for i := 0; i < n; i++ {
		res[i] = savedMessagesLB.messages[total-1-i]
	}
	return res
}
