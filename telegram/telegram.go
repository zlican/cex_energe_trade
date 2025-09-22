package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const telegramAPIURL = "https://api.telegram.org/bot"

type Message struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

// SavedMessage 代表保存的已发送消息（含时间）
type SavedMessage struct {
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	savedMessages = struct {
		sync.RWMutex
		messages []SavedMessage
		maxSize  int
	}{
		messages: make([]SavedMessage, 0, 100),
		maxSize:  100,
	}
)

func SendMessage(botToken, chatID, text string) error {
	proxy := "http://127.0.0.1:10809"
	proxyURL, err := url.Parse(proxy)
	if err != nil {
		return fmt.Errorf("解析代理地址失败: %w", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
		},
		Timeout: 10 * time.Second,
	}
	url := fmt.Sprintf("%s%s/sendMessage", telegramAPIURL, botToken)

	message := Message{
		ChatID: chatID,
		Text:   text,
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	backoff := 1 * time.Second
	var lastErr error

	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonMessage))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			// 记录已发送消息
			AddMessage(SavedMessage{
				Text:      text,
				Timestamp: time.Now(),
			})
			return nil
		}

		if err != nil {
			lastErr = fmt.Errorf("发送失败 (尝试 %d): %w", attempt, err)
		} else {
			lastErr = fmt.Errorf("非 200 返回 (尝试 %d): %s", attempt, resp.Status)
			resp.Body.Close()
		}

		log.Printf("[Telegram] ❌ %v，等待 %v 后重试", lastErr, backoff)
		time.Sleep(backoff)
		backoff *= 2 // 指数退避
	}

	return fmt.Errorf("最终发送失败: %w", lastErr)
}

// AddMessage 添加一条消息，超出maxSize自动删除最早的
func AddMessage(msg SavedMessage) {
	savedMessages.Lock()
	defer savedMessages.Unlock()

	if len(savedMessages.messages) >= savedMessages.maxSize {
		// 删除最早的一条，保持长度不变
		savedMessages.messages = savedMessages.messages[1:]
	}
	savedMessages.messages = append(savedMessages.messages, msg)

	// 异步分析刚刚添加的消息（非阻塞）
	go AnalyzeNewMessage(msg)
}

// GetLatestMessages 返回最新n条，倒序
func GetLatestMessages(n int) []SavedMessage {
	savedMessages.RLock()
	defer savedMessages.RUnlock()

	total := len(savedMessages.messages)
	if total == 0 {
		return nil
	}

	if n > total {
		n = total
	}

	res := make([]SavedMessage, n)
	for i := 0; i < n; i++ {
		res[i] = savedMessages.messages[total-1-i]
	}
	return res
}
