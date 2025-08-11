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
	savedMessagesWaiting = struct {
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

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
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

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonMessage))
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 response: %s", resp.Status)
	}

	AddMessage(SavedMessage{
		Text:      text,
		Timestamp: time.Now(),
	})

	return nil
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

func SendMessageWaiting(botToken, chatID, text string) error {
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

	message := Message{
		ChatID: chatID,
		Text:   text,
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonMessage))
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 response: %s", resp.Status)
	}

	AddMessageWaiting(SavedMessage{
		Text:      text,
		Timestamp: time.Now(),
	})

	return nil
}

// AddMessage 添加一条消息，超出maxSize自动删除最早的
func AddMessageWaiting(msg SavedMessage) {
	savedMessagesWaiting.Lock()
	defer savedMessagesWaiting.Unlock()

	if len(savedMessagesWaiting.messages) >= savedMessagesWaiting.maxSize {
		// 删除最早的一条，保持长度不变
		savedMessagesWaiting.messages = savedMessagesWaiting.messages[1:]
	}
	savedMessagesWaiting.messages = append(savedMessagesWaiting.messages, msg)
}

// GetLatestMessages 返回最新1条，倒序
func GetLatestMessagesWaiting(n int) []SavedMessage {
	savedMessagesWaiting.RLock()
	defer savedMessagesWaiting.RUnlock()

	total := len(savedMessagesWaiting.messages)
	if total == 0 {
		return nil
	}

	if n > total {
		n = total
	}

	res := make([]SavedMessage, n)
	for i := 0; i < n; i++ {
		res[i] = savedMessagesWaiting.messages[total-1-i]
	}
	return res
}
