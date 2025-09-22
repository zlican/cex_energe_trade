package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"time"
)

var (
	alertBotToken = "7573473925:AAE1IbVhFTgOmhvgV61IkD25Qr9kkbgBgQo"
	alertChatID   = "6074996357"
)

// sendRawMessage 与 SendMessage 类似，但**不会**把发送记录加入 savedMessages，避免循环调用
func sendRawMessage(botToken, chatID, text string) error {
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

	message := Message{ChatID: chatID, Text: text}
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
			return nil
		}
		if err != nil {
			lastErr = fmt.Errorf("发送失败 (尝试 %d): %w", attempt, err)
		} else {
			lastErr = fmt.Errorf("非 200 返回 (尝试 %d): %s", attempt, resp.Status)
			resp.Body.Close()
		}
		log.Printf("[Telegram raw] ❌ %v，等待 %v 后重试", lastErr, backoff)
		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("最终发送失败: %w", lastErr)
}

// ------- 分析逻辑实现 -------

// symbolRegex 用于从文本中抽取交易对（例如 BTCUSDT）
var symbolRegex = regexp.MustCompile(`([A-Z0-9]{1,10}USDT)`) // 简单规则，覆盖大多数 USDT 交易对

// ExtractSymbol 从消息文本中提取交易对，返回 (symbol, true) 或 ("", false)
func ExtractSymbol(text string) (string, bool) {
	m := symbolRegex.FindStringSubmatch(text)
	if len(m) >= 2 {
		return m[1], true
	}
	return "", false
}

// AnalyzeNewMessage 对刚刚添加的消息进行“首次警报”判定：
// 规则：
//  1. 如果该交易对在历史 savedMessages 中从未出现过 -> 首次警报
//  2. 否则如果该交易对上一次出现时间距本条消息 >= 30 分钟 -> 首次警报
//
// 如果判定为首次警报，会调用配置好的 alert bot 发送一条通知（不会把该通知再写入 savedMessages）
func AnalyzeNewMessage(msg SavedMessage) {
	symbol, ok := ExtractSymbol(msg.Text)
	if !ok {
		// 无法提取交易对，忽略
		return
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// 复制一份 messages（在短锁内完成拷贝），避免在分析时持有锁或读到并发修改的不稳定状态
	savedMessages.RLock()
	copied := make([]SavedMessage, len(savedMessages.messages))
	copy(copied, savedMessages.messages)
	savedMessages.RUnlock()

	// 寻找除最后一条（刚加入的那条）之外的最近一次该交易对出现时间
	var lastPrev *SavedMessage
	if len(copied) >= 2 {
		for i := len(copied) - 2; i >= 0; i-- {
			if s, ok := ExtractSymbol(copied[i].Text); ok && s == symbol {
				lastPrev = &copied[i]
				break
			}
		}
	}

	isFirst := false
	reason := ""
	if lastPrev == nil {
		isFirst = true
		reason = "never_seen_before"
	} else {
		// 如果上一次出现距离当前消息 >= 30 分钟，则也视为首次警报
		if msg.Timestamp.Sub(lastPrev.Timestamp) >= 30*time.Minute {
			isFirst = true
			reason = "no_same_within_30m"
		}
	}

	if !isFirst {
		// 不是首次警报，结束
		return
	}

	// 配置检查
	if alertBotToken == "" || alertChatID == "" {
		log.Printf("[Alert] 首次警报 (%s) 被判定, 但 alert bot 未配置，消息: %s", symbol, msg.Text)
		return
	}

	// 构造告警文本（简洁）
	alertText := fmt.Sprintf("🔔 短线首次警报 — %s\n原始: %s\n时间: %s\n原因: %s", symbol, msg.Text, msg.Timestamp.Format(time.RFC3339), reason)

	if err := sendRawMessage(alertBotToken, alertChatID, alertText); err != nil {
		log.Printf("[Alert] 发送首次警报失败: %v", err)
	}
}

func AnalyzeNewMessageL(msg SavedMessageL) {
	symbol, ok := ExtractSymbol(msg.Text)
	if !ok {
		// 无法提取交易对，忽略
		return
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// 复制一份 messages（在短锁内完成拷贝），避免在分析时持有锁或读到并发修改的不稳定状态
	savedMessagesL.RLock()
	copied := make([]SavedMessageL, len(savedMessagesL.messages))
	copy(copied, savedMessagesL.messages)
	savedMessagesL.RUnlock()

	// 寻找除最后一条（刚加入的那条）之外的最近一次该交易对出现时间
	var lastPrev *SavedMessageL
	if len(copied) >= 2 {
		for i := len(copied) - 2; i >= 0; i-- {
			if s, ok := ExtractSymbol(copied[i].Text); ok && s == symbol {
				lastPrev = &copied[i]
				break
			}
		}
	}

	isFirst := false
	reason := ""
	if lastPrev == nil {
		isFirst = true
		reason = "never_seen_before"
	} else {
		// 如果上一次出现距离当前消息 >= 480 分钟，则也视为首次警报
		if msg.Timestamp.Sub(lastPrev.Timestamp) >= 480*time.Minute {
			isFirst = true
			reason = "no_same_within_480m"
		}
	}

	if !isFirst {
		// 不是首次警报，结束
		return
	}

	// 配置检查
	if alertBotToken == "" || alertChatID == "" {
		log.Printf("[Alert] 首次警报 (%s) 被判定, 但 alert bot 未配置，消息: %s", symbol, msg.Text)
		return
	}

	// 构造告警文本（简洁）
	alertText := fmt.Sprintf("🔔 中线首次警报 — %s\n原始: %s\n时间: %s\n原因: %s", symbol, msg.Text, msg.Timestamp.Format(time.RFC3339), reason)

	if err := sendRawMessage(alertBotToken, alertChatID, alertText); err != nil {
		log.Printf("[Alert] 发送首次警报失败: %v", err)
	}
}
