package utils

import (
	"database/sql"
	"energe/telegram"
	"fmt"
	"log"
	"strings"
	"time"

	"energe/types"
)

func PushTelegram(results []types.CoinIndicator, botToken, high_profit_srsi_botToken, chatID string, volumeCache *types.VolumeCache, db *sql.DB, betrend types.BETrend) error {
	now := time.Now().Format("2006-01-02 15:04")
	var msgBuilder strings.Builder

	/* 	// ---------- 添加主趋势播报 ----------
	   	var btcLine, ethLine string

	   	switch betrend.BTC {
	   	case "up":
	   		btcLine = "🟢 BTC趋势：强势上涨"
	   	case "down":
	   		btcLine = "🔴 BTC趋势：强势下跌"
	   	default:
	   		btcLine = "⚪️ BTC趋势：随机漫步"
	   	}

	   	switch betrend.ETH {
	   	case "up":
	   		ethLine = "🟢 ETH趋势：强势上涨"
	   	case "down":
	   		ethLine = "🔴 ETH趋势：强势下跌"
	   	default:
	   		ethLine = "⚪️ ETH趋势：随机漫步"
	   	} */

	msgBuilder.WriteString(fmt.Sprintf("🎁Time：%s\n", now))

	for _, r := range results {
		operation := r.Operation
		if r.Status == "Wait" {
			continue
		}
		volume, ok := volumeCache.Get(r.Symbol)
		if !ok || volume < 200000000 {
			continue
		}

		var line string
		if operation == "Buy" {
			if r.Symbol == "BTCUSDT" || r.Symbol == "ETHUSDT" || r.Symbol == "SOLUSDT" {
				line = fmt.Sprintf("💎🟢%-4s %-10s (%4s)", r.Operation, r.Symbol, r.Status)
			} else {
				line = fmt.Sprintf("🟢%-4s %-10s (%4s)", r.Operation, r.Symbol, r.Status)
			}
		} else if operation == "Sell" {
			if r.Symbol == "BTCUSDT" || r.Symbol == "ETHUSDT" || r.Symbol == "SOLUSDT" {
				line = fmt.Sprintf("💎🔴%-4s %-10s (%4s)", r.Operation, r.Symbol, r.Status)
			} else {
				line = fmt.Sprintf("🔴%-4s %-10s (%4s)", r.Operation, r.Symbol, r.Status)
			}
		} else if operation == "LongBuy" {
			line = fmt.Sprintf("🟢%-4s %-10s (%4s)", r.Operation, r.Symbol, r.Status)
		} else if operation == "LongSell" {
			line = fmt.Sprintf("🔴%-4s %-10s (%4s)", r.Operation, r.Symbol, r.Status)
		} else {
			continue // 忽略非 Buy/Sell 操作
		}

		msgBuilder.WriteString(line + "\n")
	}

	msg := msgBuilder.String()
	if strings.TrimSpace(msg) == "" {
		log.Println("📭 无需推送 Telegram 消息")
		return nil
	}

	if err := sendWithRetry(botToken, chatID, msg); err != nil {
		log.Printf("发送合并消息失败: %v", err)
	}
	return nil
}

// sendWithRetry 封装了带一次重试的 Telegram 发送逻辑
func sendWithRetry(botToken, chatID, msg string) error {
	err := telegram.SendMessage(botToken, chatID, msg)
	if err != nil {
		time.Sleep(2 * time.Second)
		err = telegram.SendMessage(botToken, chatID, msg)
	}
	return err
}
