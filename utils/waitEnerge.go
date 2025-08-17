package utils

import (
	"database/sql"
	"energe/telegram"
	"energe/types"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

type waitToken struct {
	Symbol    string
	Operation string
	Status    string
	AddedAt   time.Time
}

var waitMu sync.Mutex
var waitList = make(map[string]waitToken)

// sendWaitListBroadcast 用于主动推送等待区列表
func sendWaitListBroadcast(now time.Time, waiting_token, chatID string) {
	waitMu.Lock()
	defer waitMu.Unlock()

	if len(waitList) == 0 {
		telegram.SendMessageWaiting(waiting_token, chatID, "等待区为空")
		return
	}

	var msgBuilder strings.Builder

	var emoje string

	for _, token := range waitList {
		if token.Operation == "FomoBuy" {
			emoje = "🟢"
		} else if token.Operation == "FomoSell" {
			emoje = "🔴"
		} else if token.Operation == "Singu" {
			emoje = "🟣"
		} else {
			emoje = "-"
		}

		msgBuilder.WriteString(fmt.Sprintf("%s %-12s\n", emoje, token.Symbol))
	}
	msg := msgBuilder.String()
	log.Printf("📤 推送等待区更新列表，共 %d 个代币", len(waitList))
	telegram.SendMessageWaiting(waiting_token, chatID, msg)
}
func waitUntilNext5Min() time.Duration { //每5分钟监控
	now := time.Now()
	next := now.Truncate(time.Minute).Add(time.Duration(5-now.Minute()%5) * time.Minute)
	if next.Before(now) || next.Equal(now) {
		next = next.Add(5 * time.Minute)
	}
	return time.Until(next)
}
func WaitEnerge(resultsChan chan []types.CoinIndicator, db_trend *sql.DB, wait_sucess_token, chatID string, client *futures.Client, klinesCount int, waiting_token string) {
	go func() {
		time.Sleep(waitUntilNext5Min())
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for now := range ticker.C {
			time.Sleep(7 * time.Second)
			go func(now time.Time) {
				var changed bool // 是否发生了删除

				waitMu.Lock()
				waitCopy := make(map[string]waitToken)
				for k, v := range waitList {
					waitCopy[k] = v
				}
				waitMu.Unlock()

				for sym, token := range waitCopy {
					MACDM5, _ := GetTrendResult(db_trend, sym, "5m")
					MACDM15, _ := GetTrendResult(db_trend, sym, "15m")
					MACDH1, _ := GetTrendResult(db_trend, sym, "1h")
					//BuyMACDH4, _ := GetTrendResult(db, symbol, "4h")
					//BuyMACDD1, _ := GetTrendResult(db, symbol, "1d")
					//BuyMACDD3, _ := GetTrendResult(db, symbol, "3d")
					switch token.Operation {
					case "FomoBuy":
						if MACDH1 == "BUYMACD" && MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD" {
							msg := fmt.Sprintf("监控回响：🟢%s ", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🟢 等待成功 Buy : %s", sym)
						} else if MACDM15 == "SELLMACD" {
							log.Printf("❌ Wait失败 Buy : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "FomoSell":
						if MACDH1 == "SELLMACD" && MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD" {
							msg := fmt.Sprintf("监控回响：🔴%s", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🔴 等待成功 Sell : %s", sym)
						} else if MACDM15 == "BUYMACD" {
							log.Printf("❌ Wait失败 Sell : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "Singu":
						if MACDH1 == "BUYMACD" && MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD" {
							msg := fmt.Sprintf("监控回响：🟢%s ", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🟢 等待成功 Buy : %s", sym)
						} else if MACDH1 == "SELLMACD" && MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD" {
							msg := fmt.Sprintf("监控回响：🔴%s", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🔴 等待成功 Sell : %s", sym)
						} else if (MACDM15 == "SELLMACD") || (MACDM15 == "BUYMACD") {
							log.Printf("❌ Wait失败 Sell : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					}

					if now.Sub(token.AddedAt) > 8*time.Hour {
						log.Printf("⏰ Wait超时清理 : %s", sym)
						waitMu.Lock()
						delete(waitList, sym)
						waitMu.Unlock()
						changed = true
					}
				}

				if changed {
					sendWaitListBroadcast(now, waiting_token, chatID)
				}
			}(now)
		}
	}()

	// 接收新 results 并更新 waitList（逻辑不变）
	for newResults := range resultsChan {
		var newAdded bool
		now := time.Now()

		waitMu.Lock()
		for _, coin := range newResults {
			if coin.Status == "Wait" {
				exist, exists := waitList[coin.Symbol]
				if !exists {
					waitList[coin.Symbol] = waitToken{
						Symbol:    coin.Symbol,
						Operation: coin.Operation,
						Status:    coin.Status,
						AddedAt:   now,
					}
					log.Printf("✅ 添加或替换等待代币: %s", coin.Symbol)
					newAdded = true
				}
				if exists && exist.Operation != coin.Operation {
					waitList[coin.Symbol] = waitToken{
						Symbol:    coin.Symbol,
						Operation: coin.Operation,
						Status:    coin.Status,
						AddedAt:   now,
					}
					log.Printf("✅ 添加或替换等待代币: %s", coin.Symbol)
					newAdded = true
				}
			}
		}
		waitMu.Unlock()

		if newAdded {
			sendWaitListBroadcast(now, waiting_token, chatID)
		}
	}
}
