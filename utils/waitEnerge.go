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
		if token.Operation == "BEBUY" || token.Operation == "OTBUY" {
			emoje = "🟢🟢"
		} else if token.Operation == "BESELL" || token.Operation == "OTSELL" {
			emoje = "🔴🔴"
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
					var MACDM5, MACDM15 string
					var ema25Now, ma60 float64

					if sym == "BTCUSDT" || sym == "ETHUSDT" {
						MACDM5, _ = GetTrendResult(db_trend, sym, "5m")
						MACDM15, _ = GetTrendResult(db_trend, sym, "15m")
						//MACDH1, _ = GetTrendResult(db_trend, sym, "1h")
						//BuyMACDH4, _ := GetTrendResult(db, symbol, "4h")
						//BuyMACDD1, _ := GetTrendResult(db, symbol, "1d")
						//BuyMACDD3, _ := GetTrendResult(db, symbol, "3d")
					} else {
						_, _, closesM5, _ := GetKlinesByAPI(client, sym, "5m", 200)
						price := closesM5[len(closesM5)-1]
						ma60 = CalculateMA(closesM5, 60)
						ema25 := CalculateEMA(closesM5, 25)
						ema25Now = ema25[len(ema25)-1]
						if price > ma60 && price > ema25Now {
							MACDM5 = "BUYMACD"
						} else if price < ma60 && price < ema25Now {
							MACDM5 = "SELLMACD"
						}
						_, _, closesM15, _ := GetKlinesByAPI(client, sym, "15m", 200)
						DEAUP := IsDEAUP(closesM15, 6, 13, 5)
						DEADOWN := IsDEADOWN(closesM15, 6, 13, 5)
						ema25M15 := CalculateEMA(closesM15, 25)
						if price > ema25M15[len(ema25M15)-1] && DEAUP {
							MACDM15 = "BUYMACD"
						} else if price < ema25M15[len(ema25M15)-1] && DEADOWN {
							MACDM15 = "SELLMACD"
						} else {
							continue
						}
					}
					switch token.Operation {
					case "BEBUY":
						if MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD" {
							msg := fmt.Sprintf("🟢做多：🟢%s ", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
						} else if MACDM15 != "BUYMACD" {
							log.Printf("❌ Wait失败 Buy : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "BESELL":
						if MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD" {
							msg := fmt.Sprintf("🔴做空：🔴%s", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
						} else if MACDM15 != "SELLMACD" {
							log.Printf("❌ Wait失败 Sell : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "OTBUY":
						if MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD" {
							msg := fmt.Sprintf("🟢做多：🟢%s ", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
						} else if MACDM15 != "BUYMACD" {
							log.Printf("❌ Wait失败 Sell : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "OTSELL":
						if MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD" {
							msg := fmt.Sprintf("🔴做空：🔴%s", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
						} else if MACDM15 != "SELLMACD" {
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
		waitMu.Unlock()

		if newAdded {
			sendWaitListBroadcast(now, waiting_token, chatID)
		}
	}
}
