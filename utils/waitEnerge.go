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
	Symbol              string
	Inst                string
	Operation           string
	Status              string
	Source              types.MarketSource
	AddedAt             time.Time
	LastPushedOperation string // 新增字段：记录最后一次推送的操作
	LastInvalidPushed   bool   // 新增字段：是否已经推送过失效消息
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

		msgBuilder.WriteString(fmt.Sprintf("%s %-36s(%s)\n", emoje, token.Symbol, token.Source))
	}
	msg := msgBuilder.String()
	log.Printf("📤 推送等待区更新列表，共 %d 个代币", len(waitList))
	telegram.SendMessageWaiting(waiting_token, chatID, msg)
}

func executeWaitCheck(db_trend *sql.DB, wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	time.Sleep(7 * time.Second) // 保持你原来的延迟

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
			var closesM15, closesM5 []float64
			if token.Source == types.SourceBinance {
				_, _, closesM5, _ = GetKlinesByAPI(client, sym, "5m", 200)
			} else if token.Source == types.SourceOKX {
				_, _, closesM5, _ = GetKlinesByAPI_OKX(token.Inst, "5m", 200)
			}
			price := closesM5[len(closesM5)-1]
			ma60 = CalculateMA(closesM5, 60)
			ema25 := CalculateEMA(closesM5, 25)
			ema25Now = ema25[len(ema25)-1]
			if price > ma60 && price > ema25Now {
				MACDM5 = "BUYMACD"
			} else if price < ma60 && price < ema25Now {
				MACDM5 = "SELLMACD"
			}
			if token.Source == types.SourceBinance {
				_, _, closesM15, _ = GetKlinesByAPI(client, sym, "15m", 200)
			} else if token.Source == types.SourceOKX {
				_, _, closesM15, _ = GetKlinesByAPI_OKX(token.Inst, "15m", 200)
			}
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
				if token.LastPushedOperation != "BEBUY" {
					msg := fmt.Sprintf("🟢做多：🟢%s ", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					waitMu.Lock()
					t := waitList[sym]
					t.LastPushedOperation = "BEBUY"
					t.LastInvalidPushed = false // 重置失效推送标志
					waitList[sym] = t
					waitMu.Unlock()
				}
			} else if MACDM15 != "BUYMACD" {
				log.Printf("❌ Wait失败 Sell : %s", sym)
				waitMu.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitList[sym]
				if t.LastPushedOperation == "BEBUY" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitList[sym] = t
				}
				delete(waitList, sym) // 删除
				waitMu.Unlock()
				changed = true
			} else {
				log.Printf("❌ 信号失效，重置状态: %s", sym)
				waitMu.Lock()
				t := waitList[sym]
				if t.LastPushedOperation == "BEBUY" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
				}
				t.LastPushedOperation = "" // 清空，允许下次推送
				t.LastInvalidPushed = true
				waitList[sym] = t
				waitMu.Unlock()
			}
		case "BESELL":
			if MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD" {
				// 如果上次推送过相同方向，就不推送
				if token.LastPushedOperation != "BESELL" {
					msg := fmt.Sprintf("🔴做空：🔴%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)

					// 更新状态
					waitMu.Lock()
					t := waitList[sym]
					t.LastPushedOperation = "BESELL"
					waitList[sym] = t
					waitMu.Unlock()
				}
			} else if MACDM15 != "SELLMACD" {
				log.Printf("❌ Wait失败 Sell : %s", sym)
				waitMu.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitList[sym]
				if t.LastPushedOperation == "BESELL" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitList[sym] = t
				}
				delete(waitList, sym) // 删除
				waitMu.Unlock()
				changed = true
			} else {
				log.Printf("❌ 信号失效，重置状态: %s", sym)
				waitMu.Lock()
				t := waitList[sym]
				if t.LastPushedOperation == "BESELL" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
				}
				t.LastPushedOperation = "" // 清空，允许下次推送
				t.LastInvalidPushed = true
				waitList[sym] = t
				waitMu.Unlock()
			}
		case "OTBUY":
			if MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD" {
				if token.LastPushedOperation != "OTBUY" {
					msg := fmt.Sprintf("🟢做多：🟢%s ", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					waitMu.Lock()
					t := waitList[sym]
					t.LastPushedOperation = "OTBUY"
					waitList[sym] = t
					waitMu.Unlock()
				}
			} else if MACDM15 != "BUYMACD" {
				log.Printf("❌ Wait失败 Sell : %s", sym)
				waitMu.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitList[sym]
				if t.LastPushedOperation == "OTBUY" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitList[sym] = t
				}
				delete(waitList, sym) // 删除
				waitMu.Unlock()
				changed = true
			} else {
				log.Printf("❌ 信号失效，重置状态: %s", sym)
				waitMu.Lock()
				t := waitList[sym]
				if t.LastPushedOperation == "OTBUY" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
				}
				t.LastPushedOperation = "" // 清空，允许下次推送
				t.LastInvalidPushed = true
				waitList[sym] = t
				waitMu.Unlock()
			}
		case "OTSELL":
			if MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD" {
				if token.LastPushedOperation != "OTSELL" {
					msg := fmt.Sprintf("🔴做空：🔴%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)

					// 更新状态
					waitMu.Lock()
					t := waitList[sym]
					t.LastPushedOperation = "OTSELL"
					waitList[sym] = t
					waitMu.Unlock()
				}
			} else if MACDM15 != "SELLMACD" {
				log.Printf("❌ Wait失败 Sell : %s", sym)
				waitMu.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitList[sym]
				if t.LastPushedOperation == "OTSELL" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitList[sym] = t
				}
				delete(waitList, sym) // 删除
				waitMu.Unlock()
				changed = true
			} else {
				log.Printf("❌ 信号失效，重置状态: %s", sym)
				waitMu.Lock()
				t := waitList[sym]
				if t.LastPushedOperation == "OTSELL" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
				}
				t.LastPushedOperation = "" // 清空，允许下次推送
				t.LastInvalidPushed = true
				waitList[sym] = t
				waitMu.Unlock()
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
		// 🚀 启动时立即执行一次
		now := time.Now()
		executeWaitCheck(db_trend, wait_sucess_token, chatID, client, waiting_token, now)

		// 先等到下一个 5 分钟整点
		time.Sleep(waitUntilNext5Min())

		// 然后每 5 分钟一次（分钟 % 5 == 0）
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for now := range ticker.C {
			go executeWaitCheck(db_trend, wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()
	// 接收新 results 并更新 waitList
	for newResults := range resultsChan {
		var newAdded bool
		now := time.Now()

		waitMu.Lock()
		// 检查当前 waitList 是否包含 BTC 或 ETH
		hasBTC := false
		hasETH := false
		for sym := range waitList {
			if sym == "BTCUSDT" {
				hasBTC = true
			}
			if sym == "ETHUSDT" {
				hasETH = true
			}
		}

		// 如果新结果里有 BTC/ETH，就强制清理掉其他代币
		incomingHasBTC := false
		incomingHasETH := false
		for _, coin := range newResults {
			if coin.Symbol == "BTCUSDT" {
				incomingHasBTC = true
			}
			if coin.Symbol == "ETHUSDT" {
				incomingHasETH = true
			}
		}

		if incomingHasBTC || incomingHasETH {
			// 🚮 清理掉所有非BTC/ETH代币
			for sym := range waitList {
				if sym != "BTCUSDT" && sym != "ETHUSDT" {
					log.Printf("🧹 清理非BTC/ETH代币: %s", sym)
					delete(waitList, sym)
				}
			}
		}

		// 再按规则添加/更新
		for _, coin := range newResults {
			// 如果已有 BTC/ETH，忽略其他代币
			if (hasBTC || hasETH || incomingHasBTC || incomingHasETH) &&
				(coin.Symbol != "BTCUSDT" && coin.Symbol != "ETHUSDT") {
				log.Printf("⏭ 忽略非BTC/ETH代币: %s，因为等待区已有BTC或ETH", coin.Symbol)
				continue
			}

			exist, exists := waitList[coin.Symbol]
			if !exists {
				waitList[coin.Symbol] = waitToken{
					Symbol:    coin.Symbol,
					Inst:      coin.Inst,
					Operation: coin.Operation,
					Status:    coin.Status,
					Source:    coin.Source,
					AddedAt:   now,
				}
				log.Printf("✅ 添加等待代币: %s", coin.Symbol)
				newAdded = true
			}
			if exists && exist.Operation != coin.Operation {
				waitList[coin.Symbol] = waitToken{
					Symbol:    coin.Symbol,
					Inst:      coin.Inst,
					Operation: coin.Operation,
					Status:    coin.Status,
					Source:    coin.Source,
					AddedAt:   now,
				}
				log.Printf("♻️ 更新等待代币: %s", coin.Symbol)
				newAdded = true
			}
		}
		waitMu.Unlock()

		if newAdded {
			sendWaitListBroadcast(now, waiting_token, chatID)
		}
	}
}
