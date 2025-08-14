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
func WaitEnerge(resultsChan chan []types.CoinIndicator, db *sql.DB, wait_sucess_token, chatID string, client *futures.Client, klinesCount int, waiting_token string) {
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
					_, _, closes, err := GetKlinesByAPI(client, sym, "15m", klinesCount)
					if err != nil {
						log.Printf("❌ 获取K线失败: %s", sym)
						continue
					}
					price := closes[len(closes)-1]
					ema25M15, ema50M15, _ := Get15MEMAFromDB(db, sym)
					ema25H1, ema50H1 := Get1HEMAFromDB(db, sym)
					ema25M5, ema50M5 := Get5MEMAFromDB(db, sym)

					//动能模型
					var TrendUpH1, TrendUpM15, TrendDOWNH1, TrendDOWNM15 bool
					TrendUpH1 = price > ema25H1 && ema25H1 > ema50H1
					TrendDOWNH1 = price < ema25H1 && ema25H1 < ema50H1
					TrendUpM15 = price > ema25M15 && ema25M15 > ema50M15
					TrendDOWNM15 = price < ema25M15 && ema25M15 < ema50M15

					//MACD模型
					UpMACDM5, DownMACDM5, XUpMACDM5, XDownMACDM5 := GetMACDM5FromDB(db, sym)
					var BuyMACDM5, SellMACDM5 bool
					M5UPEMA := ema25M5 > ema50M5
					M5DOWNEMA := ema25M5 < ema50M5
					if M5UPEMA && price > ema25M5 && UpMACDM5 { //金叉浅回调
						BuyMACDM5 = true
					} else if M5UPEMA && price < ema25M5 && XUpMACDM5 { //金叉深回调
						BuyMACDM5 = true
					} else if M5DOWNEMA && price > ema25M5 && XUpMACDM5 { //死叉反转
						BuyMACDM5 = true
					} else if M5DOWNEMA && price < ema25M5 && DownMACDM5 {
						SellMACDM5 = true
					} else if M5DOWNEMA && price > ema25M5 && XDownMACDM5 {
						SellMACDM5 = true
					} else if M5UPEMA && price < ema25M5 && XDownMACDM5 {
						SellMACDM5 = true
					} else {
						BuyMACDM5 = false
						SellMACDM5 = false
					}
					switch token.Operation {
					case "FomoBuy":
						if !TrendDOWNH1 && !TrendDOWNM15 && BuyMACDM5 {
							msg := fmt.Sprintf("监控回响：🟣%s ", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🟣 等待成功 Buy : %s", sym)
							changed = true
						} else if TrendDOWNM15 {
							log.Printf("❌ Wait失败 Buy : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "FomoSell":
						if !TrendUpH1 && !TrendUpM15 && SellMACDM5 {
							msg := fmt.Sprintf("监控回响：🔴%s", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🔴 等待成功 Sell : %s", sym)
							changed = true
						} else if TrendUpM15 {
							log.Printf("❌ Wait失败 Sell : %s", sym)
							waitMu.Lock()
							delete(waitList, sym)
							waitMu.Unlock()
							changed = true
						}
					case "Singu":
						if !TrendDOWNH1 && !TrendDOWNM15 && BuyMACDM5 {
							msg := fmt.Sprintf("监控回响：🟢%s ", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🟢 等待成功 Buy : %s", sym)
							changed = true
						} else if !TrendUpH1 && !TrendUpM15 && SellMACDM5 {
							msg := fmt.Sprintf("监控回响：🔴%s", sym)
							telegram.SendMessage(wait_sucess_token, chatID, msg)
							log.Printf("🔴 等待成功 Sell : %s", sym)
							changed = true
						} else if TrendUpM15 || TrendDOWNM15 {
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
