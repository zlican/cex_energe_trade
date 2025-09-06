package utils

import (
	"database/sql"
	"energe/telegram"
	"energe/types"
	"fmt"
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

		msgBuilder.WriteString(fmt.Sprintf("%s %-36s\n", emoje, token.Symbol))
	}
	msg := msgBuilder.String()
	telegram.SendMessageWaiting(waiting_token, chatID, msg)
}

func executeWaitCheck(db_trend *sql.DB, wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	// 使用 defer 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			fmt.Printf("[executeWaitCheck] Panic recovered \n")
			// 返回默认值，表示处理失败
			// 你也可以根据需求记录到日志文件或监控系统
		}
	}()
	time.Sleep(7 * time.Second) // 保持你原来的延迟

	var changed bool // 是否发生了删除

	waitMu.Lock()
	waitCopy := make(map[string]waitToken)
	for k, v := range waitList {
		waitCopy[k] = v
	}
	waitMu.Unlock()

	for sym, token := range waitCopy {
		var MACDM5, MACDM15, MACDH1 string

		if sym == "BTCUSDT" || sym == "ETHUSDT" {
			MACDM5, _ = GetTrendResult(db_trend, sym, "5m")
			MACDM15, _ = GetTrendResult(db_trend, sym, "15m")
			MACDH1, _ = GetTrendResult(db_trend, sym, "1h")
			//BuyMACDH4, _ := GetTrendResult(db, symbol, "4h")
			//BuyMACDD1, _ := GetTrendResult(db, symbol, "1d")
			//BuyMACDD3, _ := GetTrendResult(db, symbol, "3d")
		} else {
			var closesM15, closesM5, closesH1 []float64
			var err error
			closesM15, err = GetClosesWithFallback(client, sym, "15m")
			if err != nil {
				fmt.Println("获取数据失败:", err)
			}
			price := closesM15[len(closesM15)-1]
			DIFUPM15 := IsDIFUP(closesM15, 6, 13, 5)
			DIFDOWNM15 := IsDIFDOWN(closesM15, 6, 13, 5)
			ma60M15 := CalculateMA(closesM15, 60)
			ema25M15 := CalculateEMA(closesM15, 25)
			ema25M15now := ema25M15[len(ema25M15)-1]
			if price > ema25M15now && price > ma60M15 && DIFUPM15 {
				MACDM15 = "BUYMACD"
			} else if price < ema25M15now && price < ma60M15 && DIFDOWNM15 {
				MACDM15 = "SELLMACD"
			}
			XSTRONGUPM15 := XSTRONGUP(closesM15, 6, 13, 5)
			if XSTRONGUPM15 && price > ma60M15 {
				MACDM15 = "XBUYMID"
			}
			XSTRONGDOWNM15 := XSTRONGDOWN(closesM15, 6, 13, 5)
			if XSTRONGDOWNM15 && price < ma60M15 {
				MACDM15 = "XSELLMID"
			}
			//5分钟小时
			closesM5, err = GetClosesWithFallback(client, sym, "5m")
			if err != nil {
				fmt.Println("获取数据失败:", err)
			}
			ma60M5 := CalculateMA(closesM5, 60)
			XSTRONGUPM5 := XSTRONGUP(closesM5, 6, 13, 5)
			XSTRONGDOWNM5 := XSTRONGDOWN(closesM5, 6, 13, 5)
			if price > ma60M5 && XSTRONGUPM5 {
				MACDM5 = "BUYMACD"
			} else if price < ma60M5 && XSTRONGDOWNM5 {
				MACDM5 = "SELLMACD"
			}
			//1小时大时
			closesH1, err = GetClosesWithFallback(client, sym, "1h")
			if err != nil {
				fmt.Println("获取数据失败:", err)
			}
			ema25H1 := CalculateEMA(closesH1, 25)
			ema25H1Now := ema25H1[len(ema25H1)-1]
			ma60H1 := CalculateMA(closesH1, 60)
			DIFUPH1 := IsDIFUP(closesH1, 6, 13, 5)
			DIFDOWNH1 := IsDIFDOWN(closesH1, 6, 13, 5)
			if price > ema25H1Now && price > ma60H1 && DIFUPH1 {
				MACDH1 = "BUYMACD"
			} else if price < ema25H1Now && price < ma60H1 && DIFDOWNH1 {
				MACDH1 = "SELLMACD"
			}
		}
		switch token.Operation {
		case "BEBUY":
			if MACDH1 == "BUYMACD" && ((MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD") || MACDM15 == "XBUYMID") {
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
			} else if MACDM15 != "BUYMACD" && MACDM15 != "XBUYMID" {
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
			if MACDH1 == "SELLMACD" && ((MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD") || MACDM15 == "XSELLMID") {
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
			} else if MACDM15 != "SELLMACD" && MACDM15 != "XSELLMID" {
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
			if MACDH1 == "BUYMACD" && ((MACDM15 == "BUYMACD" && MACDM5 == "BUYMACD") || MACDM15 == "XBUYMID") {
				if token.LastPushedOperation != "OTBUY" {
					msg := fmt.Sprintf("🟢做多：🟢%s ", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					waitMu.Lock()
					t := waitList[sym]
					t.LastPushedOperation = "OTBUY"
					waitList[sym] = t
					waitMu.Unlock()
				}
			} else if MACDM15 != "BUYMACD" && MACDM15 != "XBUYMID" {
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
			if MACDH1 == "SELLMACD" && ((MACDM15 == "SELLMACD" && MACDM5 == "SELLMACD") || MACDM15 == "XSELLMID") {
				if token.LastPushedOperation != "OTSELL" {
					msg := fmt.Sprintf("🔴做空：🔴%s", sym)
					telegram.SendMessage(wait_sucess_token, chatID, msg)
					waitMu.Lock()
					t := waitList[sym]
					t.LastPushedOperation = "OTSELL"
					waitList[sym] = t
					waitMu.Unlock()
				}
			} else if MACDM15 != "SELLMACD" && MACDM15 != "XSELLMID" {
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
func WaitEnerge(
	resultsChan chan []types.CoinIndicator,
	db_trend *sql.DB,
	wait_sucess_token, chatID string,
	client *futures.Client,
	klinesCount int,
	waiting_token string,
) {
	go func() {
		// 🚀 先消费一次已有消息，保证 waitList 不为空
		drainResults(resultsChan, waiting_token, chatID)

		// 再执行首次检测
		now := time.Now()
		executeWaitCheck(db_trend, wait_sucess_token, chatID, client, waiting_token, now)

		// 等到下一个 5 分钟整点
		time.Sleep(waitUntilNext5Min())

		// 每 5 分钟触发
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for now := range ticker.C {
			go executeWaitCheck(db_trend, wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()

	// 常规消费
	for newResults := range resultsChan {
		addToWaitList(newResults, waiting_token, chatID)
	}
}

// ================== 辅助函数 ==================

// 启动时先 drain 一次通道（非阻塞，防止残留）
func drainResults(resultsChan chan []types.CoinIndicator, waiting_token, chatID string) {
	for {
		select {
		case newResults := <-resultsChan:
			addToWaitList(newResults, waiting_token, chatID)
		default:
			return
		}
	}
}

/* // 公共的添加逻辑（含 BTC/ETH 优先规则）
func addToWaitListBYBE(newResults []types.CoinIndicator, waiting_token, chatID string) {
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
} */

// 公共的添加逻辑（含 BTC/ETH 优先规则）
func addToWaitList(newResults []types.CoinIndicator, waiting_token, chatID string) {
	var newAdded bool
	now := time.Now()

	waitMu.Lock()
	for _, coin := range newResults {

		exist, exists := waitList[coin.Symbol]
		if !exists {
			waitList[coin.Symbol] = waitToken{
				Symbol:    coin.Symbol,
				Inst:      coin.Inst,
				Operation: coin.Operation,
				Status:    coin.Status,
				AddedAt:   now,
			}
			newAdded = true
		}
		if exists && exist.Operation != coin.Operation {
			waitList[coin.Symbol] = waitToken{
				Symbol:    coin.Symbol,
				Inst:      coin.Inst,
				Operation: coin.Operation,
				Status:    coin.Status,
				AddedAt:   now,
			}
			newAdded = true
		}
	}
	waitMu.Unlock()

	if newAdded {
		sendWaitListBroadcast(now, waiting_token, chatID)
	}
}
