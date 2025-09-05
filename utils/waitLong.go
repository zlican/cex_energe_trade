package utils

import (
	"energe/telegram"
	"energe/types"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

type waitTokenL struct {
	Symbol              string
	Inst                string
	Operation           string
	Status              string
	Source              types.MarketSource
	AddedAt             time.Time
	LastPushedOperation string // 新增字段：记录最后一次推送的操作
	LastInvalidPushed   bool   // 新增字段：是否已经推送过失效消息
}

var waitMuL sync.Mutex
var waitListL = make(map[string]waitTokenL)

// sendWaitListBroadcast 用于主动推送等待区列表
func sendWaitListBroadcastL(now time.Time, waiting_token, chatID string) {
	waitMuL.Lock()
	defer waitMuL.Unlock()

	if len(waitListL) == 0 {
		telegram.SendMessageWaitingL(waiting_token, chatID, "等待区为空")
		return
	}

	var msgBuilder strings.Builder

	var emoje string

	for _, token := range waitListL {
		if token.Operation == "BUYLong" {
			emoje = "🟢🟢"
		} else if token.Operation == "SELLLong" {
			emoje = "🔴🔴"
		} else {
			emoje = "-"
		}

		msgBuilder.WriteString(fmt.Sprintf("%s %-36s\n", emoje, token.Symbol))
	}
	msg := msgBuilder.String()
	log.Printf("📤 推送等待区更新列表，共 %d 个代币", len(waitListL))
	telegram.SendMessageWaitingL(waiting_token, chatID, msg)
}

func executeWaitCheckL(wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	// 使用 defer 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			fmt.Printf("[executeWaitCheckL] Panic recovered \n")
			// 返回默认值，表示处理失败
			// 你也可以根据需求记录到日志文件或监控系统
		}
	}()
	time.Sleep(7 * time.Second) // 保持你原来的延迟

	var changed bool // 是否发生了删除

	waitMuL.Lock()
	waitCopy := make(map[string]waitTokenL)
	for k, v := range waitListL {
		waitCopy[k] = v
	}
	waitMuL.Unlock()

	for sym, token := range waitCopy {
		var MACDH4, MACDD1, MACDD3 string

		var closesD3, closesD1, closesH4 []float64
		if token.Source == types.MarketBinance {
			_, _, closesD1, _ = GetKlinesByAPI(client, sym, "1d", 200)
		} else if token.Source == types.MarketOKX {
			_, _, closesD1, _ = GetKlinesByAPI_OKX(token.Inst, "1d", 200)
		}
		price := closesD1[len(closesD1)-1]
		DEAUP := IsDEAUP(closesD1, 6, 13, 5)
		DEADOWN := IsDEADOWN(closesD1, 6, 13, 5)
		ma60D1 := CalculateMA(closesD1, 60)
		ema25D1 := CalculateEMA(closesD1, 25)
		ema25D1now := ema25D1[len(ema25D1)-1]
		if price > ema25D1now && price > ma60D1 && DEAUP {
			MACDD1 = "BUYMACD"
		} else if price < ema25D1now && price < ma60D1 && DEADOWN {
			MACDD1 = "SELLMACD"
		}
		XSTRONGUPD1 := XSTRONGUP(closesD1, 6, 13, 5)
		if XSTRONGUPD1 && price > ma60D1 {
			MACDD1 = "XBUYMID"
		}
		XSTRONGDOWND1 := XSTRONGDOWN(closesD1, 6, 13, 5)
		if XSTRONGDOWND1 && price < ma60D1 {
			MACDD1 = "XSELLMID"
		}
		if token.Source == types.MarketBinance {
			_, _, closesH4, _ = GetKlinesByAPI(client, sym, "4h", 200)
		} else if token.Source == types.MarketOKX {
			_, _, closesH4, _ = GetKlinesByAPI_OKX(token.Inst, "4h", 200)
		}
		ma60H4 := CalculateMA(closesH4, 60)
		XSTRONGUPH4 := XSTRONGUP(closesH4, 6, 13, 5)
		XSTRONGDOWNH4 := XSTRONGDOWN(closesH4, 6, 13, 5)
		if price > ma60H4 && XSTRONGUPH4 {
			MACDH4 = "BUYMACD"
		} else if price < ma60H4 && XSTRONGDOWNH4 {
			MACDH4 = "SELLMACD"
		}
		//1小时大时
		if token.Source == types.MarketBinance {
			_, _, closesD3, _ = GetKlinesByAPI(client, sym, "3d", 200)
		} else if token.Source == types.MarketOKX {
			_, _, closesD3, _ = GetKlinesByAPI_OKX(token.Inst, "3d", 200)
		}
		ema25D3 := CalculateEMA(closesD3, 25)
		ema25D3Now := ema25D3[len(ema25D3)-1]
		ma60D3 := CalculateMA(closesD3, 60)
		DEAUPD3 := IsDEAUP(closesD3, 6, 13, 5)
		DEADOWND3 := IsDEADOWN(closesD3, 6, 13, 5)

		if DEAUPD3 && price > ema25D3Now && price > ma60D3 {
			MACDD3 = "BUYMACD"
		} else if DEADOWND3 && price < ema25D3Now && price < ma60D3 {
			MACDD3 = "SELLMACD"
		}

		switch token.Operation {
		case "BUYLong":
			if MACDD3 == "BUYMACD" && ((MACDD1 == "BUYMACD" && MACDH4 == "BUYMACD") || MACDD1 == "XBUYMID") {
				if token.LastPushedOperation != "BUYLong" {
					msg := fmt.Sprintf("🟢做多：🟢%s ", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
					waitMuL.Lock()
					t := waitListL[sym]
					t.LastPushedOperation = "BUYLong"
					t.LastInvalidPushed = false // 重置失效推送标志
					waitListL[sym] = t
					waitMuL.Unlock()
				}
			} else if MACDD1 != "BUYMACD" && MACDD1 != "XBUYMID" {
				waitMuL.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitListL[sym]
				if t.LastPushedOperation == "BUYLong" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitListL[sym] = t
				}
				delete(waitListL, sym) // 删除
				waitMuL.Unlock()
				changed = true
			} else {
				waitMuL.Lock()
				t := waitListL[sym]
				if t.LastPushedOperation == "BUYLong" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
				}
				t.LastPushedOperation = "" // 清空，允许下次推送
				t.LastInvalidPushed = true
				waitListL[sym] = t
				waitMuL.Unlock()
			}
		case "SELLLong":
			if MACDD3 == "SELLMACD" && ((MACDD1 == "SELLMACD" && MACDH4 == "SELLMACD") || MACDD1 == "XSELLMID") {
				// 如果上次推送过相同方向，就不推送
				if token.LastPushedOperation != "SELLLong" {
					msg := fmt.Sprintf("🔴做空：🔴%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)

					// 更新状态
					waitMuL.Lock()
					t := waitListL[sym]
					t.LastPushedOperation = "SELLLong"
					waitListL[sym] = t
					waitMuL.Unlock()
				}
			} else if MACDD1 != "SELLMACD" && MACDD1 != "XSELLMID" {
				waitMuL.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitListL[sym]
				if t.LastPushedOperation == "SELLLong" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitListL[sym] = t
				}
				delete(waitListL, sym) // 删除
				waitMuL.Unlock()
				changed = true
			} else {
				waitMuL.Lock()
				t := waitListL[sym]
				if t.LastPushedOperation == "SELLLong" && !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
				}
				t.LastPushedOperation = "" // 清空，允许下次推送
				t.LastInvalidPushed = true
				waitListL[sym] = t
				waitMuL.Unlock()
			}
			if now.Sub(token.AddedAt) > 48*time.Hour {
				waitMuL.Lock()
				delete(waitListL, sym)
				waitMuL.Unlock()
				changed = true
			}
		}
		if changed {
			sendWaitListBroadcastL(now, waiting_token, chatID)
		}
	}
}

// waitUntilNextHour 计算等待时间直到下一小时整点
func waitUntilNextHour() time.Duration {
	now := time.Now()
	next := now.Truncate(time.Hour).Add(time.Hour)
	return time.Until(next)
}

func WaitEnergeL(
	resultsChanLong chan []types.CoinIndicator,
	wait_sucess_token, chatID string,
	client *futures.Client,
	klinesCount int,
	waiting_token string,
) {
	go func() {
		// 🚀 先消费一次已有消息，保证 waitList 不为空
		drainResultsL(resultsChanLong, waiting_token, chatID)

		// 再执行首次检测
		now := time.Now()
		executeWaitCheckL(wait_sucess_token, chatID, client, waiting_token, now)

		time.Sleep(waitUntilNextHour())
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for now := range ticker.C {
			go executeWaitCheckL(wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()

	// 常规消费
	for newResults := range resultsChanLong {
		addToWaitListL(newResults, waiting_token, chatID)
	}
}

// ================== 辅助函数 ==================

// 启动时先 drain 一次通道（非阻塞，防止残留）
func drainResultsL(resultsChan chan []types.CoinIndicator, waiting_token, chatID string) {
	for {
		select {
		case newResults := <-resultsChan:
			addToWaitListL(newResults, waiting_token, chatID)
		default:
			return
		}
	}
}

// 公共的添加逻辑（含 BTC/ETH 优先规则）
func addToWaitListL(newResults []types.CoinIndicator, waiting_token, chatID string) {
	var newAdded bool
	now := time.Now()
	waitMuL.Lock()

	// 再按规则添加/更新
	for _, coin := range newResults {
		exist, exists := waitListL[coin.Symbol]
		if !exists {
			waitListL[coin.Symbol] = waitTokenL{
				Symbol:    coin.Symbol,
				Inst:      coin.Inst,
				Operation: coin.Operation,
				Status:    coin.Status,
				Source:    coin.Source,
				AddedAt:   now,
			}
			newAdded = true
		}
		if exists && exist.Operation != coin.Operation {
			waitListL[coin.Symbol] = waitTokenL{
				Symbol:    coin.Symbol,
				Inst:      coin.Inst,
				Operation: coin.Operation,
				Status:    coin.Status,
				Source:    coin.Source,
				AddedAt:   now,
			}
			newAdded = true
		}
	}
	waitMuL.Unlock()

	if newAdded {
		sendWaitListBroadcastL(now, waiting_token, chatID)
	}
}
