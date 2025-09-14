package utils

import (
	"energe/telegram"
	"energe/types"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

var minMonitorOnceB sync.Once

type waitTokenLB struct {
	Symbol            string
	Inst              string
	Operation         string
	Status            string
	AddedAt           time.Time
	LastInvalidPushed bool // 新增字段：是否已经推送过失效消息
}

// New: minMonitorToken for 15-min monitoring
type minMonitorTokenB struct {
	Symbol    string
	Operation string
	AddedAt   time.Time
}

var waitMuLB sync.Mutex
var waitListLB = make(map[string]waitTokenLB)
var minMonitorMuB sync.Mutex
var minMonitorListB = make(map[string]minMonitorTokenB)

// sendWaitListBroadcast 用于主动推送等待区列表
func sendWaitListBroadcastLB(now time.Time, waiting_token, chatID string) {
	waitMuLB.Lock()
	defer waitMuLB.Unlock()

	if len(waitListLB) == 0 {
		telegram.SendMessageWaitingLB(waiting_token, chatID, "等待区为空")
		return
	}

	var msgBuilder strings.Builder

	var emoje string

	for _, token := range waitListLB {
		if token.Operation == "BUYLongB" {
			emoje = "🟢🟢"
		} else if token.Operation == "SELLLongB" {
			emoje = "🔴🔴"
		} else {
			emoje = "-"
		}

		msgBuilder.WriteString(fmt.Sprintf("%s %-36s\n", emoje, token.Symbol))
	}
	msg := msgBuilder.String()
	telegram.SendMessageWaitingLB(waiting_token, chatID, msg)
}

func executeWaitCheckLB(wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	// 使用 defer 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			progressLogger.Printf("[executeWaitCheckLB] Panic recovered \n")
			// 返回默认值，表示处理失败
			// 你也可以根据需求记录到日志文件或监控系统
		}
	}()
	time.Sleep(10 * time.Second) // 保持你原来的延迟

	var changed bool // 是否发生了删除

	waitMuLB.Lock()
	waitCopy := make(map[string]waitTokenLB)
	for k, v := range waitListLB {
		waitCopy[k] = v
	}
	waitMuLB.Unlock()

	for sym, token := range waitCopy {
		var MACDD3, MACDW1, MACDD1 string
		var mid string

		var closesD1, closesW1, closesD3 []float64
		var err error
		closesD3, err = GetClosesWithFallback(client, sym, "3d")
		if err != nil {
			progressLogger.Println("获取数据失败:", err)
		}
		price := closesD3[len(closesD3)-1]
		pricePre := closesD3[len(closesD3)-2]
		pricePre2 := closesD3[len(closesD3)-3]

		isGolden := IsGolden(closesD3, 6, 13, 5)
		isDead := IsDead(closesD3, 6, 13, 5)
		_, ema25D3now := CalculateEMA(closesD3, 25)
		MACDD3 = "RANGE"
		if price > ema25D3now && isGolden {
			MACDD3 = "BUYMACD"
		} else if price < ema25D3now && isDead {
			MACDD3 = "SELLMACD"
		}

		//趋势结束标志
		mid = "RANGE"
		if pricePre > ema25D3now || pricePre2 > ema25D3now {
			mid = "UP"
		} else if pricePre < ema25D3now || pricePre2 < ema25D3now {
			mid = "DOWN"
		}

		closesD1, err = GetClosesWithFallback(client, sym, "1d")
		if err != nil {
			progressLogger.Println("获取数据失败:", err)
		}
		ma60D1 := CalculateMA(closesD1, 60)
		_, EMA25D1 := CalculateEMA(closesD1, 25)
		MACDSmallUP := IsSmallTFUP(closesD1, 6, 13, 5)
		MACDSmallDOWN := IsSmallTFDOWN(closesD1, 6, 13, 5)
		MACDD1 = "RANGE"
		if price > EMA25D1 && price > ma60D1 && MACDSmallUP {
			MACDD1 = "BUYMACD"
		} else if price < EMA25D1 && price < ma60D1 && MACDSmallDOWN {
			MACDD1 = "SELLMACD"
		}

		closesW1, err = GetClosesWithFallback(client, sym, "1w")
		if err != nil {
			progressLogger.Println("获取数据失败:", err)
		}
		_, ema25W1Now := CalculateEMA(closesW1, 25)
		DIFW1UP := IsDIFUP(closesW1, 6, 13, 5)
		DIFW1DOWN := IsDIFDOWN(closesW1, 6, 13, 5)

		if price > ema25W1Now && DIFW1UP {
			MACDW1 = "BUYMACD"
		} else if price < ema25W1Now && DIFW1DOWN {
			MACDW1 = "SELLMACD"
		}

		switch token.Operation {
		case "BUYLong":
			if MACDW1 == "BUYMACD" && MACDD3 == "BUYMACD" && MACDD1 == "BUYMACD" {
				// Add to 4H monitoring pipeline
				minMonitorMuB.Lock()
				if _, exists := minMonitorListB[sym]; !exists {
					minMonitorListB[sym] = minMonitorTokenB{
						Symbol:    sym,
						Operation: token.Operation,
						AddedAt:   time.Now(),
					}
				}
				minMonitorMuB.Unlock()
			} else if mid != "UP" {
				waitMuLB.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitListLB[sym]
				if !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageLB(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitListLB[sym] = t
				}
				delete(waitListLB, sym) // 删除
				waitMuLB.Unlock()
				changed = true
			} else {
				waitMuLB.Lock()
				t := waitListLB[sym]
				if !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageLB(wait_sucess_token, chatID, msg)
				}
				t.LastInvalidPushed = true
				waitListLB[sym] = t
				waitMuL.Unlock()
			}
		case "SELLLong":
			if MACDW1 == "SELLMACD" && MACDD3 == "SELLMACD" && MACDD1 == "SELLMACD" {
				// Add to 4H monitoring pipeline
				minMonitorMuB.Lock()
				if _, exists := minMonitorListB[sym]; !exists {
					minMonitorListB[sym] = minMonitorTokenB{
						Symbol:    sym,
						Operation: token.Operation,
						AddedAt:   time.Now(),
					}
				}
				minMonitorMuB.Unlock()
			} else if mid != "DOWN" {
				waitMuL.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitListLB[sym]
				if !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageLB(wait_sucess_token, chatID, msg)
					t.LastInvalidPushed = true
					waitListLB[sym] = t
				}
				delete(waitListLB, sym) // 删除
				waitMuLB.Unlock()
				changed = true
			} else {
				waitMuLB.Lock()
				t := waitListLB[sym]
				if !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageLB(wait_sucess_token, chatID, msg)
				}
				t.LastInvalidPushed = true
				waitListLB[sym] = t
				waitMuLB.Unlock()
			}
			if now.Sub(token.AddedAt) > 1000*time.Hour {
				waitMuLB.Lock()
				delete(waitListLB, sym)
				waitMuLB.Unlock()
				changed = true
			}
		}
		if changed {
			sendWaitListBroadcastLB(now, waiting_token, chatID)
		}
	}
}

func executeMinMonitorCheckB(wait_sucess_token, chatID string, client *futures.Client, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[execute15MinMonitorCheck] Panic recovered: %v\n", r)
		}
	}()

	// small delay if needed (保持你原来的 10s 也可以)
	time.Sleep(10 * time.Second)

	// Copy list quickly under lock
	minMonitorMuB.Lock()
	monitorCopy := make(map[string]minMonitorTokenB, len(minMonitorListB))
	for k, v := range minMonitorListB {
		monitorCopy[k] = v
	}
	minMonitorMuB.Unlock()

	// collect changes
	toRemove := make([]string, 0)
	// messages to send (sym -> msg)
	msgsToSend := make([]struct{ sym, operation string }, 0)

	for sym, token := range monitorCopy {
		// --- 获取 4H 数据（无锁） ---
		closesH4, err := GetClosesWithFallback(client, sym, "4H")
		if err != nil || len(closesH4) == 0 {
			progressLogger.Printf("获取 %s (4H) 数据失败: %v\n", sym, err)
			continue
		}
		priceH4 := closesH4[len(closesH4)-1]
		ma60H4 := CalculateMA(closesH4, 60)
		XSTRONGUPH4 := XSTRONGUP(closesH4, 6, 13, 5)
		XSTRONGDOWNH4 := XSTRONGDOWN(closesH4, 6, 13, 5)
		DIFUPH4 := IsDIFUP(closesH4, 6, 13, 5)
		DIFDOWNH4 := IsDIFDOWN(closesH4, 6, 13, 5)

		validX := "XBUY"
		if token.Operation == "SELLLong" {
			validX = "XSELL"
		}

		validMACD := "BUYMACD"
		if token.Operation == "SELLLong" {
			validMACD = "SELLMACD"
		}

		MACDH4 := ""
		if priceH4 > ma60H4 && XSTRONGUPH4 && DIFUPH4 {
			MACDH4 = "XBUY"
		} else if priceH4 < ma60H4 && XSTRONGDOWNH4 && DIFDOWNH4 {
			MACDH4 = "XSELL"
		}

		// --- 获取 3d 数据（无锁） ---
		closesD3, err := GetClosesWithFallback(client, sym, "3d")
		if err != nil || len(closesD3) == 0 {
			progressLogger.Printf("获取 %s (1m) 数据失败: %v\n", sym, err)
			continue
		}
		price := closesD3[len(closesD3)-1]

		isGolden := IsGolden(closesD3, 6, 13, 5)
		isDead := IsDead(closesD3, 6, 13, 5)
		_, ema25D3now := CalculateEMA(closesD3, 25)
		MACDD3 := "RANGE"
		if price > ema25D3now && isGolden {
			MACDD3 = "BUYMACD"
		} else if price < ema25D3now && isDead {
			MACDD3 = "SELLMACD"
		}
		// 触发
		if MACDH4 == validX && MACDD3 == validMACD {
			msgsToSend = append(msgsToSend, struct{ sym, operation string }{sym, token.Operation})
			toRemove = append(toRemove, sym) //发送一次就删除了
		}

		if MACDD3 != validMACD {
			toRemove = append(toRemove, sym)
			progressLogger.Printf("Removed %s from 4H monitoring due to trend end\n", sym)
			continue
		}

		// timeout
		if now.Sub(token.AddedAt) > 1000*time.Hour {
			toRemove = append(toRemove, sym)
			progressLogger.Printf("Removed %s from 4H monitoring due to timeout\n", sym)
			continue
		}
	}

	// APPLY  removals under lock
	if len(toRemove) > 0 {
		minMonitorMuB.Lock()
		for _, sym := range toRemove {
			delete(minMonitorListB, sym)
		}
		minMonitorMuB.Unlock()
		progressLogger.Printf("15-min monitor list updated, %d coins remaining\n", len(minMonitorListB))
	}

	// SEND messages (outside lock)
	for _, m := range msgsToSend {
		if err := sendMinMonitorBroadcastB(m.sym, m.operation, wait_sucess_token, chatID); err != nil {
			progressLogger.Printf("发送 1分钟消息失败: %s %v\n", m.sym, err)
		}
	}
}

// New: sendMinuteMonitorBroadcast for 4H monitoring signals
func sendMinMonitorBroadcastB(sym string, operation, wait_sucess_token, chatID string) error {
	emoji := "🟢"
	action := "做多"
	if operation == "SELLLong" {
		emoji = "🔴"
		action = "做空"
	}
	msg := fmt.Sprintf("%s%s(4H) ：%s%s", emoji, action, emoji, sym)
	if err := telegram.SendMessageLB(wait_sucess_token, chatID, msg); err != nil {
		progressLogger.Printf("发送 1分钟监控 Telegram 消息失败 (%s): %v\n", sym, err)
		return err
	}
	waitMuLB.Lock()
	t := waitListLB[sym]
	t.LastInvalidPushed = false
	waitListLB[sym] = t
	waitMuLB.Unlock()
	return nil
}

// waitUntilNextHour 计算等待时间直到下一小时整点
func waitUntilNext8Hour() time.Duration {
	now := time.Now()
	next := now.Truncate(8 * time.Hour).Add(8 * time.Hour)
	return time.Until(next)
}

func WaitEnergeLB(
	resultsChanLongB chan []types.CoinIndicator,
	wait_sucess_token, chatID string,
	client *futures.Client,
	klinesCount int,
	waiting_token string,
) {
	go func() {
		// 🚀 先消费一次已有消息，保证 waitList 不为空
		drainResultsLB(resultsChanLongB, waiting_token, chatID)

		// 再执行首次检测
		now := time.Now()
		executeWaitCheckLB(wait_sucess_token, chatID, client, waiting_token, now)

		time.Sleep(waitUntilNext8Hour())
		ticker := time.NewTicker(8 * time.Hour)
		defer ticker.Stop()

		for now := range ticker.C {
			go executeWaitCheckLB(wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()

	start4HMinMonitorLoopB(wait_sucess_token, chatID, client)
	// 常规消费
	for newResults := range resultsChanLongB {
		addToWaitListLB(newResults, waiting_token, chatID)
	}
}

// ================== 辅助函数 ==================

// 启动时先 drain 一次通道（非阻塞，防止残留）
func drainResultsLB(resultsChan chan []types.CoinIndicator, waiting_token, chatID string) {
	for {
		select {
		case newResults := <-resultsChan:
			addToWaitListLB(newResults, waiting_token, chatID)
		default:
			return
		}
	}
}

// 公共的添加逻辑（含 BTC/ETH 优先规则）
func addToWaitListLB(newResults []types.CoinIndicator, waiting_token, chatID string) {
	var newAdded bool
	now := time.Now()
	waitMuLB.Lock()

	// 再按规则添加/更新
	for _, coin := range newResults {
		exist, exists := waitListLB[coin.Symbol]
		if !exists {
			waitListLB[coin.Symbol] = waitTokenLB{
				Symbol:            coin.Symbol,
				Inst:              coin.Inst,
				Operation:         coin.Operation,
				Status:            coin.Status,
				AddedAt:           now,
				LastInvalidPushed: true,
			}
			newAdded = true
		}
		if exists && exist.Operation != coin.Operation {
			waitListLB[coin.Symbol] = waitTokenLB{
				Symbol:            coin.Symbol,
				Inst:              coin.Inst,
				Operation:         coin.Operation,
				Status:            coin.Status,
				AddedAt:           now,
				LastInvalidPushed: true,
			}
			newAdded = true
		}
	}
	waitMuLB.Unlock()

	if newAdded {
		sendWaitListBroadcastLB(now, waiting_token, chatID)
	}
}

func start4HMinMonitorLoopB(wait_sucess_token, chatID string, client *futures.Client) {
	minMonitorOnceB.Do(func() {
		go func() {
			// 计算到下一个 4 小時整点的时间
			now := time.Now()
			next := now.Truncate(4 * time.Hour).Add(4 * time.Hour)
			time.Sleep(time.Until(next))

			ticker := time.NewTicker(4 * time.Hour)
			defer ticker.Stop()

			for t := range ticker.C {
				// 每 4H 整点执行一次检查
				go executeMinMonitorCheckB(wait_sucess_token, chatID, client, t)
			}
		}()
	})
}
