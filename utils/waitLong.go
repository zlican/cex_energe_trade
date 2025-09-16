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

var minMonitorOnce sync.Once
var firstCheckOnce sync.Once

type waitTokenL struct {
	Symbol            string
	Inst              string
	Operation         string
	Status            string
	AddedAt           time.Time
	LastInvalidPushed bool // 新增字段：是否已经推送过失效消息
}

// New: minMonitorToken for 15-min monitoring
type minMonitorToken struct {
	Symbol    string
	Operation string
	AddedAt   time.Time
}

var waitMuL sync.Mutex
var waitListL = make(map[string]waitTokenL)
var minMonitorMu sync.Mutex
var minMonitorList = make(map[string]minMonitorToken)

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
	telegram.SendMessageWaitingL(waiting_token, chatID, msg)
}

func executeWaitCheckL(wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			progressLogger.Printf("[executeWaitCheckL] Panic recovered \n")
			// 返回默认值，表示处理失败
			// 你也可以根据需求记录到日志文件或监控系统
		}
	}()
	time.Sleep(10 * time.Second) // 保持你原来的延迟

	var changed bool // 是否发生了删除

	waitMuL.Lock()
	waitCopy := make(map[string]waitTokenL)
	for k, v := range waitListL {
		waitCopy[k] = v
	}
	waitMuL.Unlock()

	for sym, token := range waitCopy {
		var MACDH4, MACDD1, MACDH1 string
		var mid string

		var closesH1, closesD1, closesH4 []float64
		var err error
		closesH4, err = GetClosesWithFallback(client, sym, "4h")
		if err != nil {
			progressLogger.Println("获取数据失败:", err)
		}
		price := closesH4[len(closesH4)-1]
		pricePre := closesH4[len(closesH4)-2]
		pricePre2 := closesH4[len(closesH4)-3]

		isGolden := IsGolden(closesH4, 6, 13, 5)
		isDead := IsDead(closesH4, 6, 13, 5)
		_, ema25H4now := CalculateEMA(closesH4, 25)

		DIFH4UP := IsDIFUP(closesH4, 6, 13, 5)
		DIFH4DOWN := IsDIFDOWN(closesH4, 6, 13, 5)
		MACDH4 = "RANGE"
		if price > ema25H4now && isGolden && DIFH4UP {
			MACDH4 = "BUYMACD"
		} else if price < ema25H4now && isDead && DIFH4DOWN {
			MACDH4 = "SELLMACD"
		}

		//趋势结束标志
		mid = "RANGE"
		if (pricePre > ema25H4now || pricePre2 > ema25H4now) && DIFH4UP {
			mid = "UP"
		} else if (pricePre < ema25H4now || pricePre2 < ema25H4now) && DIFH4DOWN {
			mid = "DOWN"
		}

		closesH1, err = GetClosesWithFallback(client, sym, "1h")
		if err != nil {
			progressLogger.Println("获取数据失败:", err)
		}
		ma60H1 := CalculateMA(closesH1, 60)
		_, EMA25H1 := CalculateEMA(closesH1, 25)
		MACDSmallUP := IsSmallTFUP(closesH1, 6, 13, 5)
		MACDSmallDOWN := IsSmallTFDOWN(closesH1, 6, 13, 5)
		MACDH1 = "RANGE"
		if price > EMA25H1 && price > ma60H1 && MACDSmallUP {
			MACDH1 = "BUYMACD"
		} else if price < EMA25H1 && price < ma60H1 && MACDSmallDOWN {
			MACDH1 = "SELLMACD"
		}

		closesD1, err = GetClosesWithFallback(client, sym, "1d")
		if err != nil {
			progressLogger.Println("获取数据失败:", err)
		}
		_, ema25D1Now := CalculateEMA(closesD1, 25)
		DIFD1UP := IsDIFUP(closesD1, 6, 13, 5)
		DIFD1DOWN := IsDIFDOWN(closesD1, 6, 13, 5)
		MA60D1 := CalculateMA(closesD1, 60)

		if price > ema25D1Now && price > MA60D1 && DIFD1UP {
			MACDD1 = "BUYMACD"
		} else if price < ema25D1Now && price < MA60D1 && DIFD1DOWN {
			MACDD1 = "SELLMACD"
		}

		switch token.Operation {
		case "BUYLong":
			if MACDD1 == "BUYMACD" && MACDH4 == "BUYMACD" && MACDH1 == "BUYMACD" {
				// Add to 15-min monitoring pipeline
				minMonitorMu.Lock()
				if _, exists := minMonitorList[sym]; !exists {
					minMonitorList[sym] = minMonitorToken{
						Symbol:    sym,
						Operation: token.Operation,
						AddedAt:   time.Now(),
					}
				}
				minMonitorMu.Unlock()
			} else if mid != "UP" {
				waitMuL.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitListL[sym]
				if !t.LastInvalidPushed {
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
				if !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
				}
				t.LastInvalidPushed = true
				waitListL[sym] = t
				waitMuL.Unlock()
			}
		case "SELLLong":
			if MACDD1 == "SELLMACD" && MACDH4 == "SELLMACD" && MACDH1 == "SELLMACD" {
				// Add to 15-min monitoring pipeline
				minMonitorMu.Lock()
				if _, exists := minMonitorList[sym]; !exists {
					minMonitorList[sym] = minMonitorToken{
						Symbol:    sym,
						Operation: token.Operation,
						AddedAt:   time.Now(),
					}
				}
				minMonitorMu.Unlock()
			} else if mid != "DOWN" {
				waitMuL.Lock()
				// 如果之前推送过买入信号，而且还没发过“失效”消息
				t := waitListL[sym]
				if !t.LastInvalidPushed {
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
				if !t.LastInvalidPushed {
					msg := fmt.Sprintf("⚠️信号失效：%s", sym)
					telegram.SendMessageL(wait_sucess_token, chatID, msg)
				}
				t.LastInvalidPushed = true
				waitListL[sym] = t
				waitMuL.Unlock()
			}
			if now.Sub(token.AddedAt) > 1000*time.Hour {
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

func executeMinMonitorCheck(wait_sucess_token, chatID string, client *futures.Client, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[execute15MinMonitorCheck] Panic recovered: %v\n", r)
		}
	}()

	// small delay if needed (保持你原来的 10s 也可以)
	time.Sleep(10 * time.Second)

	// Copy list quickly under lock
	minMonitorMu.Lock()
	monitorCopy := make(map[string]minMonitorToken, len(minMonitorList))
	for k, v := range minMonitorList {
		monitorCopy[k] = v
	}
	minMonitorMu.Unlock()

	// collect changes
	toRemove := make([]string, 0)
	// messages to send (sym -> msg)
	msgsToSend := make([]struct{ sym, operation string }, 0)

	for sym, token := range monitorCopy {
		// --- 获取 15m 数据（无锁） ---
		closesM15, err := GetClosesWithFallback(client, sym, "15m")
		if err != nil || len(closesM15) == 0 {
			progressLogger.Printf("获取 %s (15m) 数据失败: %v\n", sym, err)
			continue
		}
		price15 := closesM15[len(closesM15)-1]
		ma60M15 := CalculateMA(closesM15, 60)
		XSTRONGUPM15 := XSTRONGUP(closesM15, 6, 13, 5)
		XSTRONGDOWNM15 := XSTRONGDOWN(closesM15, 6, 13, 5)

		validX := "XBUY"
		if token.Operation == "SELLLong" {
			validX = "XSELL"
		}

		validMACD := "BUYMACD"
		if token.Operation == "SELLLong" {
			validMACD = "SELLMACD"
		}

		MACDM15 := ""
		if price15 > ma60M15 && XSTRONGUPM15 {
			MACDM15 = "XBUY"
		} else if price15 < ma60M15 && XSTRONGDOWNM15 {
			MACDM15 = "XSELL"
		}

		// --- 获取 15m 数据（无锁） ---
		closesH4, err := GetClosesWithFallback(client, sym, "4H")
		if err != nil || len(closesH4) == 0 {
			progressLogger.Printf("获取 %s (15m) 数据失败: %v\n", sym, err)
			continue
		}
		price := closesH4[len(closesH4)-1]

		isGolden := IsGolden(closesH4, 6, 13, 5)
		isDead := IsDead(closesH4, 6, 13, 5)
		_, ema25H4now := CalculateEMA(closesH4, 25)

		DIFH4UP := IsDIFUP(closesH4, 6, 13, 5)
		DIFH4DOWN := IsDIFDOWN(closesH4, 6, 13, 5)
		MACDH4 := "RANGE"
		if price > ema25H4now && isGolden && DIFH4UP {
			MACDH4 = "BUYMACD"
		} else if price < ema25H4now && isDead && DIFH4DOWN {
			MACDH4 = "SELLMACD"
		}
		if MACDM15 == validX && MACDH4 == validMACD {
			msgsToSend = append(msgsToSend, struct{ sym, operation string }{sym, token.Operation})
			toRemove = append(toRemove, sym) //发送一次就删除了
		}

		if MACDH4 != validMACD {
			toRemove = append(toRemove, sym)
			progressLogger.Printf("Removed %s from 15-min monitoring due to trend end\n", sym)
			continue
		}

		// timeout
		if now.Sub(token.AddedAt) > 120*time.Hour {
			toRemove = append(toRemove, sym)
			progressLogger.Printf("Removed %s from 15-min monitoring due to timeout\n", sym)
			continue
		}
	}

	// APPLY  removals under lock
	if len(toRemove) > 0 {
		minMonitorMu.Lock()
		for _, sym := range toRemove {
			delete(minMonitorList, sym)
		}
		minMonitorMu.Unlock()
		progressLogger.Printf("15-min monitor list updated, %d coins remaining\n", len(minMonitorList))
	}

	// SEND messages (outside lock)
	for _, m := range msgsToSend {
		if err := sendMinMonitorBroadcast(m.sym, m.operation, wait_sucess_token, chatID); err != nil {
			progressLogger.Printf("发送 1分钟消息失败: %s %v\n", m.sym, err)
		}
	}
}

// New: sendMinuteMonitorBroadcast for 15-min monitoring signals
func sendMinMonitorBroadcast(sym string, operation, wait_sucess_token, chatID string) error {
	emoji := "🟢"
	action := "做多"
	if operation == "SELLLong" {
		emoji = "🔴"
		action = "做空"
	}
	msg := fmt.Sprintf("%s%s(15m) ：%s%s", emoji, action, emoji, sym)
	if err := telegram.SendMessageL(wait_sucess_token, chatID, msg); err != nil {
		progressLogger.Printf("发送 1分钟监控 Telegram 消息失败 (%s): %v\n", sym, err)
		return err
	}
	waitMuL.Lock()
	t := waitListL[sym]
	t.LastInvalidPushed = false
	waitListL[sym] = t
	waitMuL.Unlock()
	return nil
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
		drainResultsL(resultsChanLong, waiting_token, chatID, wait_sucess_token, client)
		start15MinMonitorLoop(wait_sucess_token, chatID, client)
		time.Sleep(waitUntilNextHour())
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for now := range ticker.C {
			go executeWaitCheckL(wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()

	// 常规消费
	for newResults := range resultsChanLong {
		addToWaitListL(newResults, waiting_token, chatID, wait_sucess_token, client)
	}
}

// ================== 辅助函数 ==================

// 启动时先 drain 一次通道（非阻塞，防止残留）
func drainResultsL(resultsChan chan []types.CoinIndicator, waiting_token, chatID, wait_sucess_token string, client *futures.Client) {
	for {
		select {
		case newResults := <-resultsChan:
			addToWaitListL(newResults, waiting_token, chatID, wait_sucess_token, client)
		default:
			return
		}
	}
}

// 公共的添加逻辑（含 BTC/ETH 优先规则）
func addToWaitListL(newResults []types.CoinIndicator, waiting_token, chatID, wait_sucess_token string, client *futures.Client) {
	var newAdded bool
	now := time.Now()
	waitMuL.Lock()

	// 再按规则添加/更新
	for _, coin := range newResults {
		exist, exists := waitListL[coin.Symbol]
		if !exists {
			waitListL[coin.Symbol] = waitTokenL{
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
			waitListL[coin.Symbol] = waitTokenL{
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
	waitMuL.Unlock()

	if newAdded {
		sendWaitListBroadcastL(now, waiting_token, chatID)

		//首次执行检测
		firstCheckOnce.Do(func() {
			executeWaitCheckL(wait_sucess_token, chatID, client, waiting_token, time.Now())
		})
	}
}

func start15MinMonitorLoop(wait_sucess_token, chatID string, client *futures.Client) {
	minMonitorOnce.Do(func() {
		go func() {
			//立刻执行一次
			executeMinMonitorCheck(wait_sucess_token, chatID, client, time.Now())
			// 计算到下一个 15 分钟整点的时间
			now := time.Now()
			next := now.Truncate(15 * time.Minute).Add(15 * time.Minute)
			time.Sleep(time.Until(next))

			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()

			for t := range ticker.C {
				// 每 15 分钟整点执行一次检查
				go executeMinMonitorCheck(wait_sucess_token, chatID, client, t)
			}
		}()
	})
}
