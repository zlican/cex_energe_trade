package utils

import (
	"energe/telegram"
	"energe/types"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

var minuteMonitorOnce sync.Once

// waitToken remains unchanged
type waitToken struct {
	Symbol            string
	Inst              string
	Operation         string
	Status            string
	AddedAt           time.Time
	LastInvalidPushed bool
}

// New: minuteMonitorToken for 1-minute monitoring
type minuteMonitorToken struct {
	Symbol    string
	Operation string
	AddedAt   time.Time
}

// Global variables
var waitMu sync.Mutex
var waitList = make(map[string]waitToken)
var minuteMonitorMu sync.Mutex
var minuteMonitorList = make(map[string]minuteMonitorToken)
var progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)

// sendWaitListBroadcast (unchanged)
func sendWaitListBroadcast(now time.Time, waiting_token, chatID string) {
	if len(waitList) == 0 {
		telegram.SendMessageWaiting(waiting_token, chatID, "等待区为空")
		return
	}

	var msgBuilder strings.Builder
	var emoji string

	for _, token := range waitList {
		if token.Operation == "BUY" {
			emoji = "🟢🟢"
		} else if token.Operation == "SELL" {
			emoji = "🔴🔴"
		} else {
			emoji = "-"
		}
		msgBuilder.WriteString(fmt.Sprintf("%s %-36s\n", emoji, token.Symbol))
	}
	msg := msgBuilder.String()
	telegram.SendMessageWaiting(waiting_token, chatID, msg)
}

// New: sendMinuteMonitorBroadcast for 1-minute monitoring signals
func sendMinuteMonitorBroadcast(sym string, operation, wait_sucess_token, chatID string) error {
	emoji := "🟢"
	action := "做多"
	if operation == "SELL" {
		emoji = "🔴"
		action = "做空"
	}
	msg := fmt.Sprintf("%s%s ：%s%s", emoji, action, emoji, sym)
	if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
		progressLogger.Printf("发送 1分钟监控 Telegram 消息失败 (%s): %v\n", sym, err)
		return err
	}
	t := waitList[sym]
	t.LastInvalidPushed = false
	waitList[sym] = t
	return nil
}

// Modified: handleOperation to integrate 1-minute monitoring
func handleOperation(sym string, token waitToken, mid string, MACDM5, MACDM15, MACDH1, wait_sucess_token, chatID string) bool {
	isBuy := token.Operation == "BUY"
	validMACD := "BUYMACD"
	validMid := "UP"
	if !isBuy {
		validMACD = "SELLMACD"
		validMid = "DOWN"
	}

	if MACDH1 == validMACD && MACDM15 == validMACD && MACDM5 == validMACD {
		// Add to 1-minute monitoring pipeline
		minuteMonitorMu.Lock()
		if _, exists := minuteMonitorList[sym]; !exists {
			minuteMonitorList[sym] = minuteMonitorToken{
				Symbol:    sym,
				Operation: token.Operation,
				AddedAt:   time.Now(),
			}
		}
		minuteMonitorMu.Unlock()
		return false
	}

	// Condition 2: 15-minute signal invalid, remove from waitList and minuteMonitorList
	if mid != validMid {
		t := waitList[sym]
		if !t.LastInvalidPushed {
			msg := fmt.Sprintf("⚠️信号失效：%s", sym)
			if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
				progressLogger.Printf("发送 Telegram 失效消息失败 (%s): %v\n", sym, err)
			} else {
				t.LastInvalidPushed = true
				waitList[sym] = t
			}
		}
		// Remove from minute monitoring if present
		minuteMonitorMu.Lock()
		delete(minuteMonitorList, sym)
		minuteMonitorMu.Unlock()
		delete(waitList, sym)
		return true
	}

	// Condition 3: Other cases, send invalid signal and clear push state
	t := waitList[sym]
	if !t.LastInvalidPushed {
		msg := fmt.Sprintf("⚠️信号失效：%s", sym)
		if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
			progressLogger.Printf("发送 Telegram 失效消息失败 (%s): %v\n", sym, err)
		}
		t.LastInvalidPushed = true
	}
	waitList[sym] = t
	return false
}

// New: executeMinuteMonitorCheck for 1-minute monitoring
func executeMinuteMonitorCheck(wait_sucess_token, chatID string, client *futures.Client, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[executeMinuteMonitorCheck] Panic recovered: %v\n", r)
		}
	}()

	// small delay if needed (保持你原来的 10s 也可以)
	time.Sleep(10 * time.Second)

	// Copy list quickly under lock
	minuteMonitorMu.Lock()
	monitorCopy := make(map[string]minuteMonitorToken, len(minuteMonitorList))
	for k, v := range minuteMonitorList {
		monitorCopy[k] = v
	}
	minuteMonitorMu.Unlock()

	// collect changes
	toRemove := make([]string, 0)
	// messages to send (sym -> msg)
	msgsToSend := make([]struct{ sym, operation string }, 0)

	for sym, token := range monitorCopy {
		// --- 获取 1m 数据（无锁） ---
		closesM1, err := GetClosesWithFallback(client, sym, "1m")
		if err != nil || len(closesM1) == 0 {
			progressLogger.Printf("获取 %s (1m) 数据失败: %v\n", sym, err)
			continue
		}
		price1 := closesM1[len(closesM1)-1]
		ma60M1 := CalculateMA(closesM1, 60)
		XSTRONGUPM1 := XSTRONGUP(closesM1, 6, 13, 5)
		XSTRONGDOWNM1 := XSTRONGDOWN(closesM1, 6, 13, 5)
		DIFUPM1 := IsDIFUP(closesM1, 6, 13, 5)
		DIFDOWNM1 := IsDIFDOWN(closesM1, 6, 13, 5)

		validX := "XBUY"
		if token.Operation == "SELL" {
			validX = "XSELL"
		}

		validMACD := "BUYMACD"
		if token.Operation == "SELL" {
			validMACD = "SELLMACD"
		}

		MACDM1 := ""
		if price1 > ma60M1 && XSTRONGUPM1 && DIFUPM1 {
			MACDM1 = "XBUY"
		} else if price1 < ma60M1 && XSTRONGDOWNM1 && DIFDOWNM1 {
			MACDM1 = "XSELL"
		}

		// --- 获取 15m 数据（无锁） ---
		closesM15, err := GetClosesWithFallback(client, sym, "15m")
		if err != nil || len(closesM15) == 0 {
			progressLogger.Printf("获取 %s (1m) 数据失败: %v\n", sym, err)
			continue
		}
		price := closesM15[len(closesM15)-1]
		isGolden := IsGolden(closesM15, 6, 13, 5)
		isDead := IsDead(closesM15, 6, 13, 5)
		_, ema25M15now := CalculateEMA(closesM15, 25)
		MACDM15 := "RANGE"
		if price > ema25M15now && isGolden {
			MACDM15 = "BUYMACD"
		} else if price < ema25M15now && isDead {
			MACDM15 = "SELLMACD"
		}

		// 触发
		if MACDM1 == validX && MACDM15 == validMACD {
			msgsToSend = append(msgsToSend, struct{ sym, operation string }{sym, token.Operation})
			toRemove = append(toRemove, sym) //发送一次就删除了
		}

		if MACDM15 != validMACD {
			toRemove = append(toRemove, sym)
			progressLogger.Printf("Removed %s from 1-minute monitoring due to trend end\n", sym)
			continue
		}

		// timeout
		if now.Sub(token.AddedAt) > 1*time.Hour {
			toRemove = append(toRemove, sym)
			progressLogger.Printf("Removed %s from 1-minute monitoring due to timeout\n", sym)
			continue
		}
	}

	// APPLY  removals under lock
	if len(toRemove) > 0 {
		minuteMonitorMu.Lock()
		for _, sym := range toRemove {
			delete(minuteMonitorList, sym)
		}
		minuteMonitorMu.Unlock()
		progressLogger.Printf("1-minute monitor list updated, %d coins remaining\n", len(minuteMonitorList))
	}

	// SEND messages (outside lock)
	for _, m := range msgsToSend {
		if err := sendMinuteMonitorBroadcast(m.sym, m.operation, wait_sucess_token, chatID); err != nil {
			progressLogger.Printf("发送 1分钟消息失败: %s %v\n", m.sym, err)
		}
	}
}

// Modified: executeWaitCheck to start 1-minute monitoring loop
func executeWaitCheck(wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	// Existing logic (unchanged except for mutex scope)
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[executeWaitCheck] Panic recovered: %v\n", r)
		}
	}()

	time.Sleep(10 * time.Second)

	var changed bool
	waitMu.Lock()
	waitCopy := make(map[string]waitToken)
	for k, v := range waitList {
		waitCopy[k] = v
	}
	waitMu.Unlock()

	waitMu.Lock()
	defer waitMu.Unlock()

	for sym, token := range waitCopy {
		var MACDM5, MACDM15, MACDH1 string
		var mid string

		closesM15, err := GetClosesWithFallback(client, sym, "15m")
		if err != nil {
			progressLogger.Printf("获取 %s (15m) 数据失败: %v\n", sym, err)
			continue
		}
		if len(closesM15) < 3 {
			progressLogger.Printf("%s (15m) 数据不足: %d\n", sym, len(closesM15))
			continue
		}
		price := closesM15[len(closesM15)-1]
		pricePre := closesM15[len(closesM15)-2]
		pricePre2 := closesM15[len(closesM15)-3]
		isGolden := IsGolden(closesM15, 6, 13, 5)
		isDead := IsDead(closesM15, 6, 13, 5)
		ema25M15, ema25M15now := CalculateEMA(closesM15, 25)
		if len(ema25M15) == 0 {
			progressLogger.Printf("计算 %s (15m) EMA25 失败: 空数组\n", sym)
			continue
		}
		MACDM15 = "RANGE"
		if price > ema25M15now && isGolden {
			MACDM15 = "BUYMACD"
		} else if price < ema25M15now && isDead {
			MACDM15 = "SELLMACD"
		}
		mid = "RANGE"
		if pricePre > ema25M15now || pricePre2 > ema25M15now {
			mid = "UP"
		} else if pricePre < ema25M15now || pricePre2 < ema25M15now {
			mid = "DOWN"
		}

		closesM5, err := GetClosesWithFallback(client, sym, "5m")
		if err != nil {
			progressLogger.Printf("获取 %s (5m) 数据失败: %v\n", sym, err)
			continue
		}
		if len(closesM5) < 3 {
			progressLogger.Printf("%s (5m) 数据不足: %d\n", sym, len(closesM5))
			continue
		}
		ma60M5 := CalculateMA(closesM5, 60)
		_, ema25M5now := CalculateEMA(closesM5, 25)
		MACDSmallUP := IsSmallTFUP(closesM5, 6, 13, 5)
		MACDsmallDOWN := IsSmallTFDOWN(closesM5, 6, 13, 5)
		if price > ema25M5now && price > ma60M5 && MACDSmallUP {
			MACDM5 = "BUYMACD"
		} else if price < ema25M5now && price < ma60M5 && MACDsmallDOWN {
			MACDM5 = "SELLMACD"
		}

		closesH1, err := GetClosesWithFallback(client, sym, "1h")
		if err != nil {
			progressLogger.Printf("获取 %s (1h) 数据失败: %v\n", sym, err)
			continue
		}
		if len(closesH1) < 3 {
			progressLogger.Printf("%s (1h) 数据不足: %d\n", sym, len(closesH1))
			continue
		}
		ema25H1, ema25H1Now := CalculateEMA(closesH1, 25)
		if len(ema25H1) == 0 {
			progressLogger.Printf("计算 %s (1h) EMA25 失败: 空数组\n", sym)
			continue
		}
		DIFH1UP := IsDIFUP(closesH1, 6, 13, 5)
		DIFH1DOWN := IsDIFDOWN(closesH1, 6, 13, 5)
		MA60H1 := CalculateMA(closesH1, 60)
		if price > ema25H1Now && price > MA60H1 && DIFH1UP {
			MACDH1 = "BUYMACD"
		} else if price < ema25H1Now && price < MA60H1 && DIFH1DOWN {
			MACDH1 = "SELLMACD"
		}

		if handleOperation(sym, token, mid, MACDM5, MACDM15, MACDH1, wait_sucess_token, chatID) {
			changed = true
		}

		if now.Sub(token.AddedAt) > 64*time.Hour {
			delete(waitList, sym)
			minuteMonitorMu.Lock()
			delete(minuteMonitorList, sym)
			minuteMonitorMu.Unlock()
			changed = true
		}
	}

	if changed {
		sendWaitListBroadcast(now, waiting_token, chatID)
	}
}

// waitUntilNext5Min (unchanged)
func waitUntilNext5Min() time.Duration {
	now := time.Now()
	next := now.Truncate(time.Minute).Add(time.Duration(5-now.Minute()%5) * time.Minute)
	if next.Before(now) || next.Equal(now) {
		next = next.Add(5 * time.Minute)
	}
	return time.Until(next)
}

// WaitEnerge (unchanged)
func WaitEnerge(
	resultsChan chan []types.CoinIndicator,
	wait_sucess_token, chatID string,
	client *futures.Client,
	klinesCount int,
	waiting_token string,
	chBanToWaitList chan []string,
) {
	go func() {
		drainResults(resultsChan, waiting_token, chatID)
		now := time.Now()
		executeWaitCheck(wait_sucess_token, chatID, client, waiting_token, now)
		time.Sleep(waitUntilNext5Min())
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for now := range ticker.C {
			go executeWaitCheck(wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()
	go startMinuteMonitorLoop(wait_sucess_token, chatID, client)
	go removeBanFromWaitList(chBanToWaitList, chatID, wait_sucess_token, waiting_token)
	for newResults := range resultsChan {
		addToWaitList(newResults, waiting_token, chatID)
	}
}

func removeBanFromWaitList(chBan chan []string, chatID string, wait_sucess_token, waiting_token string) {
	for banList := range chBan {
		changed := false
		for _, ban := range banList {
			_, exists := waitList[ban]
			if exists {
				msg := fmt.Sprintf("⚠️信号失效：%s", ban)
				if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
					progressLogger.Printf("发送 Telegram 失效消息失败 (%s): %v\n", ban, err)
				}
				waitMu.Lock()
				delete(waitList, ban)
				waitMu.Unlock()
				minuteMonitorMu.Lock()
				delete(minuteMonitorList, ban)
				minuteMonitorMu.Unlock()

				changed = true
			}
		}
		if changed {
			now := time.Now()
			sendWaitListBroadcast(now, waiting_token, chatID)
		}
	}
}

// drainResults (unchanged)
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

// addToWaitList (unchanged)
func addToWaitList(newResults []types.CoinIndicator, waiting_token, chatID string) {
	var newAdded bool
	now := time.Now()

	waitMu.Lock()
	for _, coin := range newResults {
		exist, exists := waitList[coin.Symbol]
		if !exists {
			waitList[coin.Symbol] = waitToken{
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
			waitList[coin.Symbol] = waitToken{
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

	if newAdded {
		sendWaitListBroadcast(now, waiting_token, chatID)
	}
	waitMu.Unlock()
}

func startMinuteMonitorLoop(wait_sucess_token, chatID string, client *futures.Client) {
	minuteMonitorOnce.Do(func() {
		go func() {
			// 对齐到下一个整分钟
			time.Sleep(time.Until(time.Now().Truncate(time.Minute).Add(time.Minute)))
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for now := range ticker.C {
				// 每分钟并发执行一次检查（执行过程中不会持锁）
				go executeMinuteMonitorCheck(wait_sucess_token, chatID, client, now)
			}
		}()
	})
}
