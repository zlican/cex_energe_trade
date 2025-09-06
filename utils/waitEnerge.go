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

type waitToken struct {
	Symbol              string
	Inst                string
	Operation           string
	Status              string
	AddedAt             time.Time
	LastPushedOperation string // 记录最后一次推送的操作
	LastInvalidPushed   bool   // 是否已经推送过失效消息
}

var waitMu sync.Mutex
var waitList = make(map[string]waitToken)

// sendWaitListBroadcast 用于主动推送等待区列表
func sendWaitListBroadcast(now time.Time, waiting_token, chatID string) {
	waitMu.Lock()
	defer waitMu.Unlock()

	if len(waitList) == 0 {
		// 错误注释：如果 Telegram 发送失败，依赖其内置指数退避重试机制
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
	// 错误注释：如果发送失败，Telegram 内置重试机制会尝试重新发送
	telegram.SendMessageWaiting(waiting_token, chatID, msg)
}

// handleOperation 处理单一操作（BEBUY/BESELL/OTBUY/OTSELL）的通用逻辑
// 返回值：bool 表示是否从 waitList 删除该代币
func handleOperation(sym string, token waitToken, MACDM5, MACDM15, MACDH1, wait_sucess_token, chatID string) bool {
	isBuy := token.Operation == "BUY"
	emoji := "🟢"
	validMACD15 := "BUYMACD"
	validXMid := "XBUYMID"
	if !isBuy {
		emoji = "🔴"
		validMACD15 = "SELLMACD"
		validXMid = "XSELLMID"
	}
	// 条件 1：信号有效，发送交易信号
	if MACDH1 == validMACD15 && ((MACDM15 == validMACD15 && MACDM5 == validMACD15) || MACDM15 == validXMid) {
		if token.LastPushedOperation != token.Operation {
			var action string
			if isBuy {
				action = "做多"
			} else {
				action = "做空"
			}
			msg := fmt.Sprintf("%s%s：%s%s", emoji, action, emoji, sym)
			// 错误注释：如果 Telegram 发送失败，依赖其内置指数退避重试机制，失败后跳过状态更新
			if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
				fmt.Printf("发送 Telegram 消息失败 (%s): %v\n", sym, err)
				return false
			}
			t := waitList[sym]
			t.LastPushedOperation = token.Operation
			t.LastInvalidPushed = false // 重置失效推送标志
			waitList[sym] = t
		}
		return false
	}

	// 条件 2：15分钟信号失效，从 waitList 删除
	if MACDM15 != validMACD15 && MACDM15 != validXMid {
		t := waitList[sym]
		if t.LastPushedOperation == token.Operation && !t.LastInvalidPushed {
			msg := fmt.Sprintf("⚠️信号失效：%s", sym)
			// 错误注释：如果 Telegram 发送失败，依赖其内置重试机制，失败后仍删除代币以避免重复处理
			if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
				fmt.Printf("发送 Telegram 失效消息失败 (%s): %v\n", sym, err)
			} else {
				t.LastInvalidPushed = true
				waitList[sym] = t
			}
		}
		delete(waitList, sym)
		return true
	}

	// 条件 3：其他情况，发送失效消息并清空推送状态
	t := waitList[sym]
	if t.LastPushedOperation == token.Operation && !t.LastInvalidPushed {
		msg := fmt.Sprintf("⚠️信号失效：%s", sym)
		// 错误注释：如果 Telegram 发送失败，依赖其内置重试机制，失败后仍更新状态以避免重复发送
		if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
			fmt.Printf("发送 Telegram 失效消息失败 (%s): %v\n", sym, err)
		}
		t.LastInvalidPushed = true
	}
	t.LastPushedOperation = "" // 清空，允许下次推送
	waitList[sym] = t
	return false
}

func executeWaitCheck(wait_sucess_token, chatID string, client *futures.Client, waiting_token string, now time.Time) {
	// 使用 defer 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 错误注释：捕获 panic，避免程序崩溃，但需记录详细日志以便调试
			fmt.Printf("[executeWaitCheck] Panic recovered: %v\n", r)
		}
	}()

	time.Sleep(10 * time.Second) // 保持原有延迟

	var changed bool // 是否发生了删除

	// 单次锁定，复制 waitList 以避免并发修改
	waitMu.Lock()
	waitCopy := make(map[string]waitToken)
	for k, v := range waitList {
		waitCopy[k] = v
	}
	waitMu.Unlock()

	// 单次锁定处理所有代币
	waitMu.Lock()
	defer waitMu.Unlock()

	for sym, token := range waitCopy {
		var MACDM5, MACDM15, MACDH1 string

		var closesM15, closesM5, closesH1 []float64
		var err error
		// 获取 15 分钟 K 线数据
		closesM15, err = GetClosesWithFallback(client, sym, "15m")
		if err != nil || len(closesM15) == 0 {
			// 错误注释：数据获取失败或返回空数组，跳过以避免 panic
			fmt.Printf("获取 %s (15m) 数据失败: %v\n", sym, err)
			continue
		}
		price := closesM15[len(closesM15)-1]
		DIFUPM15 := IsDIFUP(closesM15, 6, 13, 5)
		DIFDOWNM15 := IsDIFDOWN(closesM15, 6, 13, 5)
		ma60M15 := CalculateMA(closesM15, 60)
		ema25M15 := CalculateEMA(closesM15, 25)
		// 错误注释：检查 ema25M15 长度，避免空数组访问
		if len(ema25M15) == 0 {
			fmt.Printf("计算 %s (15m) EMA25 失败: 空数组\n", sym)
			continue
		}
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
		// 获取 5 分钟 K 线数据
		closesM5, err = GetClosesWithFallback(client, sym, "5m")
		if err != nil || len(closesM5) == 0 {
			// 错误注释：数据获取失败或返回空数组，跳过以避免 panic
			fmt.Printf("获取 %s (5m) 数据失败: %v\n", sym, err)
			continue
		}
		ma60M5 := CalculateMA(closesM5, 60)
		XSTRONGUPM5 := XSTRONGUP(closesM5, 6, 13, 5)
		XSTRONGDOWNM5 := XSTRONGDOWN(closesM5, 6, 13, 5)
		if price > ma60M5 && XSTRONGUPM5 {
			MACDM5 = "BUYMACD"
		} else if price < ma60M5 && XSTRONGDOWNM5 {
			MACDM5 = "SELLMACD"
		}
		// 获取 1 小时 K 线数据
		closesH1, err = GetClosesWithFallback(client, sym, "1h")
		if err != nil || len(closesH1) == 0 {
			// 错误注释：数据获取失败或返回空数组，跳过以避免 panic
			fmt.Printf("获取 %s (1h) 数据失败: %v\n", sym, err)
			continue
		}
		ema25H1 := CalculateEMA(closesH1, 25)
		if len(ema25H1) == 0 {
			// 错误注释：检查 ema25H1 长度，避免空数组访问
			fmt.Printf("计算 %s (1h) EMA25 失败: 空数组\n", sym)
			continue
		}
		ema25H1Now := ema25H1[len(ema25H1)-1]
		ma60H1 := CalculateMA(closesH1, 60)
		DIFUPH1 := IsDIFUP(closesH1, 6, 13, 5)
		DIFDOWNH1 := IsDIFDOWN(closesH1, 6, 13, 5)
		if price > ema25H1Now && price > ma60H1 && DIFUPH1 {
			MACDH1 = "BUYMACD"
		} else if price < ema25H1Now && price < ma60H1 && DIFDOWNH1 {
			MACDH1 = "SELLMACD"
		}

		// 处理操作逻辑
		if handleOperation(sym, token, MACDM5, MACDM15, MACDH1, wait_sucess_token, chatID) {
			changed = true
		}

		// 检查是否超时（8小时）
		if now.Sub(token.AddedAt) > 8*time.Hour {
			// 错误注释：超时删除代币，未通知用户，可能需添加 Telegram 通知
			delete(waitList, sym)
			changed = true
		}
	}

	if changed {
		// 错误注释：如果 Telegram 发送失败，依赖其内置重试机制
		sendWaitListBroadcast(now, waiting_token, chatID)
	}
}

func waitUntilNext5Min() time.Duration { // 每5分钟监控
	now := time.Now()
	next := now.Truncate(time.Minute).Add(time.Duration(5-now.Minute()%5) * time.Minute)
	if next.Before(now) || next.Equal(now) {
		next = next.Add(5 * time.Minute)
	}
	return time.Until(next)
}

func WaitEnerge(
	resultsChan chan []types.CoinIndicator,
	wait_sucess_token, chatID string,
	client *futures.Client,
	klinesCount int,
	waiting_token string,
) {
	go func() {
		// 先消费一次已有消息，保证 waitList 不为空
		drainResults(resultsChan, waiting_token, chatID)

		// 执行首次检测
		now := time.Now()
		executeWaitCheck(wait_sucess_token, chatID, client, waiting_token, now)

		// 等到下一个 5 分钟整点
		time.Sleep(waitUntilNext5Min())

		// 每 5 分钟触发
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for now := range ticker.C {
			go executeWaitCheck(wait_sucess_token, chatID, client, waiting_token, now)
		}
	}()

	// 常规消费
	for newResults := range resultsChan {
		addToWaitList(newResults, waiting_token, chatID)
	}
}

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
		// 错误注释：如果 Telegram 发送失败，依赖其内置重试机制
		sendWaitListBroadcast(now, waiting_token, chatID)
	}
}
