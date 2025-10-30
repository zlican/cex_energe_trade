package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"energe/telegram"
	"energe/types"
	"energe/utils"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
	"golang.org/x/sync/semaphore"
)

/* ====================== 结构体 & 全局 ====================== */

var (
	apiKey               = ""
	secretKey            = ""
	proxyURL             = "http://127.0.0.1:10809"
	wait_energe_botToken = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //CEX短线
	long_energe_bot      = "8429540001:AAH-bqd5aRxAVr37aGOKTzKlTmURdiJvYyg" //CEX中线
	chatID               = "6074996357"

	smallVol       = 100000000 //1亿
	slipCoinNo     = []string{}
	progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)
	topGainers     = []string{}          //涨幅榜
	newSymbols     = []string{}          //新币合约
	banSymbols     = []string{}          //封禁区
	ticker24h      = []utils.Ticker24h{} //24H的数据
)

var runScanRunning int32
var runScanMIDRunning int32
var BE = []string{
	"BTCUSDT", "ETHUSDT",
}

/* ====================== 主函数 ====================== */

func main() {
	progressLogger.Println("程序启动...")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/latest-tg-messages", latestMessagesHandler)
	mux.HandleFunc("/api/latest-tg-messages-long", latestMessagesLongHandler)

	go func() {
		if err := http.ListenAndServe(":8888", corsMiddleware(mux)); err != nil {
			log.Fatalf("HTTP服务器启动失败: %v", err)
		}
	}()

	client := binance.NewFuturesClient(apiKey, secretKey)
	setHTTPClient(client)

	//启动涨幅榜获取（8个1亿交易量以上）
	chTopGainers := make(chan []string)
	chTicker24h := make(chan []utils.Ticker24h)
	topGainers, ticker24h = utils.GetDailyGainers(8)
	go utils.StartTopGainersUTCFetcher(chTopGainers, chTicker24h)
	go func() {
		for Symbols := range chTopGainers {
			topGainers = Symbols
		}
	}()
	//启动24小数交易数据获取
	go func() {
		for Symbols := range chTicker24h {
			ticker24h = Symbols
		}
	}()

	//启动新币合约获取
	chNewPereCoins := make(chan []string)
	newSymbols = utils.GetNewPerpCoins()
	go utils.StartNewPereFetcher(chNewPereCoins)
	go func() {
		for Symbols := range chNewPereCoins {
			newSymbols = Symbols
		}
	}()

	//获取封禁区
	chBanList := make(chan []string, 10)
	banSymbols = utils.GetBanList()
	go utils.StartBanListFetcher(chBanList)
	go func() {
		for Symbols := range chBanList {
			banSymbols = Symbols
		}
	}()

	//短线监控模型
	go func() {
		progressLogger.Printf("[runScan] 首次立即执行: %s", time.Now().Format("15:04:05"))
		if err := runScanOnce(client, 20, wait_energe_botToken, chatID); err != nil {
			progressLogger.Printf("首次 runScan 出错: %v", err)
		}
		// 计算下一次对齐时间
		now := time.Now()
		nextAligned := now.Truncate(5 * time.Minute).Add(5 * time.Minute)
		delay := time.Until(nextAligned)
		time.Sleep(delay)

		// 进入每分钟循环（主循环在该 goroutine 内执行）
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for t := range ticker.C {
			progressLogger.Printf("[runScan] 每5分钟触发: %s", t.Format("15:04:05"))

			// 如果上一次还在跑，则跳过本次（非阻塞）
			if !atomic.CompareAndSwapInt32(&runScanRunning, 0, 1) {
				progressLogger.Println("上一次 runScan 未结束，跳过本次执行")
				continue
			}

			// 异步执行 runScanOnce，结束时清理标记
			go func(execTime time.Time) {
				defer atomic.StoreInt32(&runScanRunning, 0)
				if err := runScanOnce(client, 20, wait_energe_botToken, chatID); err != nil {
					progressLogger.Printf("周期 runScan 出错 (%s): %v", execTime.Format("15:04:05"), err)
				}
			}(t)
		}
	}()

	//中线监控模型
	go func() {
		progressLogger.Printf("[runScanMID] 首次立即执行: %s", time.Now().Format("15:04:05"))
		if err := runScanMIDOnce(client, 20, long_energe_bot, chatID); err != nil {
			progressLogger.Printf("首次 runScanMID 出错: %v", err)
		}
		// 计算下一次对齐时间
		now := time.Now()
		nextAligned := now.Truncate(60 * time.Minute).Add(60 * time.Minute)
		delay := time.Until(nextAligned)
		time.Sleep(delay)

		// 进入每分钟循环（主循环在该 goroutine 内执行）
		ticker := time.NewTicker(60 * time.Minute)
		defer ticker.Stop()

		for t := range ticker.C {
			progressLogger.Printf("[runScanMID] 每小时触发: %s", t.Format("15:04:05"))

			// 如果上一次还在跑，则跳过本次（非阻塞）
			if !atomic.CompareAndSwapInt32(&runScanMIDRunning, 0, 1) {
				progressLogger.Println("上一次 runScanMID 未结束，跳过本次执行")
				continue
			}

			// 异步执行 runScanMIDOnce，结束时清理标记
			go func(execTime time.Time) {
				defer atomic.StoreInt32(&runScanMIDRunning, 0)
				if err := runScanMIDOnce(client, 20, long_energe_bot, chatID); err != nil {
					progressLogger.Printf("周期 runScanMID 出错 (%s): %v", execTime.Format("15:04:05"), err)
				}
			}(t)
		}
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("收到退出信号，程序结束")
}

// runScanOnce：一次完整扫描（并发分析所有候选，并即时通知满足四周期条件的币）
func runScanOnce(client *futures.Client, maxWorkers int64, wait_sucess_token, chatID string) error {

	//7秒获K
	time.Sleep(7 * time.Second)
	if len(newSymbols) == 0 {
		progressLogger.Println("新币合约启动失败")
	}
	if len(topGainers) == 0 {
		progressLogger.Println("涨幅榜启动失败")
	}
	// 1) 获取候选（和你原来代码保持一致）
	CGTopGainers, err := utils.GetCGTopGainers()
	if err != nil {
		progressLogger.Printf("get CG topgainers err: %v\n", err)
	}
	candidates, _ := utils.GetHotCoins(ticker24h, slipCoinNo, banSymbols,
		utils.VolumeCMCCSlip(ticker24h, newSymbols),
		utils.VolumeCMCCSlip(ticker24h, topGainers),
		utils.VolumeCMCCSlip(ticker24h, CGTopGainers),
	)

	// 并发准备
	var (
		resMu sync.Mutex
		wg    sync.WaitGroup
		sem   = semaphore.NewWeighted(maxWorkers)
	)

	signals := make([]types.CoinIndicator, 0, 8)
	found := make(map[string]struct{}) // 本次 run 内去重

	for _, c := range candidates {
		// Acquire with context so it won't hang forever
		if err := sem.Acquire(context.Background(), 1); err != nil {
			progressLogger.Printf("semaphore acquire 失败: %v", err)
			continue
		}
		wg.Add(1)

		go func(c types.Candidate) {
			defer wg.Done()
			defer sem.Release(1)

			// 先做 volume 快速过滤（复用原逻辑）
			if !utils.CheckVolume(ticker24h, c.Symbol, float64(smallVol)) {
				return
			}

			// analyseSymbolForSignal 会拉 4h/1h/15m/5m/1m 并判断五框架条件
			ind, ok := analyseSymbolForSignal(client, c)
			if !ok {
				return
			}

			// 本次 run 内去重（防止同一符号被多次加入）
			resMu.Lock()
			if _, exists := found[ind.Symbol]; !exists {
				signals = append(signals, ind)
				found[ind.Symbol] = struct{}{}
			}
			resMu.Unlock()
		}(c)
	}

	wg.Wait()

	// 发送通知
	for _, s := range signals {
		var opIcon, opText, symIcon string

		if s.Operation == "BUY" {
			opIcon = "🟢"
			opText = "做多"
			symIcon = "🟢" + s.Symbol
		} else if s.Operation == "SELL" {
			opIcon = "🔴"
			opText = "做空"
			symIcon = "🔴" + s.Symbol
		} else {
			// 兜底，避免空值
			opIcon = "⚪"
			opText = s.Operation
			symIcon = s.Symbol
		}

		msg := fmt.Sprintf("%s%s ：%s", opIcon, opText, symIcon)

		if err := telegram.SendMessage(wait_sucess_token, chatID, msg); err != nil {
			progressLogger.Printf("SendMessage 失败: %v\n", err)
		} else {
			progressLogger.Printf("已推送信号: %s %s\n", s.Symbol, s.Operation)
		}
	}

	return nil
}

// analyseSymbolForSignal：一次性检查 4h, 1h,15m,5m；只有四个判定全部匹配时返回 true
func analyseSymbolForSignal(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	// 防止 panic
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[analyseSymbolForSignal] panic recovered %s : %v\n", c.Symbol, r)
		}
	}()

	sym := c.Symbol

	// --- STEP 0: 4h （共振筛） ---
	closesH4, err := utils.GetClosesWithFallback(client, sym, "4h")
	if err != nil || len(closesH4) < 1 {
		progressLogger.Printf("%s 4h 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceH4 := closesH4[len(closesH4)-1]
	MA60H4 := utils.CalculateMA(closesH4, 60)
	DIFUPH4 := utils.IsDIFUP(closesH4, 6, 13, 5)
	DIFDOWNH4 := utils.IsDIFDOWN(closesH4, 6, 13, 5)

	MACDH4 := "RANGE"                               //环境
	if (inBE(sym) || priceH4 > MA60H4) && DIFUPH4 { //MA60	 +	 DIF水上
		MACDH4 = "BUYMACD"
	} else if (inBE(sym) || priceH4 < MA60H4) && DIFDOWNH4 {
		if !inBE(sym) { //只能空BE
			return types.CoinIndicator{}, false
		}
		if sym != "" {
			MACDH4 = "SELLMACD"
		}
	} else {
		return types.CoinIndicator{}, false
	}

	// 基于 4h 决定本次要查 BUY 还是 SELL
	isBuy := (MACDH4 == "BUYMACD")
	validMACD := "BUYMACD"
	if !isBuy {
		validMACD = "SELLMACD"
	}

	// --- STEP A: 1h（做第一道筛） ---
	closesH1, err := utils.GetClosesWithFallback(client, sym, "1h")
	if err != nil || len(closesH1) < 1 {
		progressLogger.Printf("%s 1h 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceH1 := closesH1[len(closesH1)-1]
	ColANDDIFUPH1 := utils.ColANDDIFUP(closesH1, 6, 13, 5)
	ColANDDIFDOWNH1 := utils.ColANDDIFDOWN(closesH1, 6, 13, 5)
	DIFH1UP := utils.IsDIFUP(closesH1, 6, 13, 5)
	DIFH1DOWN := utils.IsDIFDOWN(closesH1, 6, 13, 5)
	MA60H1 := utils.CalculateMA(closesH1, 60)

	MACDH1 := ""
	if DIFH1UP && priceH1 > MA60H1 && ColANDDIFUPH1 { //MA60	+	DIF水上	+	当下柱线同向
		MACDH1 = "BUYMACD"
	} else if DIFH1DOWN && priceH1 < MA60H1 && ColANDDIFDOWNH1 {
		MACDH1 = "SELLMACD"
	}
	if MACDH1 != validMACD {
		// 1h 趋势与 4h 不一致 → 早退
		return types.CoinIndicator{}, false
	}

	// --- STEP B: 15m （做第二道筛） ---
	closesM15, err := utils.GetClosesWithFallback(client, sym, "15m")
	if err != nil || len(closesM15) < 1 {
		progressLogger.Printf("%s 15m 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceM15 := closesM15[len(closesM15)-1]
	MA60M15 := utils.CalculateMA(closesM15, 60)
	ColANDDIFupM15 := utils.ColANDDIFUP(closesM15, 6, 13, 5)
	ColANDDIFdownM15 := utils.ColANDDIFDOWN(closesM15, 6, 13, 5)
	DIFM15UP := utils.IsDIFUP(closesM15, 6, 13, 5)
	DIFM15DOWN := utils.IsDIFDOWN(closesM15, 6, 13, 5)

	MACDM15 := "RANGE"
	if priceM15 > MA60M15 && DIFM15UP && ColANDDIFupM15 { //MA60	+	DIF水上	+	当下柱线同向
		MACDM15 = "BUYMACD"
	} else if priceM15 < MA60M15 && DIFM15DOWN && ColANDDIFdownM15 {
		MACDM15 = "SELLMACD"
	}

	if MACDM15 != validMACD {
		// 15m 趋势与 4h 不一致 → 早退
		return types.CoinIndicator{}, false
	}

	// --- STEP C: 5m （更细致） ---
	closesM5, err := utils.GetClosesWithFallback(client, sym, "5m")
	if err != nil || len(closesM5) < 1 {
		progressLogger.Printf("%s 5m 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceM5 := closesM5[len(closesM5)-1]
	ma60M5 := utils.CalculateMA(closesM5, 60)
	ColANDDIFupM5 := utils.ColANDDIFUP(closesM5, 6, 13, 5)
	ColANDDIFdownM5 := utils.ColANDDIFDOWN(closesM5, 6, 13, 5)
	DIFUPM5 := utils.IsDIFUP(closesM5, 6, 13, 5)
	DIFDOWNM5 := utils.IsDIFDOWN(closesM5, 6, 13, 5)

	MACDM5 := "RANGE"
	if priceM5 > ma60M5 && DIFUPM5 && ColANDDIFupM5 { //MA60	+	DIF水上		+ 当下线柱同向
		MACDM5 = "BUYMACD"
	} else if priceM5 < ma60M5 && DIFDOWNM5 && ColANDDIFdownM5 {
		MACDM5 = "SELLMACD"
	}

	if MACDM5 != validMACD {
		// 5m 未满足 → 早退
		return types.CoinIndicator{}, false
	}

	// 最终条件：4h + 1h + 15m + 5m  满足
	if MACDH4 == validMACD && MACDH1 == validMACD && MACDM15 == validMACD && MACDM5 == validMACD {
		op := "BUY"
		if !isBuy {
			op = "SELL"
		}
		return types.CoinIndicator{
			Symbol:    sym,
			Status:    "Signal",
			Operation: op,
		}, true
	}

	return types.CoinIndicator{}, false
}

// runScanMIDOnce：一次完整扫描（并发分析所有候选，并即时通知满足四周期条件的币）
func runScanMIDOnce(client *futures.Client, maxWorkers int64, wait_sucess_token, chatID string) error {

	//7秒获K
	time.Sleep(7 * time.Second)
	if len(newSymbols) == 0 {
		progressLogger.Println("新币合约启动失败")
	}
	if len(topGainers) == 0 {
		progressLogger.Println("涨幅榜启动失败")
	}
	// 1) 获取候选（和你原来代码保持一致）
	CGTopGainers, err := utils.GetCGTopGainers()
	if err != nil {
		progressLogger.Printf("get CG topgainers err: %v\n", err)
	}
	candidates, _ := utils.GetHotCoins(ticker24h, slipCoinNo, banSymbols,
		utils.VolumeCMCCSlip(ticker24h, newSymbols),
		utils.VolumeCMCCSlip(ticker24h, topGainers),
		utils.VolumeCMCCSlip(ticker24h, CGTopGainers),
	)
	// 并发准备
	var (
		resMu sync.Mutex
		wg    sync.WaitGroup
		sem   = semaphore.NewWeighted(maxWorkers)
	)

	signals := make([]types.CoinIndicator, 0, 8)
	found := make(map[string]struct{}) // 本次 run 内去重

	for _, c := range candidates {
		// Acquire with context so it won't hang forever
		if err := sem.Acquire(context.Background(), 1); err != nil {
			progressLogger.Printf("semaphore acquire 失败: %v", err)
			continue
		}
		wg.Add(1)

		go func(c types.Candidate) {
			defer wg.Done()
			defer sem.Release(1)

			// analyseSymbolForSignal 会拉 3d/1d/4h/1h/15m 并判断四框架条件
			ind, ok := analyseSymbolMIDForSignal(client, c)
			if !ok {
				return
			}

			// 本次 run 内去重（防止同一符号被多次加入）
			resMu.Lock()
			if _, exists := found[ind.Symbol]; !exists {
				signals = append(signals, ind)
				found[ind.Symbol] = struct{}{}
			}
			resMu.Unlock()
		}(c)
	}

	wg.Wait()

	// 发送通知
	for _, s := range signals {
		var opIcon, opText, symIcon string

		if s.Operation == "BUY" {
			opIcon = "🟢"
			opText = "做多"
			symIcon = "🟢" + s.Symbol
		} else if s.Operation == "SELL" {
			opIcon = "🔴"
			opText = "做空"
			symIcon = "🔴" + s.Symbol
		} else {
			// 兜底，避免空值
			opIcon = "⚪"
			opText = s.Operation
			symIcon = s.Symbol
		}

		msg := fmt.Sprintf("%s%s ：%s", opIcon, opText, symIcon)

		if err := telegram.SendMessageL(wait_sucess_token, chatID, msg); err != nil {
			progressLogger.Printf("SendMessage 失败: %v\n", err)
		} else {
			progressLogger.Printf("已推送信号: %s %s\n", s.Symbol, s.Operation)
		}
	}

	return nil
}

// analyseSymbolForSignal：一次性检查 3d 1d 4h 1h；只有四个判定全部匹配时返回 true
func analyseSymbolMIDForSignal(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	// 防止 panic
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[analyseSymbolForSignal] panic recovered %s : %v\n", c.Symbol, r)
		}
	}()

	sym := c.Symbol

	// --- STEP 0: 3d （共振筛） ---
	closesD3, err := utils.GetClosesWithFallback(client, sym, "3d")
	if err != nil || len(closesD3) < 1 {
		progressLogger.Printf("%s 3D 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceD3 := closesD3[len(closesD3)-1]
	MA60D3 := utils.CalculateMA(closesD3, 60)
	DIFUPD3 := utils.IsDIFUP(closesD3, 6, 13, 5)
	DIFDOWND3 := utils.IsDIFDOWN(closesD3, 6, 13, 5)

	MACDD3 := "RANGE"
	if (inBE(sym) || priceD3 > MA60D3) && DIFUPD3 { //MA60	 +	 DIF水上
		MACDD3 = "BUYMACD"
	} else if (inBE(sym) || priceD3 < MA60D3) && DIFDOWND3 {
		if !inBE(sym) { //只能空BE
			return types.CoinIndicator{}, false
		}
		MACDD3 = "SELLMACD"
	} else {
		// 3d 不满足趋势，早退
		return types.CoinIndicator{}, false
	}

	// 基于 3d 决定本次要查 BUY 还是 SELL
	isBuy := (MACDD3 == "BUYMACD")
	validMACD := "BUYMACD"
	if !isBuy {
		validMACD = "SELLMACD"
	}

	// --- STEP A: 先拉 1d（用来做快速筛） ---
	closesD1, err := utils.GetClosesWithFallback(client, sym, "1d")
	if err != nil || len(closesD1) < 1 {
		progressLogger.Printf("%s 1d 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceD1 := closesD1[len(closesD1)-1]
	ColANDDIFUPD1 := utils.ColANDDIFUP(closesD1, 6, 13, 5)
	ColANDDIFDOWND1 := utils.ColANDDIFDOWN(closesD1, 6, 13, 5)
	DIFD1UP := utils.IsDIFUP(closesD1, 6, 13, 5)
	DIFD1DOWN := utils.IsDIFDOWN(closesD1, 6, 13, 5)
	MA60D1 := utils.CalculateMA(closesD1, 60)

	MACDD1 := ""
	if DIFD1UP && priceD1 > MA60D1 && ColANDDIFUPD1 { //MA60	+	DIF水上	+	当下柱线同向
		MACDD1 = "BUYMACD"
	} else if DIFD1DOWN && priceD1 < MA60D1 && ColANDDIFDOWND1 {
		MACDD1 = "SELLMACD"
	}

	if MACDD1 != validMACD {
		// 1D 趋势与 3D 不一致 → 早退
		return types.CoinIndicator{}, false
	}

	// --- STEP B: 4H（做第二道筛） ---
	closesH4, err := utils.GetClosesWithFallback(client, sym, "4h")
	if err != nil || len(closesH4) < 1 {
		progressLogger.Printf("%s 4H 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceH4 := closesH4[len(closesH4)-1]
	MA60H4 := utils.CalculateMA(closesH4, 60)
	ColANDDIFUPH4 := utils.ColANDDIFUP(closesH4, 6, 13, 5)
	ColANDDIFDOWNH4 := utils.ColANDDIFDOWN(closesH4, 6, 13, 5)
	DIFH4UP := utils.IsDIFUP(closesH4, 6, 13, 5)
	DIFH4DOWN := utils.IsDIFDOWN(closesH4, 6, 13, 5)

	MACDH4 := "RANGE"
	if priceH4 > MA60H4 && DIFH4UP && ColANDDIFUPH4 { //MA60	+	DIF水上	+	当下柱线同向
		MACDH4 = "BUYMACD"
	} else if priceH4 < MA60H4 && DIFH4DOWN && ColANDDIFDOWNH4 {
		MACDH4 = "SELLMACD"
	}

	if MACDH4 != validMACD {
		// 4H 趋势与 3D 不一致 → 早退
		return types.CoinIndicator{}, false
	}

	// --- STEP C: 1H （更细致） ---
	closesH1, err := utils.GetClosesWithFallback(client, sym, "1h")
	if err != nil || len(closesH1) < 1 {
		progressLogger.Printf("%s 1H 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceH1 := closesH1[len(closesH1)-1]
	ma60H1 := utils.CalculateMA(closesH1, 60)
	ColANDDIFUPH1 := utils.ColANDDIFUP(closesH1, 6, 13, 5)
	ColANDDIFDOWNH1 := utils.ColANDDIFDOWN(closesH1, 6, 13, 5)
	DIFUPH1 := utils.IsDIFUP(closesH1, 6, 13, 5)
	DIFDOWNH1 := utils.IsDIFDOWN(closesH1, 6, 13, 5)

	MACDH1 := "RANGE"
	if priceH1 > ma60H1 && DIFUPH1 && ColANDDIFUPH1 { //MA60	+	DIF水上		+线柱同向
		MACDH1 = "BUYMACD"
	} else if priceH1 < ma60H1 && DIFDOWNH1 && ColANDDIFDOWNH1 {
		MACDH1 = "SELLMACD"
	}

	if MACDH1 != validMACD {
		// 1h 未满足 → 早退
		return types.CoinIndicator{}, false
	}

	// 最终条件： 3d + 1d + 4h + 1h + 15m 满足
	if MACDD3 == validMACD && MACDD1 == validMACD && MACDH4 == validMACD && MACDH1 == validMACD {
		op := "BUY"
		if !isBuy {
			op = "SELL"
		}
		return types.CoinIndicator{
			Symbol:    sym,
			Status:    "Signal",
			Operation: op,
		}, true
	}

	return types.CoinIndicator{}, false
}

// 辅助函数
func setHTTPClient(c *futures.Client) {
	proxy, _ := url.Parse(proxyURL)
	tr := &http.Transport{
		Proxy:           http.ProxyURL(proxy),
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	c.HTTPClient = &http.Client{
		Transport: tr,
		Timeout:   10 * time.Second,
	}
}
func latestMessagesHandler(w http.ResponseWriter, r *http.Request) {
	// 参数limit，默认5
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessages(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}
func latestMessagesLongHandler(w http.ResponseWriter, r *http.Request) {
	// 参数limit，默认5
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessagesL(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}
func inBE(sym string) bool {
	for _, s := range BE {
		if sym == s {
			return true
		}
	}
	return false
}
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
