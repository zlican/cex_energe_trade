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

	smallVol       = 20000000 //2千万
	slipCoinNo     = []string{}
	progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)
	topGainers     = []string{}          //涨幅榜
	newSymbols     = []string{}          //新币合约
	banSymbols     = []string{}          //封禁区
	ticker24h      = []utils.Ticker24h{} //24H的数据
)

var runScanRunning int32
var runScanMIDRunning int32

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

	//启动涨幅榜获取
	chTopGainers := make(chan []string)
	chTicker24h := make(chan []utils.Ticker24h)
	topGainers, ticker24h = utils.GetTopGainers()
	go utils.StartTopGainersFetcher(chTopGainers, chTicker24h)
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
		nextAligned := now.Truncate(time.Minute).Add(time.Minute)
		delay := time.Until(nextAligned)
		time.Sleep(delay)

		// 进入每分钟循环（主循环在该 goroutine 内执行）
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for t := range ticker.C {
			progressLogger.Printf("[runScan] 每1分钟触发: %s", t.Format("15:04:05"))

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
		nextAligned := now.Truncate(15 * time.Minute).Add(15 * time.Minute)
		delay := time.Until(nextAligned)
		time.Sleep(delay)

		// 进入每分钟循环（主循环在该 goroutine 内执行）
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()

		for t := range ticker.C {
			progressLogger.Printf("[runScanMID] 每15分钟触发: %s", t.Format("15:04:05"))

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

			// analyseSymbolForSignal 会拉 1h/15m/5m/1m 并判断四框架条件
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

// analyseSymbolForSignal：一次性检查 1h,15m,5m,1m；只有四个判定全部匹配时返回 true
func analyseSymbolForSignal(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	// 防止 panic
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[analyseSymbolForSignal] panic recovered %s : %v\n", c.Symbol, r)
		}
	}()

	sym := c.Symbol

	// --- STEP A: 先拉 1h（用来做快速筛） ---
	closesH1, err := utils.GetClosesWithFallback(client, sym, "1h")
	if err != nil || len(closesH1) < 1 {
		progressLogger.Printf("%s 1h 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceH1 := closesH1[len(closesH1)-1]
	_, ema25H1Now := utils.CalculateEMA(closesH1, 25)
	MA60H1 := utils.CalculateMA(closesH1, 60)
	DIFH1UP := utils.IsDIFUP(closesH1, 6, 13, 5)
	DIFH1DOWN := utils.IsDIFDOWN(closesH1, 6, 13, 5)

	MACDH1 := ""
	if priceH1 > ema25H1Now && priceH1 > MA60H1 && DIFH1UP {
		MACDH1 = "BUYMACD"
	} else if priceH1 < ema25H1Now && priceH1 < MA60H1 && DIFH1DOWN {
		if sym != "BTCUSDT" && sym != "ETHUSDT" { //除BE不做空
			return types.CoinIndicator{}, false
		}
		MACDH1 = "SELLMACD"
	} else {
		// 1h 不满足趋势，早退
		return types.CoinIndicator{}, false
	}

	// 基于 1h 决定本次要查 BUY 还是 SELL
	isBuy := (MACDH1 == "BUYMACD")
	validMACD := "BUYMACD"
	if !isBuy {
		validMACD = "SELLMACD"
	}
	validX := "XBUY"
	if !isBuy {
		validX = "XSELL"
	}

	// --- STEP B: 15m （做第二道筛） ---
	closesM15, err := utils.GetClosesWithFallback(client, sym, "15m")
	if err != nil || len(closesM15) < 1 {
		progressLogger.Printf("%s 15m 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceM15 := closesM15[len(closesM15)-1]
	_, ema25M15Now := utils.CalculateEMA(closesM15, 25)
	isGolden := utils.IsGolden(closesM15, 6, 13, 5)
	isDead := utils.IsDead(closesM15, 6, 13, 5)
	DIFM15UP := utils.IsDIFUP(closesM15, 6, 13, 5)
	DIFM15DOWN := utils.IsDIFDOWN(closesM15, 6, 13, 5)

	MACDM15 := "RANGE"
	if priceM15 > ema25M15Now && isGolden && DIFM15UP {
		MACDM15 = "BUYMACD"
	} else if priceM15 < ema25M15Now && isDead && DIFM15DOWN {
		MACDM15 = "SELLMACD"
	}

	if MACDM15 != validMACD {
		// 15m 趋势与 1h 不一致 → 早退
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
	_, ema25M5Now := utils.CalculateEMA(closesM5, 25)
	MACDSmallUP := utils.IsSmallTFUP(closesM5, 6, 13, 5)
	MACDsmallDOWN := utils.IsSmallTFDOWN(closesM5, 6, 13, 5)

	MACDM5 := "RANGE"
	if priceM5 > ema25M5Now && priceM5 > ma60M5 && MACDSmallUP {
		MACDM5 = "BUYMACD"
	} else if priceM5 < ema25M5Now && priceM5 < ma60M5 && MACDsmallDOWN {
		MACDM5 = "SELLMACD"
	}

	if MACDM5 != validMACD {
		// 5m 未满足 → 早退
		return types.CoinIndicator{}, false
	}

	// --- STEP D: 1m （最终触发条件） ---
	closesM1, err := utils.GetClosesWithFallback(client, sym, "1m")
	if err != nil || len(closesM1) == 0 {
		progressLogger.Printf("%s 1m 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceM1 := closesM1[len(closesM1)-1]
	ma60M1 := utils.CalculateMA(closesM1, 60)
	XSTRONGUPM1 := utils.XSTRONGUP(closesM1, 6, 13, 5) // 你之前用的名字
	XSTRONGDOWNM1 := utils.XSTRONGDOWN(closesM1, 6, 13, 5)

	MACDM1 := ""
	if priceM1 > ma60M1 && XSTRONGUPM1 {
		MACDM1 = "XBUY"
	} else if priceM1 < ma60M1 && XSTRONGDOWNM1 {
		MACDM1 = "XSELL"
	}

	// 最终条件：1h + 15m + 5m + 1m 满足（按你的要求）
	if MACDH1 == validMACD && MACDM15 == validMACD && MACDM5 == validMACD && MACDM1 == validX {
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
	//MID代币列表
	LongSymbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "HYPEUSDT", "PAXGUSDT"}
	var candidates = []types.Candidate{}
	for _, sym := range LongSymbols {
		candidates = append(candidates, types.Candidate{Symbol: sym})
	}
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

			// analyseSymbolForSignal 会拉 1d/4h/1h/15m 并判断四框架条件
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

// analyseSymbolForSignal：一次性检查 1d 4h 1h 15m；只有四个判定全部匹配时返回 true
func analyseSymbolMIDForSignal(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	// 防止 panic
	defer func() {
		if r := recover(); r != nil {
			progressLogger.Printf("[analyseSymbolForSignal] panic recovered %s : %v\n", c.Symbol, r)
		}
	}()

	sym := c.Symbol

	// --- STEP A: 先拉 1d（用来做快速筛） ---
	closesD1, err := utils.GetClosesWithFallback(client, sym, "1d")
	if err != nil || len(closesD1) < 1 {
		progressLogger.Printf("%s 1d 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceD1 := closesD1[len(closesD1)-1]
	_, ema25D1Now := utils.CalculateEMA(closesD1, 25)
	MA60D1 := utils.CalculateMA(closesD1, 60)
	DIFD1UP := utils.IsDIFUP(closesD1, 6, 13, 5)
	DIFD1DOWN := utils.IsDIFDOWN(closesD1, 6, 13, 5)

	MACDD1 := ""
	if priceD1 > ema25D1Now && priceD1 > MA60D1 && DIFD1UP {
		MACDD1 = "BUYMACD"
	} else if priceD1 < ema25D1Now && priceD1 < MA60D1 && DIFD1DOWN {
		if sym != "BTCUSDT" && sym != "ETHUSDT" { //除BE不做空
			return types.CoinIndicator{}, false
		}
		MACDD1 = "SELLMACD"
	} else {
		// 1h 不满足趋势，早退
		return types.CoinIndicator{}, false
	}

	// 基于 1h 决定本次要查 BUY 还是 SELL
	isBuy := (MACDD1 == "BUYMACD")
	validMACD := "BUYMACD"
	if !isBuy {
		validMACD = "SELLMACD"
	}
	validX := "XBUY"
	if !isBuy {
		validX = "XSELL"
	}

	// --- STEP B: 15m （做第二道筛） ---
	closesH4, err := utils.GetClosesWithFallback(client, sym, "4h")
	if err != nil || len(closesH4) < 1 {
		progressLogger.Printf("%s 4H 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceH4 := closesH4[len(closesH4)-1]
	_, ema25H4Now := utils.CalculateEMA(closesH4, 25)
	isGolden := utils.IsGolden(closesH4, 6, 13, 5)
	isDead := utils.IsDead(closesH4, 6, 13, 5)
	DIFH4UP := utils.IsDIFUP(closesH4, 6, 13, 5)
	DIFH4DOWN := utils.IsDIFDOWN(closesH4, 6, 13, 5)

	MACDH4 := "RANGE"
	if priceH4 > ema25H4Now && isGolden && DIFH4UP {
		MACDH4 = "BUYMACD"
	} else if priceH4 < ema25H4Now && isDead && DIFH4DOWN {
		MACDH4 = "SELLMACD"
	}

	if MACDH4 != validMACD {
		// 4H 趋势与 1D 不一致 → 早退
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
	_, ema25H1Now := utils.CalculateEMA(closesH1, 25)
	MACDSmallUP := utils.IsSmallTFUP(closesH1, 6, 13, 5)
	MACDsmallDOWN := utils.IsSmallTFDOWN(closesH1, 6, 13, 5)

	MACDH1 := "RANGE"
	if priceH1 > ema25H1Now && priceH1 > ma60H1 && MACDSmallUP {
		MACDH1 = "BUYMACD"
	} else if priceH1 < ema25H1Now && priceH1 < ma60H1 && MACDsmallDOWN {
		MACDH1 = "SELLMACD"
	}

	if MACDH1 != validMACD {
		// 1h 未满足 → 早退
		return types.CoinIndicator{}, false
	}

	// --- STEP D: 15m （最终触发条件） ---
	closesM15, err := utils.GetClosesWithFallback(client, sym, "15m")
	if err != nil || len(closesM15) == 0 {
		progressLogger.Printf("%s 15m 数据不足或获取失败: %v\n", sym, err)
		return types.CoinIndicator{}, false
	}
	priceM15 := closesM15[len(closesM15)-1]
	ma60M15 := utils.CalculateMA(closesM15, 60)
	XSTRONGUPM15 := utils.XSTRONGUP(closesM15, 6, 13, 5) // 你之前用的名字
	XSTRONGDOWNM15 := utils.XSTRONGDOWN(closesM15, 6, 13, 5)

	MACDM15 := ""
	if priceM15 > ma60M15 && XSTRONGUPM15 {
		MACDM15 = "XBUY"
	} else if priceM15 < ma60M15 && XSTRONGDOWNM15 {
		MACDM15 = "XSELL"
	}

	// 最终条件：1d + 4h + 1h + 15m 满足（按你的要求）
	if MACDD1 == validMACD && MACDH4 == validMACD && MACDH1 == validMACD && MACDM15 == validX {
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
