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
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
	"golang.org/x/sync/semaphore"
)

/* ====================== 结构体 & 全局 ====================== */

var (
	apiKey                  = ""
	secretKey               = ""
	proxyURL                = "http://127.0.0.1:10809"
	klinesCount             = 200
	maxWorkers              = 20
	wait_energe_botToken    = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //CEX短线
	energe_waiting_botToken = "7417712542:AAGjCOMeFFFuNCo5vNBWDYJqGs0Qm2ifwmY" //CEX短线等待
	long_energe_bot         = "8429540001:AAH-bqd5aRxAVr37aGOKTzKlTmURdiJvYyg" //CEX中线
	long_waiting_bot        = "8236814626:AAHFq7SmeJ16Lvr1e0c_eoJSTLfqDtSqFCA" //CEX中线等待
	CEX_BIG_BOT             = "8433752576:AAEKwIwcJFgUfT2mAzYXe4JrhEEYEEfyOoE" //CEX长线
	CEX_BIG_WAIT_BOT        = "8274690252:AAHjGJQt61NKPlzY20Q60zCFofQ1Q1DDL_8" //CEX长线等待
	chatID                  = "6074996357"

	smallVol = 20000000  //2千万
	longVol  = 300000000 //3亿

	slipCoinNo     = []string{}
	progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)
	waitChan       = make(chan []types.CoinIndicator, 30) //短线等待区
	waitChanLong   = make(chan []types.CoinIndicator, 30) //中线等待区
	waitChanBIG    = make(chan []types.CoinIndicator, 30) //长线等待区
	topGainers     = []string{}                           //涨幅榜
	newSymbols     = []string{}                           //新币合约
	banSymbols     = []string{}                           //封禁区
	ticker24h      = []utils.Ticker24h{}                  //24H的数据
)

/* ====================== 主函数 ====================== */

func main() {
	progressLogger.Println("程序启动...")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/latest-tg-messages", latestMessagesHandler)
	mux.HandleFunc("/api/latest-tg-messages-waiting", latestMessagesWaitingHandler)
	mux.HandleFunc("/api/latest-tg-messages-long", latestMessagesLongHandler)
	mux.HandleFunc("/api/latest-tg-messages-waiting-long", latestMessagesWaitingLongHandler)
	mux.HandleFunc("/api/latest-tg-messages-long-big", latestMessagesLongBIGHandler)
	mux.HandleFunc("/api/latest-tg-messages-waiting-long-big", latestMessagesWaitingLongBIGHandler)

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
	chBanListToWaitList := make(chan []string, 10)
	banSymbols = utils.GetBanList()
	go utils.StartBanListFetcher(chBanList, chBanListToWaitList)
	go func() {
		for Symbols := range chBanList {
			banSymbols = Symbols
		}
	}()

	// 首次 runScan 成功后再启动等待区
	go utils.WaitEnerge(
		waitChan,
		wait_energe_botToken,
		chatID,
		client,
		klinesCount,
		energe_waiting_botToken,
		chBanListToWaitList,
	)

	go utils.WaitEnergeL(
		waitChanLong,
		long_energe_bot,
		chatID,
		client,
		klinesCount,
		long_waiting_bot,
	)

	go utils.WaitEnergeLB(
		waitChanBIG,
		CEX_BIG_BOT,
		chatID,
		client,
		klinesCount,
		CEX_BIG_WAIT_BOT,
	)

	//短线监控模型
	go func() {
		progressLogger.Printf("[runScan] 首次立即执行: %s", time.Now().Format("15:04:05"))
		if err := runScan(client); err != nil {
			progressLogger.Printf("首次 runScan 出错: %v", err)
		}
		// 计算下一次对齐时间
		now := time.Now()
		minutesToNext := 5 - (now.Minute() % 5)
		nextAligned := now.Truncate(time.Minute).Add(time.Duration(minutesToNext) * time.Minute).Truncate(time.Minute)

		delay := time.Until(nextAligned)

		time.AfterFunc(delay, func() {
			if err := runScan(client); err != nil {
				progressLogger.Printf("对齐 runScan 出错: %v", err)
			}

			ticker := time.NewTicker(5 * time.Minute)
			for t := range ticker.C {
				progressLogger.Printf("[runScan] 每5分钟触发: %s", t.Format("15:04:05"))
				go func() {
					if err := runScan(client); err != nil {
						progressLogger.Printf("周期 runScan 出错: %v", err)
					}
				}()
			}
		})
	}()

	//中线监控模型
	go func() {
		progressLogger.Printf("[runScanLong] 首次立即执行: %s", time.Now().Format("15:04:05"))
		if err := runScanLong(client); err != nil {
			progressLogger.Printf("首次 runScanLong 出错: %v", err)
		}

		// 计算下一次整点时间
		now := time.Now()
		nextAligned := now.Truncate(time.Hour).Add(time.Hour) // 下一整点
		delay := time.Until(nextAligned)

		time.AfterFunc(delay, func() {
			if err := runScanLong(client); err != nil {
				progressLogger.Printf("整点 runScan 出错: %v", err)
			}

			ticker := time.NewTicker(1 * time.Hour)
			for t := range ticker.C {
				progressLogger.Printf("[runScanLong] 每小时触发: %s", t.Format("15:04:05"))
				go func() {
					if err := runScanLong(client); err != nil {
						progressLogger.Printf("周期 runScan 出错: %v", err)
					}
				}()
			}
		})
	}()

	//长线监控模型
	go func() {
		progressLogger.Printf("[runScanBIG] 首次立即执行: %s", time.Now().Format("15:04:05"))
		if err := runScanBIG(client); err != nil {
			progressLogger.Printf("首次 runScanBIG 出错: %v", err)
		}

		// 计算下一次整点时间
		now := time.Now()
		nextAligned := now.Truncate(8 * time.Hour).Add(8 * time.Hour) // 下一个 8 小时整点
		delay := time.Until(nextAligned)

		time.AfterFunc(delay, func() {
			if err := runScanBIG(client); err != nil {
				progressLogger.Printf("8小时整点 runScan 出错: %v", err)
			}

			ticker := time.NewTicker(8 * time.Hour)
			for t := range ticker.C {
				progressLogger.Printf("[runScanBIG] 每8小时触发: %s", t.Format("15:04:05"))
				go func() {
					if err := runScanBIG(client); err != nil {
						progressLogger.Printf("周期 runScanBIG 出错: %v", err)
					}
				}()
			}
		})
	}()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("收到退出信号，程序结束")
}

func runScan(client *futures.Client) error {
	//10秒获K
	time.Sleep(10 * time.Second)

	var results []types.CoinIndicator

	// ---------- 1. 构建合并候选 ----------
	if len(newSymbols) == 0 {
		progressLogger.Println("新币合约启动失败")
	}
	if len(topGainers) == 0 {
		progressLogger.Println("涨幅榜启动失败")
	}

	CGTopGainers, err := utils.GetCGTopGainers()
	if err != nil {
		fmt.Println("get CG topgainers", err)
	}
	candidates, _ := utils.GetHotCoins(ticker24h, slipCoinNo, banSymbols,
		utils.VolumeCMCCSlip(ticker24h, newSymbols),
		utils.VolumeCMCCSlip(ticker24h, topGainers),
		utils.VolumeCMCCSlip(ticker24h, CGTopGainers),
	)

	// ---------- 2. 并发分析 ----------
	var (
		resMu sync.Mutex
		wg    sync.WaitGroup
		sem   = semaphore.NewWeighted(int64(maxWorkers))
	)
	for _, c := range candidates {
		if err := sem.Acquire(context.Background(), 1); err != nil {
			progressLogger.Printf("semaphore acquire 失败: %v", err)
			continue
		}
		wg.Add(1)
		go func(c types.Candidate) {
			defer wg.Done()
			defer sem.Release(1)

			ind, ok := analyseSymbol(client, c)
			if ok {
				resMu.Lock()
				results = append(results, ind)
				resMu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	// ---------- 3. 发送等待区 channel ----------
	select {
	case waitChan <- results:
	default:
		progressLogger.Println("waitChan 被阻塞，跳过本次发送")
	}

	sort.Slice(results, func(i, j int) bool { return results[i].StochRSI > results[j].StochRSI })

	return nil
}

func runScanLong(client *futures.Client) error {
	//10秒获K
	time.Sleep(10 * time.Second)
	// ---------- 1. 构建合并候选 ----------
	if len(newSymbols) == 0 {
		progressLogger.Println("新币合约启动失败")
	}
	if len(topGainers) == 0 {
		progressLogger.Println("涨幅榜启动失败")
	}

	CGTopGainers, err := utils.GetCGTopGainers()
	if err != nil {
		fmt.Println("get CG topgainers", err)
	}
	candidates, _ := utils.GetHotCoins(ticker24h, slipCoinNo, banSymbols,
		utils.VolumeCMCCSlip(ticker24h, newSymbols),
		utils.VolumeCMCCSlip(ticker24h, topGainers),
		utils.VolumeCMCCSlip(ticker24h, CGTopGainers),
	)

	//Long代币列表
	LongSymbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT"}

	var resultsLong []types.CoinIndicator
	for _, sym := range LongSymbols {
		ind, ok := analyseSymbolLong(client, types.Candidate{Symbol: sym})
		if ok {
			resultsLong = append(resultsLong, ind)
		}
	}

	// 再分析 candidates
	for _, cand := range candidates {
		ind, ok := analyseSymbolLong(client, cand)
		if ok {
			resultsLong = append(resultsLong, ind)
		}
	}
	// ---------- 3. 发送等待区 channel ----------
	select {
	case waitChanLong <- resultsLong:
	default:
		progressLogger.Println("waitChan 被阻塞，跳过本次发送")
	}

	return nil
}
func runScanBIG(client *futures.Client) error {
	//10秒获K
	time.Sleep(10 * time.Second)
	//Long代币列表
	BIGSymbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "HYPEUSDT", "DOGEUSDT", "PAXGUSDT"}

	var resultsBIG []types.CoinIndicator
	for _, sym := range BIGSymbols {
		ind, ok := analyseSymbolBIG(client, types.Candidate{Symbol: sym})
		if ok {
			resultsBIG = append(resultsBIG, ind)
		}
	}

	// ---------- 3. 发送等待区 channel ----------
	select {
	case waitChanBIG <- resultsBIG:
	default:
		progressLogger.Println("waitChan 被阻塞，跳过本次发送")
	}

	return nil
}

/* ====================== 短线分析 ====================== */
func analyseSymbol(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	// 使用 defer 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			fmt.Printf("[analyseSymbol] Panic recovered for symbol %s: %v\n", c.Symbol, r)
			// 返回默认值，表示处理失败
			// 你也可以根据需求记录到日志文件或监控系统
		}
	}()
	symbol := c.Symbol

	//短线过滤2千万
	volbool := utils.CheckVolume(ticker24h, symbol, float64(smallVol))
	if !volbool {
		return types.CoinIndicator{}, false
	}

	var inst string
	var MACDH1 string
	var closesH1, closesM15 []float64
	var err error

	//短线大时
	closesH1, err = utils.GetClosesWithFallback(client, symbol, "1h")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}
	//大时趋势环境
	price := closesH1[len(closesH1)-1]
	BIGDIFUP := utils.IsDIFUP(closesH1, 6, 13, 5)
	BIGDIFDOWN := utils.IsDIFDOWN(closesH1, 6, 13, 5)
	_, ema25H1Now := utils.CalculateEMA(closesH1, 25)
	MA60H1 := utils.CalculateMA(closesH1, 60)

	if price > ema25H1Now && price > MA60H1 && BIGDIFUP {
		MACDH1 = "BUYMACD"
	} else if price < ema25H1Now && price < MA60H1 && BIGDIFDOWN {
		MACDH1 = "SELLMACD"
	} else {
		return types.CoinIndicator{}, false
	}

	//中时
	closesM15, err = utils.GetClosesWithFallback(client, symbol, "15m")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	//中时未有效跌破
	pricePre := closesM15[len(closesM15)-2]
	pricePre2 := closesM15[len(closesM15)-3]
	_, EMA25M15NOW := utils.CalculateEMA(closesM15, 25)
	DIFM15UP := utils.IsDIFUP(closesM15, 6, 13, 5)
	DIFM15DOWN := utils.IsDIFDOWN(closesM15, 6, 13, 5)

	if (pricePre > EMA25M15NOW || pricePre2 > EMA25M15NOW) && DIFM15UP {
		if MACDH1 == "BUYMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BUY",
				Inst:      inst,
			}, true
		}
	} else if (pricePre < EMA25M15NOW || pricePre2 < EMA25M15NOW) && DIFM15DOWN && (symbol == "BTCUSDT" || symbol == "ETHUSDT") {
		if MACDH1 == "SELLMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "SELL",
				Inst:      inst,
			}, true
		}
	} else {
		return types.CoinIndicator{}, false
	}

	return types.CoinIndicator{}, false
}

/* ====================== 中线分析 ====================== */
func analyseSymbolLong(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			fmt.Printf("[analyseSymbolLong] Panic recovered for symbol %s: %v\n", c.Symbol, r)
		}
	}()
	symbol := c.Symbol

	//长线过滤3亿
	volbool := utils.CheckVolume(ticker24h, symbol, float64(longVol))
	if !volbool {
		return types.CoinIndicator{}, false
	}

	var inst string

	var MACDD1 string
	var closesD1, closesH4 []float64
	var err error

	closesD1, err = utils.GetClosesWithFallback(client, symbol, "1d")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	price := closesD1[len(closesD1)-1]
	BIGDIFUP := utils.IsDIFUP(closesD1, 6, 13, 5)
	BIGDIFDOWN := utils.IsDIFDOWN(closesD1, 6, 13, 5)
	_, EMA25D1NOW := utils.CalculateEMA(closesD1, 25)
	MA60D1 := utils.CalculateMA(closesD1, 60)

	if price > EMA25D1NOW && price > MA60D1 && BIGDIFUP {
		MACDD1 = "BUYMACD"
	} else if price < EMA25D1NOW && price < MA60D1 && BIGDIFDOWN {
		MACDD1 = "SELLMACD"
	} else {
		return types.CoinIndicator{}, false
	}
	closesH4, err = utils.GetClosesWithFallback(client, symbol, "4h")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	//中时未有效跌破
	pricePre := closesH4[len(closesH4)-2]
	pricePre2 := closesH4[len(closesH4)-3]
	_, EMA25H4NOW := utils.CalculateEMA(closesH4, 25)
	DIFH4UP := utils.IsDIFUP(closesH4, 6, 13, 5)
	DIFH4DOWN := utils.IsDIFDOWN(closesH4, 6, 13, 5)
	if (pricePre > EMA25H4NOW || pricePre2 > EMA25H4NOW) && DIFH4UP {
		if MACDD1 == "BUYMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BUYLong",
				Inst:      inst,
			}, true
		}
	} else if (pricePre < EMA25H4NOW || pricePre2 < EMA25H4NOW) && DIFH4DOWN && (symbol == "BTCUSDT" || symbol == "ETHUSDT") {
		if MACDD1 == "SELLMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "SELLLong",
				Inst:      inst,
			}, true
		}
	} else {
		return types.CoinIndicator{}, false
	}
	return types.CoinIndicator{}, false
}

/* ====================== 长线分析 ====================== */
func analyseSymbolBIG(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			fmt.Printf("[analyseSymbolLong] Panic recovered for symbol %s: %v\n", c.Symbol, r)
		}
	}()
	symbol := c.Symbol

	var inst string

	var MACDW1 string
	var closesW1, closesD3 []float64
	var err error

	closesW1, err = utils.GetClosesWithFallback(client, symbol, "1w")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	price := closesW1[len(closesW1)-1]
	BIGDIFUP := utils.IsDIFUP(closesW1, 6, 13, 5)
	BIGDIFDOWN := utils.IsDIFDOWN(closesW1, 6, 13, 5)
	_, EMA25W1NOW := utils.CalculateEMA(closesW1, 25)
	MA60W1 := utils.CalculateMA(closesW1, 60)

	if price > EMA25W1NOW && price > MA60W1 && BIGDIFUP {
		MACDW1 = "BUYMACD"
	} else if price < EMA25W1NOW && price < MA60W1 && BIGDIFDOWN {
		MACDW1 = "SELLMACD"
	} else {
		return types.CoinIndicator{}, false
	}
	closesD3, err = utils.GetClosesWithFallback(client, symbol, "3d")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	//中时未有效跌破
	pricePre := closesD3[len(closesD3)-2]
	pricePre2 := closesD3[len(closesD3)-3]
	_, EMA25D3NOW := utils.CalculateEMA(closesD3, 25)
	DIFD3UP := utils.IsDIFUP(closesD3, 6, 13, 5)
	DIFD3DOWN := utils.IsDIFDOWN(closesD3, 6, 13, 5)
	if (pricePre > EMA25D3NOW || pricePre2 > EMA25D3NOW) && DIFD3UP {
		if MACDW1 == "BUYMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BUYLongB",
				Inst:      inst,
			}, true
		}
	} else if (pricePre < EMA25D3NOW || pricePre2 < EMA25D3NOW) && DIFD3DOWN {
		if MACDW1 == "SELLMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "SELLLongB",
				Inst:      inst,
			}, true
		}
	} else {
		return types.CoinIndicator{}, false
	}
	return types.CoinIndicator{}, false
}

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
func latestMessagesWaitingHandler(w http.ResponseWriter, r *http.Request) {
	// 参数limit，默认1
	limit := 1
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessagesWaiting(limit)
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
func latestMessagesWaitingLongHandler(w http.ResponseWriter, r *http.Request) {
	// 参数limit，默认1
	limit := 1
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessagesWaitingL(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func latestMessagesLongBIGHandler(w http.ResponseWriter, r *http.Request) {
	// 参数limit，默认5
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessagesLB(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}
func latestMessagesWaitingLongBIGHandler(w http.ResponseWriter, r *http.Request) {
	// 参数limit，默认1
	limit := 1
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessagesWaitingLB(limit)
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
