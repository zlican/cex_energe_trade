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
	apiKey                    = ""
	secretKey                 = ""
	proxyURL                  = "http://127.0.0.1:10809"
	klinesCount               = 200
	maxWorkers                = 20
	botToken                  = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //二级印钞
	wait_energe_botToken      = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //播报成功（合并右侧回响）
	energe_waiting_botToken   = "7417712542:AAGjCOMeFFFuNCo5vNBWDYJqGs0Qm2ifwmY" //等待区bot
	high_profit_srsi_botToken = "7924943629:AAEontupSGOxEm4TPJE6tc-CSzTAqlzwQNY" //极品左侧抄底bot
	long_energe_bot           = "8429540001:AAH-bqd5aRxAVr37aGOKTzKlTmURdiJvYyg" //长线模型
	long_waiting_bot          = "8236814626:AAHFq7SmeJ16Lvr1e0c_eoJSTLfqDtSqFCA" //长线等待区
	chatID                    = "6074996357"

	slipCoin = []string{"XRPUSDT", "1000PEPEUSDT", "ADAUSDT", "BNBUSDT", "AGIXUSDT",
		"LINKUSDT", "FARTCOINUSDT", "1000BONKUSDT", "AVAXUSDT", "LTCUSDT", "ALPACAUSDT",
		"BCHUSDT", "XLMUSDT", "XRPUSDC", "BNXUSDT", "ETHUSDC", "BTCUSDC", "SOLUSDC", "VIDTUSDT",
		"DOTUSDT", "NEARUSDT", "ARBUSDT", "1000SHIBUSDT", "TRXUSDT", "PNUTUSDT",
		"HBARUSDT", "1INCHUSDT", "SUIUSDC", "1000FLOKIUSDT", "GALAUSDT", "TIAUSDT", "ETHFIUSDT",
		"FILUSDT", "TAOUSDT", "CRVUSDT", "FETUSDT", "INJUSDT", "1000BONKUSDC",
		"SPXUSDT", "TONUSDT", "ETCUSDT", "PUMPUSDT", "ENAUSDT", "LDOUSDT", "NEIROUSDT", "AAVEUSDT",
		"UNIUSDT", "APTUSDT", "TRUMPUSDT", "DOGEUSDC", "VIRTUALUSDT", "SEIUSDT", "WIFUSDT",
		"ONDOUSDT", "MOODENGUSDT", "PENGUUSDT", "NEIROETHUSDT", "CROSSUSDT", "OPUSDT",
		"FXSUSDT", "DOGEUSDT", "VINEUSDT", "MEMEUSDT", "FHEUSDT", "BERAUSDT", "PEPEUSDT",
		"MITOUSDT", "ATOMUSDT", "SUIUSDT", "EIGENUSDT", "AEROUSDT", "BONKUSDT", "SHIBUSDT",
		"PYTHUSDT", "BIOUSDT", "PIPPINUSDT", "OPUSDT", "IPUSDT", "PARTIUSDT", "SYRUPUSDT",
		"PENDLEUSDT", "TRUMPOFFICIALUSDT",
	} // 想排除的币放这里
	slipCoinHARD = []string{"1000PEPEUSDT", "ADAUSDT",
		"LINKUSDT", "FARTCOINUSDT", "1000BONKUSDT", "AVAXUSDT", "LTCUSDT", "ALPACAUSDT",
		"XLMUSDT", "XRPUSDC", "BNXUSDT", "ETHUSDC", "BTCUSDC", "SOLUSDC", "VIDTUSDT",
		"DOTUSDT", "ARBUSDT", "1000SHIBUSDT", "TRXUSDT",
		"HBARUSDT", "1INCHUSDT", "SUIUSDC", "1000FLOKIUSDT", "GALAUSDT",
		"FILUSDT", "1000BONKUSDC", "MEMEUSDT", "PEPEUSDT",
		"ATOMUSDT", "BONKUSDT", "SHIBUSDT",
	}
	progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)
	waitChan       = make(chan []types.CoinIndicator, 30) //等待区
	waitChanLong   = make(chan []types.CoinIndicator, 30) //等待区
)

/* ====================== 主函数 ====================== */

func main() {
	progressLogger.Println("程序启动...")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/latest-tg-messages", latestMessagesHandler)
	mux.HandleFunc("/api/latest-tg-messages-waiting", latestMessagesWaitingHandler)
	mux.HandleFunc("/api/latest-tg-messages-long", latestMessagesLongHandler)
	mux.HandleFunc("/api/latest-tg-messages-waiting-long", latestMessagesWaitingLongHandler)

	go func() {
		if err := http.ListenAndServe(":8888", corsMiddleware(mux)); err != nil {
			log.Fatalf("HTTP服务器启动失败: %v", err)
		}
	}()

	client := binance.NewFuturesClient(apiKey, secretKey)
	setHTTPClient(client)

	// 首次 runScan 成功后再启动等待区
	go utils.WaitEnerge(
		waitChan,
		wait_energe_botToken,
		chatID,
		client,
		klinesCount,
		energe_waiting_botToken,
	)

	go utils.WaitEnergeL(
		waitChanLong,
		long_energe_bot,
		chatID,
		client,
		klinesCount,
		long_waiting_bot,
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

	//长线监控模型
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
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("收到退出信号，程序结束")
}

func runScan(client *futures.Client) error {
	var results []types.CoinIndicator

	// ---------- 1. 构建合并候选 ----------
	candidates, _ := utils.GetHotCoins(slipCoinHARD)

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

	return utils.PushTelegram(results, botToken, high_profit_srsi_botToken, chatID)
}

func runScanLong(client *futures.Client) error {

	//Long代币列表
	LongSymbols := []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "ETHBTC", "PAXGUSDT", "HYPEUSDT"}
	var resultsLong []types.CoinIndicator
	for _, sym := range LongSymbols {
		ind, ok := analyseSymbolLong(client, types.Candidate{Symbol: sym})
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

	return utils.PushTelegram(resultsLong, botToken, high_profit_srsi_botToken, chatID)
}

/* ====================== 单币分析 ====================== */

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
	var inst string
	var MACDH1 string
	var closesH1, closesM15 []float64
	var err error

	//非理性断线建立在非理性长线之上。。。
	var closesD3, closesD1, closesH4 []float64
	closesD3, err = utils.GetClosesWithFallback(client, symbol, "3d")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}
	priceBIG := closesD3[len(closesD3)-1]
	_, EMA25D3 := utils.CalculateEMA(closesD3, 25)
	if priceBIG > EMA25D3 { //长线正
		closesD1, err = utils.GetClosesWithFallback(client, symbol, "1d")
		if err != nil {
			fmt.Println("获取数据失败:", err)
		}
		_, EMA25D1 := utils.CalculateEMA(closesD1, 25)
		if priceBIG > EMA25D1 {
			closesH4, err = utils.GetClosesWithFallback(client, symbol, "4h")
			if err != nil {
				fmt.Println("获取数据失败:", err)
			}
			_, EMA25H4 := utils.CalculateEMA(closesH4, 25)
			if priceBIG < EMA25H4 {
				return types.CoinIndicator{}, false
			}
		} else {
			return types.CoinIndicator{}, false
		}
	} else if priceBIG < EMA25D3 {
		closesD1, err = utils.GetClosesWithFallback(client, symbol, "1d")
		if err != nil {
			fmt.Println("获取数据失败:", err)
		}
		_, EMA25D1 := utils.CalculateEMA(closesD1, 25)
		if priceBIG < EMA25D1 {
			closesH4, err = utils.GetClosesWithFallback(client, symbol, "4h")
			if err != nil {
				fmt.Println("获取数据失败:", err)
			}
			_, EMA25H4 := utils.CalculateEMA(closesH4, 25)
			if priceBIG > EMA25H4 {
				return types.CoinIndicator{}, false
			}
		} else {
			return types.CoinIndicator{}, false
		}
	} else {
		return types.CoinIndicator{}, false
	}

	//短线大时
	closesH1, err = utils.GetClosesWithFallback(client, symbol, "1h")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}
	//大时趋势环境
	price := closesH1[len(closesH1)-1]
	_, ema25H1Now := utils.CalculateEMA(closesH1, 25)
	ma60H1 := utils.CalculateMA(closesH1, 60)
	DIFUP := utils.IsDIFUP(closesH1, 6, 13, 5)
	DIFDOWN := utils.IsDIFDOWN(closesH1, 6, 13, 5)

	if price > ema25H1Now && price > ma60H1 && DIFUP {
		MACDH1 = "BUYMACD"
	} else if price < ema25H1Now && price < ma60H1 && DIFDOWN {
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

	if pricePre > EMA25M15NOW || pricePre2 > EMA25M15NOW {
		if MACDH1 == "BUYMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BUY",
				Inst:      inst,
			}, true
		}
	} else if pricePre < EMA25M15NOW || pricePre2 < EMA25M15NOW {
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

/* ====================== 单币分析 ====================== */

func analyseSymbolLong(client *futures.Client, c types.Candidate) (types.CoinIndicator, bool) {
	// 使用 defer 捕获可能的 panic
	defer func() {
		if r := recover(); r != nil {
			// 记录 panic 信息，方便调试
			fmt.Printf("[analyseSymbolLong] Panic recovered for symbol %s: %v\n", c.Symbol, r)
			// 返回默认值，表示处理失败
			// 你也可以根据需求记录到日志文件或监控系统
		}
	}()
	symbol := c.Symbol
	var inst string

	var MACDD3 string
	var closesD3, closesD1 []float64
	var err error
	closesD3, err = utils.GetClosesWithFallback(client, symbol, "3d")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	price := closesD3[len(closesD3)-1]
	_, EMA25D3NOW := utils.CalculateEMA(closesD3, 25)
	ma60D3 := utils.CalculateMA(closesD3, 60)
	DIFUP := utils.IsDIFUP(closesD3, 6, 13, 5)
	DIFDOWN := utils.IsDIFDOWN(closesD3, 6, 13, 5)

	if price > EMA25D3NOW && price > ma60D3 && DIFUP {
		MACDD3 = "BUYMACD"
	} else if price < EMA25D3NOW && price < ma60D3 && DIFDOWN {
		MACDD3 = "SELLMACD"
	} else {
		return types.CoinIndicator{}, false
	}
	closesD1, err = utils.GetClosesWithFallback(client, symbol, "1d")
	if err != nil {
		fmt.Println("获取数据失败:", err)
	}

	//中时未有效跌破
	pricePre := closesD1[len(closesD1)-2]
	pricePre2 := closesD1[len(closesD1)-3]
	_, EMA25D1NOW := utils.CalculateEMA(closesD1, 25)
	if pricePre > EMA25D1NOW || pricePre2 > EMA25D1NOW {
		if MACDD3 == "BUYMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BUYLong",
				Inst:      inst,
			}, true
		}
	} else if pricePre < EMA25D1NOW || pricePre2 < EMA25D1NOW {
		if MACDD3 == "SELLMACD" {
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
