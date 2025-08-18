package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"energe/model"
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
	limitVolume               = 5000000000                                       //50亿 USDT
	botToken                  = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //二级印钞
	wait_energe_botToken      = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //播报成功（合并右侧回响）
	energe_waiting_botToken   = "7417712542:AAGjCOMeFFFuNCo5vNBWDYJqGs0Qm2ifwmY" //等待区bot
	high_profit_srsi_botToken = "7924943629:AAEontupSGOxEm4TPJE6tc-CSzTAqlzwQNY" //极品左侧抄底bot
	chatID                    = "6074996357"

	// volumeMap      = map[string]float64{}
	volumeCache *types.VolumeCache
	err         error
	slipCoin    = []string{"XRPUSDT", "1000PEPEUSDT", "ADAUSDT", "BNBUSDT", "AGIXUSDT",
		"LINKUSDT", "FARTCOINUSDT", "1000BONKUSDT", "AVAXUSDT", "LTCUSDT", "ALPACAUSDT",
		"BCHUSDT", "XLMUSDT", "XRPUSDC", "BNXUSDT", "ETHUSDC", "BTCUSDC", "SOLUSDC", "VIDTUSDT",
		"DOTUSDT", "NEARUSDT", "ARBUSDT", "1000SHIBUSDT", "TRXUSDT", "PNUTUSDT", "HYPEUSDT",
		"HBARUSDT", "1INCHUSDT", "SUIUSDC", "1000FLOKIUSDT", "GALAUSDT", "TIAUSDT", "ETHFIUSDT",
		"WLDUSDT", "FILUSDT", "TAOUSDT", "CRVUSDT", "FETUSDT", "INJUSDT", "1000BONKUSDC",
		"SPXUSDT", "TONUSDT", "ETCUSDT", "PUMPUSDT", "ENAUSDT", "LDOUSDT", "NEIROUSDT", "AAVEUSDT",
		"UNIUSDT", "APTUSDT", "TRUMPUSDT", "DOGEUSDC", "VIRTUALUSDT", "SEIUSDT", "WIFUSDT",
		"ONDOUSDT", "MOODENGUSDT", "PENGUUSDT", "NEIROETHUSDT", "CROSSUSDT", "SUIUSDT", "OPUSDT",
		"FXSUSDT", "DOGEUSDT", "SOLUSDT", "VINEUSDT"} // 想排除的币放这里
	muVolumeMap    sync.Mutex
	progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)
	db_trend       *sql.DB
	waitChan       = make(chan []types.CoinIndicator, 30) //等待区
	betrend        types.BETrend
)

/* ====================== 主函数 ====================== */

func main() {
	progressLogger.Println("程序启动...")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/latest-tg-messages", latestMessagesHandler)
	mux.HandleFunc("/api/latest-tg-messages-waiting", latestMessagesWaitingHandler)

	go func() {
		if err := http.ListenAndServe(":8888", corsMiddleware(mux)); err != nil {
			log.Fatalf("HTTP服务器启动失败: %v", err)
		}
	}()

	client := binance.NewFuturesClient(apiKey, secretKey)
	setHTTPClient(client)

	// 创建并预热 VolumeCache
	volumeCache, err = utils.NewVolumeCache(client, slipCoin, float64(limitVolume))
	if err != nil {
		log.Fatalf("VolumeCache 启动失败: %v", err)
	}

	<-volumeCache.Ready()
	log.Println("volumeCache 启动成功")
	defer volumeCache.Close()

	fmt.Println(volumeCache.SymbolsAbove(float64(limitVolume)))

	model.InitDBTrend()
	db_trend = model.DBTrend

	if db_trend == nil {
		log.Fatal("db_trend 初始化失败，DBTrend 为空")
	}

	go func() {
		progressLogger.Printf("[runScan] 首次立即执行: %s", time.Now().Format("15:04:05"))
		if err := runScan(client); err != nil {
			progressLogger.Printf("首次 runScan 出错: %v", err)
		}

		// 计算下一次对齐时间
		now := time.Now()
		minutesToNext := 15 - (now.Minute() % 15)
		nextAligned := now.Truncate(time.Minute).Add(time.Duration(minutesToNext) * time.Minute).Truncate(time.Minute)

		delay := time.Until(nextAligned)
		progressLogger.Printf("[runScan] 下一次对齐在 %s 执行（等待 %v）", nextAligned.Format("15:04:05"), delay)

		time.AfterFunc(delay, func() {
			progressLogger.Printf("[runScan] 对齐执行: %s", time.Now().Format("15:04:05"))
			if err := runScan(client); err != nil {
				progressLogger.Printf("对齐 runScan 出错: %v", err)
			}

			ticker := time.NewTicker(15 * time.Minute)
			for t := range ticker.C {
				progressLogger.Printf("[runScan] 每15分钟触发: %s", t.Format("15:04:05"))
				go func() {
					if err := runScan(client); err != nil {
						progressLogger.Printf("周期 runScan 出错: %v", err)
					}
				}()
			}
		})
	}()
	//开启等待区
	go utils.WaitEnerge(waitChan, db_trend, wait_energe_botToken, chatID, client, klinesCount, energe_waiting_botToken)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("收到退出信号，程序结束")
}

/* ====================== 核心扫描 ====================== */

func runScan(client *futures.Client) error {
	progressLogger.Println("开始新一轮扫描...")

	// ---------- 1. 过滤 USDT 交易对 ----------
	var symbols []string
	if volumeCache == nil {
		progressLogger.Println("volumeCache 尚未准备好")
		return nil
	}
	symbols = volumeCache.SymbolsAbove(float64(limitVolume))
	progressLogger.Printf("USDT 交易对数量: %d", len(symbols))

	// ---------- 3. 并发处理 ----------
	var (
		results []types.CoinIndicator
		resMu   sync.Mutex
		wg      sync.WaitGroup
		sem     = semaphore.NewWeighted(int64(maxWorkers))
	)

	for _, symbol := range symbols {
		if err := sem.Acquire(context.Background(), 1); err != nil {
			progressLogger.Printf("semaphore acquire 失败: %v", err)
			continue
		}

		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			defer sem.Release(1)

			ind, ok := analyseSymbol(sym, db_trend)
			if ok {
				resMu.Lock()
				results = append(results, ind)
				resMu.Unlock()
			}
		}(symbol)
	}
	wg.Wait()

	select {
	case waitChan <- results:
	default:
		progressLogger.Println("waitChan 被阻塞，跳过本次发送")
	}

	progressLogger.Printf("本轮符合条件标的数量: %d", len(results))

	sort.Slice(results, func(i, j int) bool {
		return results[i].StochRSI > results[j].StochRSI // “>” 表示降序
	})

	// ---------- 4. 推送到 Telegram ----------
	return utils.PushTelegram(results, botToken, high_profit_srsi_botToken, chatID, volumeCache)
}

/* ====================== 单币分析 ====================== */

func analyseSymbol(symbol string, db_trend *sql.DB) (types.CoinIndicator, bool) {

	MACDM5, _ := utils.GetTrendResult(db_trend, symbol, "5m")
	MACDM15, _ := utils.GetTrendResult(db_trend, symbol, "15m")
	MACDH1, _ := utils.GetTrendResult(db_trend, symbol, "1h")
	//BuyMACDH4, _ := utils.GetTrendResult(db, symbol, "4h")
	//BuyMACDD1, _ := utils.GetTrendResult(db, symbol, "1d")
	//BuyMACDD3, _ := utils.GetTrendResult(db, symbol, "3d")

	if MACDH1 == "RANGE" {
		progressLogger.Printf("奇点 触发: %s", symbol)
		status := "Singu"
		return types.CoinIndicator{
			Symbol:    symbol,
			Status:    status,
			Operation: "Singu",
		}, true
	}

	// ===== 模型1 ： Fomo模型  =====
	if MACDH1 == "BUYMACD" {
		progressLogger.Printf("Fomo UP 触发: %s", symbol)
		status := "Wait"
		if MACDM5 == "BUYMACD" && MACDM15 == "BUYMACD" {
			status = "FomoBuy"
		}
		return types.CoinIndicator{
			Symbol:    symbol,
			Status:    status,
			Operation: "FomoBuy",
		}, true
	}

	if MACDH1 == "SELLMACD" {
		progressLogger.Printf("Fomo DOWN 触发: %s", symbol)
		status := "Wait"
		if MACDM5 == "SELLMACD" && MACDM15 == "SELLMACD" {
			status = "FomoSell"
		}
		return types.CoinIndicator{
			Symbol:    symbol,
			Status:    status,
			Operation: "FomoSell",
		}, true
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
