package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"energe/model"
	"energe/okx"
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
	limitVolume               = 300000000
	okxCache                  *okx.VolumeCache
	okxLimit                  = 250000000.0                                      // 2.5 亿                                    //3亿 USDT
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
		"DOTUSDT", "NEARUSDT", "ARBUSDT", "1000SHIBUSDT", "TRXUSDT", "PNUTUSDT",
		"HBARUSDT", "1INCHUSDT", "SUIUSDC", "1000FLOKIUSDT", "GALAUSDT", "TIAUSDT", "ETHFIUSDT",
		"WLDUSDT", "FILUSDT", "TAOUSDT", "CRVUSDT", "FETUSDT", "INJUSDT", "1000BONKUSDC",
		"SPXUSDT", "TONUSDT", "ETCUSDT", "PUMPUSDT", "ENAUSDT", "LDOUSDT", "NEIROUSDT", "AAVEUSDT",
		"UNIUSDT", "APTUSDT", "TRUMPUSDT", "DOGEUSDC", "VIRTUALUSDT", "SEIUSDT", "WIFUSDT",
		"ONDOUSDT", "MOODENGUSDT", "PENGUUSDT", "NEIROETHUSDT", "CROSSUSDT", "SUIUSDT", "OPUSDT",
		"FXSUSDT", "DOGEUSDT", "VINEUSDT", "MEMEUSDT", "FHEUSDT", "WLFIUSDT", "BERAUSDT", "PEPEUSDT",
		"HYPEUSDT", "SOLUSDT"} // 想排除的币放这里
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

	okxCache, err = okx.NewVolumeCacheOKX(proxyURL, slipCoin, okxLimit)
	if err != nil {
		log.Fatalf("OKX VolumeCache 启动失败: %v", err)
	}
	<-okxCache.Ready()
	log.Println("okxCache 启动成功")
	defer okxCache.Close()

	fmt.Println(okxCache.SymbolsAboveNotional(float64(okxLimit)))
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
		} else {
			// 首次 runScan 成功后再启动等待区
			go utils.WaitEnerge(
				waitChan,
				db_trend,
				wait_energe_botToken,
				chatID,
				client,
				klinesCount,
				energe_waiting_botToken,
			)
			progressLogger.Printf("[WaitEnerge] 等待区监控已启动: %s", time.Now().Format("15:04:05"))
		}

		// 计算下一次对齐时间
		now := time.Now()
		minutesToNext := 5 - (now.Minute() % 5)
		nextAligned := now.Truncate(time.Minute).Add(time.Duration(minutesToNext) * time.Minute).Truncate(time.Minute)

		delay := time.Until(nextAligned)
		progressLogger.Printf("[runScan] 下一次对齐在 %s 执行（等待 %v）", nextAligned.Format("15:04:05"), delay)

		time.AfterFunc(delay, func() {
			progressLogger.Printf("[runScan] 对齐执行: %s", time.Now().Format("15:04:05"))
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
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("收到退出信号，程序结束")
}

/* ====================== 核心扫描 ====================== */

func runScan(client *futures.Client) error {
	progressLogger.Println("开始新一轮扫描（BINANCE + OKX）...")

	// ---------- 1. 构建合并候选 ----------
	candidates := utils.BuildCandidates(volumeCache, okxCache, float64(limitVolume), okxLimit)
	progressLogger.Printf("合并候选数量: %d", len(candidates))

	// ---------- 2. 并发分析 ----------
	var (
		results []types.CoinIndicator
		resMu   sync.Mutex
		wg      sync.WaitGroup
		sem     = semaphore.NewWeighted(int64(maxWorkers))
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

			ind, ok := analyseSymbol(client, c, db_trend)
			if ok {
				resMu.Lock()
				results = append(results, ind)
				resMu.Unlock()
			}

		}(c)
	}
	wg.Wait()

	select {
	case waitChan <- results:
	default:
		progressLogger.Println("waitChan 被阻塞，跳过本次发送")
	}

	progressLogger.Printf("本轮符合条件标的数量: %d", len(results))
	sort.Slice(results, func(i, j int) bool { return results[i].StochRSI > results[j].StochRSI })

	return utils.PushTelegram(results, botToken, high_profit_srsi_botToken, chatID)
}

/* ====================== 单币分析 ====================== */

func analyseSymbol(client *futures.Client, c types.Candidate, db_trend *sql.DB) (types.CoinIndicator, bool) {

	symbol := c.Symbol
	var inst string
	if symbol == "BTCUSDT" || symbol == "ETHUSDT" {
		inst = symbol
		//MACDM5, _ := utils.GetTrendResult(db_trend, symbol, "5m")
		MACDM15, _ := utils.GetTrendResult(db_trend, symbol, "15m")
		MACDH1, _ := utils.GetTrendResult(db_trend, symbol, "1h")
		//BuyMACDH4, _ := utils.GetTrendResult(db, symbol, "4h")
		//BuyMACDD1, _ := utils.GetTrendResult(db, symbol, "1d")
		//BuyMACDD3, _ := utils.GetTrendResult(db, symbol, "3d")

		// ===== 模型1 ： Fomo模型  =====
		if (MACDH1 == "BUYMACD" || MACDH1 == "RANGE") && MACDM15 == "BUYMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BEBUY",
				Source:    types.SourceBinance,
				Inst:      inst,
			}, true
		}

		if (MACDH1 == "SELLMACD" || MACDH1 == "RANGE") && MACDM15 == "SELLMACD" {
			status := "Wait"
			return types.CoinIndicator{
				Symbol:    symbol,
				Status:    status,
				Operation: "BESELL",
				Source:    types.SourceBinance,
				Inst:      inst,
			}, true
		}

	} else {
		var MACDH1, MACDM15 string
		var closesH1, closesM15, closesM5 []float64
		if c.Source == types.SourceBinance {
			_, _, closesH1, _ = utils.GetKlinesByAPI(client, symbol, "1h", 200)
		} else if c.Source == types.SourceOKX {
			inst, _ = okxCache.RawSymbol(c.Symbol)
			_, _, closesH1, _ = utils.GetKlinesByAPI_OKX(inst, "1H", 200)
		}

		price := closesH1[len(closesH1)-1]
		DOWNDOWN := utils.DownDown(closesH1, 6, 13, 5)
		MACDDOWN := DOWNDOWN

		if MACDDOWN {
			MACDH1 = "SELLMACD"
		} else {
			return types.CoinIndicator{}, false
		}
		if c.Source == types.SourceBinance {
			_, _, closesM15, _ = utils.GetKlinesByAPI(client, symbol, "15m", 200)
		} else if c.Source == types.SourceOKX {
			inst, _ = okxCache.RawSymbol(c.Symbol)
			_, _, closesM15, _ = utils.GetKlinesByAPI_OKX(inst, "15m", 200)
		}

		DEADOWN := utils.IsDEADOWN(closesM15, 6, 13, 5)
		ema25M15 := utils.CalculateEMA(closesM15, 25)
		if price < ema25M15[len(ema25M15)-1] && DEADOWN {
			MACDM15 = "SELLMACD"
		} else {
			return types.CoinIndicator{}, false
		}
		if c.Source == types.SourceBinance {
			_, _, closesM5, _ = utils.GetKlinesByAPI(client, symbol, "5m", 200)
		} else if c.Source == types.SourceOKX {
			inst, _ = okxCache.RawSymbol(c.Symbol)
			_, _, closesM5, _ = utils.GetKlinesByAPI_OKX(inst, "5m", 200)
		}
		ma60M5 := utils.CalculateMA(closesM5, 60)
		ema25 := utils.CalculateEMA(closesM5, 25)
		if price < ma60M5 && price < ema25[len(ema25)-1] {
			if MACDH1 == "SELLMACD" && MACDM15 == "SELLMACD" {
				status := "Wait"
				return types.CoinIndicator{
					Symbol:    symbol,
					Status:    status,
					Operation: "OTSELL",
					Source:    c.Source,
					Inst:      inst,
				}, true
			}
		} else {
			return types.CoinIndicator{}, false
		}
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
