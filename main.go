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
	"sort"
	"strconv"
	"sync"
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
	db             *sql.DB
	waitChan       = make(chan []types.CoinIndicator, 30) //等待区
	betrend        types.BETrend
)

/* ====================== 主函数 ====================== */

func main() {
	progressLogger.Println("程序启动...")

	// 注册路由
	http.HandleFunc("/api/latest-tg-messages", latestMessagesHandler)

	// 启动HTTP服务（非阻塞）
	go func() {
		if err := http.ListenAndServe(":8888", nil); err != nil {
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

	model.InitDB()
	db = model.DB

	// 立即执行一次
	utils.Update1hEMA25ToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
	utils.Update15MEMAToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
	utils.Update5MEMAToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
	// runScan 立即执行一次，并在 minute%15==0 的时间对齐后每15分钟执行一次
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
			utils.Update15MEMAToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
			if err := runScan(client); err != nil {
				progressLogger.Printf("对齐 runScan 出错: %v", err)
			}

			ticker := time.NewTicker(15 * time.Minute)
			for t := range ticker.C {
				progressLogger.Printf("[runScan] 每15分钟触发: %s", t.Format("15:04:05"))
				go func() {
					utils.Update15MEMAToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
					if err := runScan(client); err != nil {
						progressLogger.Printf("周期 runScan 出错: %v", err)
					}
				}()
			}
		})
	}()
	//开启等待区
	go utils.WaitEnerge(waitChan, db, wait_energe_botToken, chatID, client, klinesCount, energe_waiting_botToken)
	last1h := time.Time{}
	last5m := time.Time{}

	for {
		now := time.Now()
		time.Sleep(time.Until(now.Truncate(time.Second).Add(1 * time.Second)))

		minute := now.Minute()
		second := now.Second()

		if minute == 0 && now.Sub(last1h) >= time.Hour {
			last1h = now
			progressLogger.Printf("整点 %02d:00，执行 Update1hEMA25ToDB", now.Hour())
			go utils.Update1hEMA25ToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
		}

		if minute%5 == 0 && second == 0 && now.Sub(last5m) >= 5*time.Minute {
			last5m = now
			progressLogger.Printf("每5分钟触发，执行 Update5MEMAToDB")
			go utils.Update5MEMAToDB(client, db, float64(limitVolume), klinesCount, volumeCache, slipCoin)
		}
	}
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

	// ---------- 2. 获取趋势 ----------
	betrend = types.BETrend{
		BTC: utils.GetBTCTrend(db),
		ETH: utils.GetETHTrend(db),
	}

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

			ind, ok := analyseSymbol(client, sym, "15m", db)
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
	return utils.PushTelegram(results, botToken, high_profit_srsi_botToken, chatID, volumeCache, db, betrend)
}

/* ====================== 单币分析 ====================== */

func analyseSymbol(client *futures.Client, symbol, tf string, db *sql.DB) (types.CoinIndicator, bool) {

	_, _, closes, err := utils.GetKlinesByAPI(client, symbol, tf, klinesCount)
	if err != nil || len(closes) < 2 {
		return types.CoinIndicator{}, false
	}

	price := closes[len(closes)-1]
	ema25M15, ema50M15, _ := utils.Get15MEMAFromDB(db, symbol)
	ema25H1, ema50H1 := utils.Get1HEMAFromDB(db, symbol)
	ema25M5, ema50M5 := utils.Get5MEMAFromDB(db, symbol)
	priceGT_EMA25 := utils.GetPriceGT_EMA25FromDB(db, symbol) //1H 价格在25EMA上方

	//动能模型
	var up, down bool
	up = priceGT_EMA25 && ema25H1 > ema50H1 && ema25M15 > ema50M15    //1H UpTrend +15分钟金叉
	down = !priceGT_EMA25 && ema25H1 < ema50H1 && ema25M15 < ema50M15 //1H DownTrend + 15分钟死叉

	var srsi15M float64
	srsi15M = utils.Get15SRSIFromDB(db, symbol)

	buyCond := srsi15M < 35
	sellCond := srsi15M > 65

	//MACD模型
	UpMACDM15 := utils.IsAboutToGoldenCross(closes, 6, 13, 5)
	DownMACDM15 := utils.IsAboutToDeadCross(closes, 6, 13, 5)

	isBTCOrETH := symbol == "BTCUSDT" || symbol == "ETHUSDT"

	// BE 专属
	isBE := isBTCOrETH
	BEUp := price > ema25H1 && ema25H1 > ema50H1
	BEDown := price < ema25H1 && ema25H1 < ema50H1

	// ===== 模型1优先级最高 =====
	if up && buyCond {
		if !isBTCOrETH {
			return types.CoinIndicator{}, false
		}
		progressLogger.Printf("BUY 触发: %s %.2f", symbol, price)

		status := "Wait"
		if ema25M5 > ema50M5 && UpMACDM15 {
			status = "View"
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       status,
			Operation:    "Buy",
		}, true
	}

	if down && sellCond {
		if !isBTCOrETH {
			return types.CoinIndicator{}, false
		}
		progressLogger.Printf("SELL 触发: %s %.2f", symbol, price)

		status := "Wait"
		if ema25M5 < ema50M5 && DownMACDM15 {
			status = "View"
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       status,
			Operation:    "Sell",
		}, true
	}

	// ===== 模型2（仅模型1未触发时才执行） =====
	if isBE && BEUp && ema25M15 > ema50M15 && ema25M5 > ema50M5 && UpMACDM15 {
		progressLogger.Printf("Fomo UP 触发: %s %.2f", symbol, price)
		_, _, closesM5, err := utils.GetKlinesByAPI(client, symbol, "5m", klinesCount)
		if err != nil || len(closesM5) < 2 {
			return types.CoinIndicator{}, false
		}
		if utils.IsAboutToGoldenCross(closesM5, 6, 13, 5) {
			return types.CoinIndicator{
				Symbol:       symbol,
				Price:        price,
				TimeInternal: tf,
				StochRSI:     srsi15M,
				Status:       "Fomo",
				Operation:    "Fomo",
			}, true
		}
	}

	if isBE && BEDown && ema25M15 < ema50M15 && ema25M5 < ema50M5 && DownMACDM15 {
		progressLogger.Printf("Fomo DOWN 触发: %s %.2f", symbol, price)
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       "Fomo",
			Operation:    "Fomo",
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
	// 参数limit，默认3
	limit := 3
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	msgs := telegram.GetLatestMessages(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}
