package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"energe/model"
	"energe/types"
	"energe/utils"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
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
	wait_energe_botToken      = "7381664741:AAEmhhEhsq8nBgThtsOfVklNb6q4TjvI_Og" //播报成功
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

			ind, ok := analyseSymbol(client, sym, "15m", db, betrend)
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

func analyseSymbol(client *futures.Client, symbol, tf string, db *sql.DB, bestrend types.BETrend) (types.CoinIndicator, bool) {

	_, opens, closes, err := utils.GetKlinesByAPI(client, symbol, tf, klinesCount)
	if err != nil || len(opens) < 2 || len(closes) < 2 {
		return types.CoinIndicator{}, false
	}

	price := closes[len(closes)-1]
	ema25M15, ema50M15, _ := utils.Get15MEMAFromDB(db, symbol)
	ema25M1H, _ := utils.Get1HEMAFromDB(db, symbol)
	ema25M5, ema50M5 := utils.Get5MEMAFromDB(db, symbol)
	priceGT_EMA25 := utils.GetPriceGT_EMA25FromDB(db, symbol) //1H 价格在25EMA上方

	//动能模型
	//当在1小时下，且超买 15分钟在下，判定为空
	//当在1小时上，且超卖 15分钟在上，判定为多
	var up, down bool
	up = priceGT_EMA25 && ema25M15 > ema50M15    //1H GT +15分钟金叉
	down = !priceGT_EMA25 && ema25M15 < ema50M15 //1H !GT + 15分钟死叉

	var srsi15M float64
	srsi15M = utils.Get15SRSIFromDB(db, symbol)

	buyCond := srsi15M < 35
	sellCond := srsi15M > 65

	//MACD模型
	UpMACD := utils.IsAboutToGoldenCross(closes, 6, 13, 5)
	DownMACD := utils.IsAboutToDeadCross(closes, 6, 13, 5)

	isBTCOrETH := symbol == "BTCUSDT" || symbol == "ETHUSDT"

	//BE专属
	var isBE, BEBelowEMA25, BEAboveEMA25 bool
	if symbol == "BTCUSDT" || symbol == "ETHUSDT" {
		isBE = true
		BEBelowEMA25 = price < ema25M1H //1小时之下
		BEAboveEMA25 = price > ema25M1H //1小时之上
	}
	//1.猛烈下跌
	BEDOWN := isBE && BEBelowEMA25 && ema25M15 < ema50M15 && ema25M5 < ema50M5
	//2.猛烈上涨
	BEUP := isBE && BEAboveEMA25 && ema25M15 > ema50M15 && ema25M5 > ema50M5

	var status string
	switch {
	case up && buyCond:
		if !isBTCOrETH {
			// 只做多 BTC、ETH其他跳过
			return types.CoinIndicator{}, false
		}
		progressLogger.Printf("BUY 触发: %s %.2f", symbol, price) // 👈

		if ema25M5 > ema50M5 && UpMACD {
			//5分钟金叉，MACD趋向
			status = "View"
		} else {
			status = "Wait"
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       status,
			Operation:    "Buy"}, true
	case down && sellCond:
		if !isBTCOrETH {
			// 只做空 BTC、ETH其他跳过
			return types.CoinIndicator{}, false
		}
		progressLogger.Printf("SELL 触发: %s %.2f", symbol, price) // 👈

		if ema25M5 < ema50M5 && DownMACD {
			//5分钟死叉，MACD
			status = "View"
		} else {
			status = "Wait"
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       status,
			Operation:    "Sell"}, true
	case BEUP:
		progressLogger.Printf("View 触发: %s %.2f", symbol, price) // 👈
		if UpMACD {
			status = "ViewBE"
		} else {
			return types.CoinIndicator{}, false
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       status,
			Operation:    "ViewBE"}, true
	case BEDOWN:
		progressLogger.Printf("View 触发: %s %.2f", symbol, price) // 👈
		if DownMACD {
			status = "ViewBE"
		} else {
			return types.CoinIndicator{}, false
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi15M,
			Status:       status,
			Operation:    "ViewBE"}, true

	default:
		return types.CoinIndicator{}, false
	}
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
