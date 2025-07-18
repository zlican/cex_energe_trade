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
	apiKey                  = ""
	secretKey               = ""
	proxyURL                = "http://127.0.0.1:10809"
	klinesCount             = 200
	maxWorkers              = 20
	limitVolume             = 100000000                                        // 1亿 USDT
	botToken                = "8040107823:AAHC_qu5cguJf9BG4NDiUB_nwpgF-bPkJAg" //二级印钞
	wait_energe_botToken    = "7381664741:AAEmhhEhsq8nBgThtsOfVklNb6q4TjvI_Og" //播报成功
	energe_waiting_botToken = "7417712542:AAGjCOMeFFFuNCo5vNBWDYJqGs0Qm2ifwmY" //等待区bot
	chatID                  = "6074996357"

	// volumeMap      = map[string]float64{}
	volumeCache *types.VolumeCache
	err         error
	slipCoin    = []string{"XRPUSDT", "1000PEPEUSDT", "ADAUSDT", "BNBUSDT", "AGIXUSDT",
		"LINKUSDT", "FARTCOINUSDT", "1000BONKUSDT", "AVAXUSDT", "LTCUSDT", "ALPACAUSDT",
		"BCHUSDT", "XLMUSDT", "XRPUSDC", "BNXUSDT", "ETHUSDC", "BTCUSDC", "SOLUSDC", "VIDTUSDT",
		"DOTUSDT", "NEARUSDT", "ARBUSDT", "1000SHIBUSDT", "TRXUSDT", "PNUTUSDT", "HYPEUSDT",
		"HBARUSDT", "1INCHUSDT", "SUIUSDC", "1000FLOKIUSDT", "GALAUSDT", "TIAUSDT", "ETHFIUSDT",
		"WLDUSDT", "FILUSDT", "TAOUSDT", "CRVUSDT", "FETUSDT", "INJUSDT", "1000BONKUSDC",
		"SPXUSDT", "TONUSDT", "ETCUSDT", "DOGEUSDT", "SUIUSDT", "PUMPUSDT", "AAVEUSDT", "ENAUSDT",
		"UNIUSDT", "APTUSDT", "TRUMPUSDT", "DOGEUSDC", "VIRTUALUSDT", "SEIUSDT", "WIFUSDT", "OPUSDT",
		"ONDOUSDT", "MOODENGUSDT", "PENGUUSDT"} // 想排除的币放这里
	muVolumeMap    sync.Mutex
	progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)
	db             *sql.DB
	waitChan       = make(chan []types.CoinIndicator, 30) //等待区
	btctrend       types.BTCTrend
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
	btctrend = types.BTCTrend{
		MapTrend: map[string]string{
			"BTCUSDT": utils.GetBTCTrend(db),
		},
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

			ind, ok := analyseSymbol(client, sym, "15m", db, btctrend)
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
	return utils.PushTelegram(results, botToken, chatID, volumeCache, db, btctrend)
}

/* ====================== 单币分析 ====================== */

func analyseSymbol(client *futures.Client, symbol, tf string, db *sql.DB, btctrend types.BTCTrend) (types.CoinIndicator, bool) {

	_, closes, err := utils.GetKlinesByAPI(client, symbol, tf, klinesCount)
	if err != nil || len(closes) < 2 {
		return types.CoinIndicator{}, false
	}

	price := closes[len(closes)-1]
	ema25M15, ema50M15 := utils.Get15MEMAFromDB(db, symbol)
	ema25M1H, ema50M1H := utils.Get1HEMAFromDB(db, symbol)
	priceGT_EMA25 := utils.GetPriceGT_EMA25FromDB(db, symbol) //1H 价格在25EMA上方

	var up, down bool
	if symbol == "BTCUSDT" {
		up = priceGT_EMA25 && ema25M15 > ema50M15    //1H GT +15分钟在上
		down = !priceGT_EMA25 && ema25M15 < ema50M15 //1H !GT + 15分钟在下
	} else {
		up = ema25M1H > ema50M1H && ema25M15 > ema50M15
		down = ema25M1H < ema50M1H && ema25M15 < ema50M15
	}

	var srsi float64
	srsi = utils.Get15SRSIFromDB(db, symbol)

	buyCond := srsi < 25 || srsi < 20
	sellCond := srsi > 75 || srsi > 80

	// ---------- 判定BTC趋势进行动能币过滤 ----------
	var MainTrend string
	if btctrend.MapTrend["BTCUSDT"] == "up" {
		MainTrend = "up"
	} else if btctrend.MapTrend["BTCUSDT"] == "down" {
		MainTrend = "down"
	} else {
		MainTrend = "none"
	}

	var status string
	var SmallEMA25, SmallEMA50 float64
	switch {
	case up && buyCond:
		if MainTrend == "up" {
			if symbol != "BTCUSDT" && symbol != "ETHUSDT" {
				return types.CoinIndicator{}, false
			}
		}

		progressLogger.Printf("BUY 触发: %s %.2f", symbol, price)   // 👈
		SmallEMA25, SmallEMA50 = utils.Get5MEMAFromDB(db, symbol) //对5分钟进行判断
		if SmallEMA25 > SmallEMA50 && price > ema25M15 {
			status = "Soon"
		} else {
			status = "Wait"
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi,
			Status:       status,
			Operation:    "Buy"}, true
	case down && sellCond:
		if MainTrend == "down" {
			if symbol != "BTCUSDT" && symbol != "ETHUSDT" {
				return types.CoinIndicator{}, false
			}
		}

		progressLogger.Printf("SELL 触发: %s %.2f", symbol, price)  // 👈
		SmallEMA25, SmallEMA50 = utils.Get5MEMAFromDB(db, symbol) //对5分钟进行判断
		if SmallEMA25 < SmallEMA50 && price < ema25M15 {
			status = "Soon"
		} else {
			status = "Wait"
		}
		return types.CoinIndicator{
			Symbol:       symbol,
			Price:        price,
			TimeInternal: tf,
			StochRSI:     srsi,
			Status:       status,
			Operation:    "Sell"}, true
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
