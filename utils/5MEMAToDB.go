package utils

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"

	"energe/model"
	"energe/types"

	"github.com/adshao/go-binance/v2/futures"
)

func Update5MEMAToDB(client *futures.Client, db *sql.DB, limitVolume float64, klinesCount int, volumeCache *types.VolumeCache, slipCoin []string) {
	ctx := context.Background()
	time.Sleep(5 * time.Second)

	// 从 VolumeCache 拿热门币种
	symbols := volumeCache.SymbolsAbove(limitVolume)
	for _, symbol := range symbols {
		if IsSlipCoin(symbol, slipCoin) {
			continue
		}
		var (
			klines []*futures.Kline
			err    error
		)
		for attempt := 1; attempt <= 3; attempt++ {
			klines, err = client.NewKlinesService().
				Symbol(symbol).Interval("5m").Limit(klinesCount).Do(ctx)
			if err == nil && len(klines) >= 2 {
				break
			}
			log.Printf("第 %d 次拉取 %s 5m K 线失败: %v", attempt, symbol, err)
			if attempt < 3 {
				time.Sleep(time.Second)
			}
		}
		if err != nil || len(klines) < 2 {
			continue
		}
		var closes []float64
		for _, k := range klines {
			c, _ := strconv.ParseFloat(k.Close, 64)
			closes = append(closes, c)
		}

		ema25, lastEMA25 := CalculateEMA(closes, 25)
		_, lastEMA50 := CalculateEMA(closes, 50)
		ma60 := CalculateMA(closes, 60)
		if len(ema25) == 0 {
			continue
		}
		lastTime := klines[len(klines)-1].CloseTime
		_, kLine, _ := StochRSIFromClose(closes, 14, 14, 3, 3)
		lastKLine := kLine[len(kLine)-1]
		//macd的存入
		UpMACD := false
		DownMACD := false
		XUpMACD := IsGolden(closes, 6, 13, 5)
		XDownMACD := IsDead(closes, 6, 13, 5)
		// 写入数据库（UPSERT）
		_, err = model.DB.Exec(`
		INSERT INTO symbol_ema_5min (symbol, timestamp, ema25, ema50, ma60, srsi, upmacd, downmacd, xupmacd, xdownmacd)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		timestamp = VALUES(timestamp),
		ema25 = VALUES(ema25),
		ema50 = VALUES(ema50),
		ma60 = VALUES(ma60),
		srsi = VALUES(srsi),
		upmacd = VALUES(upmacd),
		downmacd = VALUES(downmacd),
		xupmacd := VALUES(xupmacd),
		xdownmacd := VALUES(xdownmacd)
	`, symbol, lastTime, lastEMA25, lastEMA50, ma60, lastKLine, UpMACD, DownMACD, XUpMACD, XDownMACD)
		if err != nil {
			log.Printf("写入 EMA25 出错 %s: %v", symbol, err)
		}
	}
}

func Get5MEMAFromDB(db *sql.DB, symbol string) (ema25, ema50 float64) {
	err := db.QueryRow("SELECT ema25, ema50 FROM symbol_ema_5min WHERE symbol = ?", symbol).Scan(&ema25, &ema50)
	if err != nil {
		log.Printf("查询 5MEMA 失败 %s: %v", symbol, err)
		return 0, 0
	}
	return ema25, ema50
}

func Get5SRSIFromDB(db *sql.DB, symbol string) (srsi float64) {
	err := db.QueryRow("SELECT srsi FROM symbol_ema_5min WHERE symbol = ?", symbol).Scan(&srsi)
	if err != nil {
		log.Printf("查询 SRSIFromDB 失败 %s: %v", symbol, err)
		return 0
	}
	return srsi
}

func GetMACDM5FromDB(db *sql.DB, symbol string) (upmacd, downmacd, xupmacd, xdownmacd bool) {
	err := db.QueryRow("SELECT upmacd, downmacd, xupmacd, xdownmacd FROM symbol_ema_5min WHERE symbol = ?", symbol).Scan(&upmacd, &downmacd, &xupmacd, &xdownmacd)
	if err != nil {
		log.Printf("查询 5MMACDFromDB 失败 %s: %v", symbol, err)
		return false, false, false, false
	}
	return upmacd, downmacd, xupmacd, xdownmacd
}

func GetMA60FromDB(db *sql.DB, symbol string) (ma60 float64) {
	err := db.QueryRow("SELECT ma60 FROM symbol_ema_5min WHERE symbol = ?", symbol).Scan(&ma60)
	if err != nil {
		log.Printf("查询 MA60 失败 %s: %v", symbol, err)
		return 0
	}
	return ma60
}
