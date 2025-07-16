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

		ema25 := CalculateEMA(closes, 25)
		ema50 := CalculateEMA(closes, 50)
		if len(ema25) == 0 {
			continue
		}
		lastEMA25 := ema25[len(ema25)-1]
		lastEMA50 := ema50[len(ema50)-1]
		lastTime := klines[len(klines)-1].CloseTime
		_, kLine, _ := StochRSIFromClose(closes, 14, 14, 3, 3)
		lastKLine := kLine[len(kLine)-1]

		// 写入数据库（UPSERT）
		_, err = model.DB.Exec(`
		INSERT INTO symbol_ema_5min (symbol, timestamp, ema25, ema50, srsi)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		timestamp = VALUES(timestamp),
		ema25 = VALUES(ema25),
		ema50 = VALUES(ema50),
		srsi = VALUES(srsi)
	`, symbol, lastTime, lastEMA25, lastEMA50, lastKLine)
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
