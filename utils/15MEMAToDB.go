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

func Update15MEMAToDB(client *futures.Client, db *sql.DB, limitVolume float64, klinesCount int, volumeCache *types.VolumeCache, slipCoin []string) {
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
				Symbol(symbol).Interval("15m").Limit(klinesCount).Do(ctx)
			if err == nil && len(klines) >= 2 {
				break
			}
			log.Printf("第 %d 次拉取 %s 15m K 线失败: %v", attempt, symbol, err)
			if attempt < 3 {
				time.Sleep(time.Second)
			}
		}
		if err != nil || len(klines) < 50 {
			continue
		}

		var closes []float64
		for _, k := range klines {
			c, _ := strconv.ParseFloat(k.Close, 64)
			closes = append(closes, c)
		}

		price := closes[len(closes)-1]
		ema25 := CalculateEMA(closes, 25)
		ema50 := CalculateEMA(closes, 50)
		ema169 := CalculateEMA(closes, 169)
		if len(ema25) == 0 {
			continue
		}
		lastEMA25 := ema25[len(ema25)-1]
		lastEMA50 := ema50[len(ema50)-1]
		lastEMA169 := ema169[len(ema169)-1]
		lastTime := klines[len(klines)-1].CloseTime
		_, kLine, _ := StochRSIFromClose(closes, 14, 14, 3, 3)
		lastKLine := kLine[len(kLine)-1]
		UpMACD := false
		DownMACD := false
		GT_EMA25 := price > lastEMA25

		// 写入数据库（UPSERT）
		_, err = model.DB.Exec(`
		INSERT INTO symbol_ema_15min (symbol, timestamp, ema25, ema50, ema169, srsi, upmacd, downmacd, gtema25)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		timestamp = VALUES(timestamp),
		ema25 = VALUES(ema25),
		ema50 = VALUES(ema50),
		ema169 = VALUES(ema169),
		srsi = VALUES(srsi),
		upmacd = VALUES(upmacd),
		downmacd = VALUES(downmacd),
		gtema25 = VALUES(gtema25)
	`, symbol, lastTime, lastEMA25, lastEMA50, lastEMA169, lastKLine, UpMACD, DownMACD, GT_EMA25)
		if err != nil {
			log.Printf("写入 EMA25 出错 %s: %v", symbol, err)
		}
	}
}

func Get15MEMAFromDB(db *sql.DB, symbol string) (ema25, ema50, ema169 float64) {
	err := db.QueryRow("SELECT ema25, ema50, ema169 FROM symbol_ema_15min WHERE symbol = ?", symbol).Scan(&ema25, &ema50, &ema169)
	if err != nil {
		log.Printf("查询 15MEMA 失败 %s: %v", symbol, err)
		return 0, 0, 0
	}
	return ema25, ema50, ema169
}

func Get15SRSIFromDB(db *sql.DB, symbol string) (srsi float64) {
	err := db.QueryRow("SELECT srsi FROM symbol_ema_15min WHERE symbol = ?", symbol).Scan(&srsi)
	if err != nil {
		log.Printf("查询 SRSIFromDB 失败 %s: %v", symbol, err)
		return 0
	}
	return srsi
}

func GetMACDM15FromDB(db *sql.DB, symbol string) (upmacd, downmacd bool) {
	err := db.QueryRow("SELECT upmacd, downmacd FROM symbol_ema_15min WHERE symbol = ?", symbol).Scan(&upmacd, &downmacd)
	if err != nil {
		log.Printf("查询 15MMACDFromDB 失败 %s: %v", symbol, err)
		return false, false
	}
	return upmacd, downmacd
}

func GetGT_EMA25FromDB(db *sql.DB, symbol string) (gtema25 bool) {
	err := db.QueryRow("SELECT gtema25 FROM symbol_ema_15min WHERE symbol = ?", symbol).Scan(&gtema25)
	if err != nil {
		log.Printf("查询 GT_EMA25FromDB 失败 %s: %v", symbol, err)
		return false
	}
	return gtema25
}
