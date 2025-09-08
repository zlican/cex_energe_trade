package utils

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

func Get1MEMA(client *futures.Client, klinesCount int, symbol string) (float64, float64) {
	ctx := context.Background()

	var (
		klines []*futures.Kline
		err    error
	)
	for attempt := 1; attempt <= 3; attempt++ {
		klines, err = client.NewKlinesService().
			Symbol(symbol).Interval("1m").Limit(klinesCount).Do(ctx)
		if err == nil && len(klines) >= 2 {
			break
		}
		log.Printf("第 %d 次拉取 %s 5m K 线失败: %v", attempt, symbol, err)
		if attempt < 3 {
			time.Sleep(time.Second)
		}
	}
	if err != nil || len(klines) < 2 {
		return 0, 0
	}
	var closes []float64
	for _, k := range klines {
		c, _ := strconv.ParseFloat(k.Close, 64)
		closes = append(closes, c)
	}

	ema25, _ := CalculateEMA(closes, 25)
	ema50, _ := CalculateEMA(closes, 50)

	return ema25[len(ema25)-1], ema50[len(ema50)-1]
}
