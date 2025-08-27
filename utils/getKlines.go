package utils

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

func GetKlinesByAPI(client *futures.Client, symbol, tf string, klinesCount int) ([]*futures.Kline, []float64, []float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()

	const maxRetries = 3

	var (
		klines []*futures.Kline
		opens  []float64
		closes []float64
		err    error
	)

	// 最多尝试 3 次
	for attempt := 1; attempt <= maxRetries; attempt++ {
		klines, err = client.NewKlinesService().
			Symbol(symbol).Interval(tf).
			Limit(klinesCount).Do(ctx)

		// 拉取成功且数量够用，直接跳出循环
		if err == nil {
			break
		}

		// 记录本次失败
		log.Printf("第 %d 次拉取 %s K 线失败: %v", attempt, symbol, err)

		// 如果还没到最后一次，可以选择短暂等待再试（可按需调整或使用指数退避）
		if attempt < maxRetries {
			time.Sleep(time.Second)
		}
	}

	// 若三次仍失败或数量不足，返回失败标记
	if err != nil || len(klines) < 2 {
		return nil, nil, nil, err
	}

	opens = make([]float64, len(klines))
	for i, k := range klines {
		c, _ := strconv.ParseFloat(k.Open, 64)
		opens[i] = c
	}

	closes = make([]float64, len(klines))
	for i, k := range klines {
		c, _ := strconv.ParseFloat(k.Close, 64)
		closes[i] = c
	}

	return klines, opens, closes, nil
}
