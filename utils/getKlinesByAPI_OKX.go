package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// OKXKline 单根K线结构体
type OKXKline struct {
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Vol       float64
	VolCcy    float64
	VolQuote  float64
	Confirm   int
}

// GetKlinesByAPI_OKX 拉取 OKX K线数据
// - symbol: "BTC-USDT-SWAP"
// - tf:     "1H", "4H", "1D" 等
// - limit:  拉取条数 (最多 200)
// 返回: K线数组, opens, closes
func GetKlinesByAPI_OKX(symbol, tf string, limit int) ([]*OKXKline, []float64, []float64, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
		defer cancel()

		url := fmt.Sprintf("https://www.okx.com/api/v5/market/candles?instId=%s&bar=%s&limit=%d", symbol, tf, limit)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			progressLogger.Printf("[OKX][KLINES] 第 %d 次构建请求失败: %v\n", attempt, err)
			if attempt < maxRetries {
				time.Sleep(time.Second)
			}
			continue
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			progressLogger.Printf("[OKX][KLINES] 第 %d 次请求失败: %v\n", attempt, err)
			if attempt < maxRetries {
				time.Sleep(time.Second)
			}
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			progressLogger.Printf("[OKX][KLINES] 第 %d 次读响应失败: %v\n", attempt, err)
			if attempt < maxRetries {
				time.Sleep(time.Second)
			}
			continue
		}

		var raw struct {
			Code string     `json:"code"`
			Msg  string     `json:"msg"`
			Data [][]string `json:"data"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			lastErr = err
			progressLogger.Printf("[OKX][KLINES] 第 %d 次 JSON 解析失败: %v\n", attempt, err)
			if attempt < maxRetries {
				time.Sleep(time.Second)
			}
			continue
		}
		if raw.Code != "0" {
			lastErr = fmt.Errorf("OKX API error: %s %s", raw.Code, raw.Msg)
			if raw.Code == "51001" && strings.Contains(raw.Msg, "Instrument ID or Spread ID doesn't exist") {
				return nil, nil, nil, lastErr
			}
			progressLogger.Printf("[OKX][KLINES] 第 %d 次返回错误: %s %s\n", attempt, raw.Code, raw.Msg)
			if attempt < maxRetries {
				time.Sleep(time.Second)
			}
			continue
		}

		klines := make([]*OKXKline, 0, len(raw.Data))
		opens := make([]float64, 0, len(raw.Data))
		closes := make([]float64, 0, len(raw.Data))

		// 注意：OKX 返回数据是 "倒序" (最新的在前)，需要反转或按需处理
		for i := len(raw.Data) - 1; i >= 0; i-- {
			row := raw.Data[i]
			if len(row) < 9 {
				continue
			}

			ts, _ := strconv.ParseInt(row[0], 10, 64)
			o, _ := strconv.ParseFloat(row[1], 64)
			h, _ := strconv.ParseFloat(row[2], 64)
			l, _ := strconv.ParseFloat(row[3], 64)
			c, _ := strconv.ParseFloat(row[4], 64)
			vol, _ := strconv.ParseFloat(row[5], 64)
			volCcy, _ := strconv.ParseFloat(row[6], 64)
			volQuote, _ := strconv.ParseFloat(row[7], 64)
			confirm, _ := strconv.Atoi(row[8])

			klines = append(klines, &OKXKline{
				Timestamp: ts,
				Open:      o,
				High:      h,
				Low:       l,
				Close:     c,
				Vol:       vol,
				VolCcy:    volCcy,
				VolQuote:  volQuote,
				Confirm:   confirm,
			})
			opens = append(opens, o)
			closes = append(closes, c)
		}

		return klines, opens, closes, nil
	}

	return nil, nil, nil, lastErr
}

func SymbolToInst(symbol string) (string, error) {
	symbol = strings.ToUpper(symbol)

	if strings.HasSuffix(symbol, "USDT") {
		base := strings.TrimSuffix(symbol, "USDT")
		return fmt.Sprintf("%s-USDT-SWAP", base), nil
	}

	if strings.HasSuffix(symbol, "USDC") {
		base := strings.TrimSuffix(symbol, "USDC")
		return fmt.Sprintf("%s-USDC-SWAP", base), nil
	}

	// 其他币对，可以按需扩展
	return "", fmt.Errorf("OKX不支持的 symbol 格式: %s", symbol)
}
