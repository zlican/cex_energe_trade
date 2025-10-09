package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// ===================== 数据结构 =====================
type Ticker24 struct {
	Symbol    string `json:"symbol"`
	LastPrice string `json:"lastPrice"`
	QuoteVol  string `json:"quoteVolume"`
}

type Kline [][]interface{}

type Gainer struct {
	Symbol    string
	ChangePct float64
}

// ===================== 主函数 =====================
// GetDailyGainers 返回自 UTC+0 当天 00:00 起涨幅最高的 USDT 永续合约列表
// 参数 limit: 输出前 N 名，默认建议 20
func GetDailyGainers(limit int) []string {
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	startTimeMs := startOfDay.UnixMilli()

	// 1. 获取全部 ticker/24hr（一次返回所有 symbol）
	tickers, err := fetchAllTickers()
	if err != nil {
		fmt.Println("fetch tickers failed:", err)
		return nil
	}

	// 2. 筛选出 USDT 永续合约（Binance Futures 的符号通常以 USDT 结尾）
	var usdtTickers []Ticker24
	for _, t := range tickers {
		if len(t.Symbol) > 4 && t.Symbol[len(t.Symbol)-4:] == "USDT" {
			usdtTickers = append(usdtTickers, t)
		}
	}

	// 3. 计算每个 symbol 的当天涨幅
	results := make([]Gainer, 0, len(usdtTickers))
	for _, t := range usdtTickers {
		// 转为 float 再比较交易量
		vol, err := strconv.ParseFloat(t.QuoteVol, 64)
		if err != nil || vol < 30000000 {
			continue
		}

		open, err := getOpenPriceUTC(t.Symbol, startTimeMs)
		if err != nil || open == 0 {
			continue
		}
		last, _ := strconv.ParseFloat(t.LastPrice, 64)
		changePct := (last - open) / open * 100
		results = append(results, Gainer{
			Symbol:    t.Symbol,
			ChangePct: changePct,
		})
	}

	// 4. 按涨幅降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].ChangePct > results[j].ChangePct
	})

	// 5. 取前 limit 个 symbol
	if limit > len(results) {
		limit = len(results)
	}
	top := results[:limit]

	var symbols []string
	for _, g := range top {
		symbols = append(symbols, g.Symbol)
	}

	return symbols
}

// ===================== 辅助函数 =====================
func fetchAllTickers() ([]Ticker24, error) {
	resp, err := http.Get("https://fapi.binance.com/fapi/v1/ticker/24hr")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var arr []Ticker24
	if err := json.Unmarshal(body, &arr); err != nil {
		return nil, err
	}
	return arr, nil
}

func getOpenPriceUTC(symbol string, startTimeMs int64) (float64, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=1d&startTime=%d&limit=1", symbol, startTimeMs)
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var klines Kline
	if err := json.Unmarshal(body, &klines); err != nil {
		return 0, err
	}
	if len(klines) == 0 {
		return 0, fmt.Errorf("no kline data")
	}

	// open price 是 klines[0][1]
	openStr, ok := klines[0][1].(string)
	if !ok {
		return 0, fmt.Errorf("invalid open type")
	}
	return strconv.ParseFloat(openStr, 64)
}
