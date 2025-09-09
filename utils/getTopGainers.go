package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Ticker24h struct {
	Symbol             string `json:"symbol"`
	PriceChangePercent string `json:"priceChangePercent"`
	LastPrice          string `json:"lastPrice"`
	Volume             string `json:"quoteVolume"`
}

// StartTopGainersFetcher 每 15 分钟抓取一次 USDT 合约涨幅榜，返回前 15 名的 symbol 列表
func StartTopGainersFetcher(ch chan<- []string, chTicker24h chan<- []Ticker24h) {
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()

		for {
			symbols, ticker24h := GetTopGainers()

			// 防止阻塞，分别推送
			select {
			case ch <- symbols:
			default:
				fmt.Println("Warning: ch blocked, skipping symbols update")
			}

			select {
			case chTicker24h <- ticker24h:
			default:
				fmt.Println("Warning: chTicker24h blocked, skipping ticker24h update")
			}

			<-ticker.C
		}
	}()
}

// fetchTopGainers 抓取一次数据并返回前 15 个 USDT 合约
func GetTopGainers() ([]string, []Ticker24h) {
	resp, err := http.Get("https://fapi.binance.com/fapi/v1/ticker/24hr")
	if err != nil {
		log.Printf("http get failed: %v", err)
		return nil, nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var tickers []Ticker24h
	if err := json.Unmarshal(body, &tickers); err != nil {
		log.Printf("json unmarshal: %v", err)
		return nil, nil
	}

	// 过滤 USDT 结尾
	var usdtTickers []Ticker24h
	for _, t := range tickers {
		if strings.HasSuffix(t.Symbol, "USDT") {
			usdtTickers = append(usdtTickers, t)
		}
	}

	// 排序
	sort.Slice(usdtTickers, func(i, j int) bool {
		a, _ := strconv.ParseFloat(usdtTickers[i].PriceChangePercent, 64)
		b, _ := strconv.ParseFloat(usdtTickers[j].PriceChangePercent, 64)
		return a > b
	})

	// 取前 15 个
	N := 15
	if len(usdtTickers) < N {
		N = len(usdtTickers)
	}
	result := make([]string, 0, N)
	for i := 0; i < N; i++ {
		result = append(result, usdtTickers[i].Symbol)
	}
	return result, tickers
}
