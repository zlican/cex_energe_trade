package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ===================== 数据结构 =====================
type Ticker24h struct {
	Symbol    string `json:"symbol"`
	LastPrice string `json:"lastPrice"`
	Volume    string `json:"quoteVolume"`
}

type Kline [][]interface{}

type Gainer struct {
	Symbol    string
	Open      float64
	Last      float64
	QuoteVol  float64
	ChangePct float64
}

var tickers24h []Ticker24h

func StartTopGainersUTCFetcher(ch chan<- []string, chTicker24h chan<- []Ticker24h) {
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()

		for {
			symbols, ticker24h := GetDailyGainers(10)

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

// ===================== HTTP client helper =====================
func newHTTPClient() *http.Client {
	proxyURL, _ := url.Parse("http://127.0.0.1:10809") // ← 你的代理地址
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			MaxIdleConns:    100,
			IdleConnTimeout: 90 * time.Second,
		},
	}
}

// ===================== 主并发函数 =====================
// GetDailyGainersConcurrent 并发获取涨幅榜。
// - limit: 返回前 N（如果小于可得数量则返回全部）
// - concurrency: 同时并发请求 klines 的最大协程数（建议 8~32，根据你机器和网络）
func GetDailyGainersConcurrent(limit int, concurrency int) ([]Gainer, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be > 0")
	}
	if concurrency <= 0 {
		concurrency = 8
	}

	// 计算 UTC 当天 00:00 的毫秒时间戳
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	startTimeMs := startOfDay.UnixMilli()

	client := newHTTPClient()

	// 1) 拉取所有 futures tickers
	tickers, err := fetchAllTickersWithClient(client)
	tickers24h = tickers
	if err != nil {
		return nil, fmt.Errorf("fetch tickers failed: %w", err)
	}

	// 2) 过滤出 USDT 合约并按成交额阈值过滤（保持你原有逻辑）
	minQuoteVol := 30000000.0
	candidates := make([]Ticker24h, 0, len(tickers))
	for _, t := range tickers {
		if len(t.Symbol) <= 4 {
			continue
		}
		if !strings.HasSuffix(t.Symbol, "USDT") {
			continue
		}
		vol, err := strconv.ParseFloat(t.Volume, 64)
		if err != nil {
			continue
		}
		if vol < minQuoteVol {
			continue
		}
		candidates = append(candidates, t)
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidates after filtering by quote volume")
	}

	// 3) 并发拉取开盘价并计算涨幅
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	resultsCh := make(chan Gainer, len(candidates))

	for _, t := range candidates {
		sem <- struct{}{}
		wg.Add(1)

		go func(t Ticker24h) {
			defer wg.Done()
			defer func() { <-sem }()

			open, err := getOpenPriceUTCWithClient(client, t.Symbol, startTimeMs)
			if err != nil || open == 0 {
				// 打印调试日志但不中断其他任务
				// fmt.Printf("skip %s: open err: %v\n", t.Symbol, err)
				return
			}

			last, err := strconv.ParseFloat(t.LastPrice, 64)
			if err != nil {
				return
			}
			vol, _ := strconv.ParseFloat(t.Volume, 64)
			change := (last - open) / open * 100.0

			resultsCh <- Gainer{
				Symbol:    t.Symbol,
				Open:      open,
				Last:      last,
				QuoteVol:  vol,
				ChangePct: change,
			}
		}(t)
	}

	wg.Wait()
	close(resultsCh)

	// 收集结果并排序
	results := make([]Gainer, 0)
	for g := range resultsCh {
		results = append(results, g)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no valid klines fetched")
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ChangePct > results[j].ChangePct
	})

	if limit > len(results) {
		limit = len(results)
	}
	return results[:limit], nil
}

// 封装向后兼容的旧接口：保留返回字符串 slice 的签名
func GetDailyGainers(limit int) ([]string, []Ticker24h) {
	gainers, err := GetDailyGainersConcurrent(limit, 16) // 默认并发 16
	if err != nil {
		fmt.Println("GetDailyGainers error:", err)
		return nil, nil
	}
	out := make([]string, 0, len(gainers))
	for _, g := range gainers {
		out = append(out, g.Symbol)
	}
	return out, tickers24h
}

// ===================== 辅助函数（client 版本） =====================
func fetchAllTickersWithClient(client *http.Client) ([]Ticker24h, error) {
	resp, err := client.Get("https://fapi.binance.com/fapi/v1/ticker/24hr")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ticker status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var arr []Ticker24h
	if err := json.Unmarshal(body, &arr); err != nil {
		return nil, err
	}
	return arr, nil
}

func getOpenPriceUTCWithClient(client *http.Client, symbol string, startTimeMs int64) (float64, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=1d&startTime=%d&limit=1", symbol, startTimeMs)

	var lastErr error
	// 简单重试机制
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 150 * time.Millisecond)
			continue
		}

		// 读取并关闭 body
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 150 * time.Millisecond)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
			// 若受到 429 可适当等待
			if resp.StatusCode == 429 {
				time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
				continue
			}
			// 对于其他错误直接返回
			return 0, lastErr
		}

		var klines Kline
		if err := json.Unmarshal(body, &klines); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * 150 * time.Millisecond)
			continue
		}
		if len(klines) == 0 {
			return 0, fmt.Errorf("no kline data")
		}

		// open 可能是 string 或 number，兼容两种
		openVal := klines[0][1]
		switch v := openVal.(type) {
		case string:
			if v == "" {
				return 0, fmt.Errorf("empty open string")
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, err
			}
			return f, nil
		case float64:
			return v, nil
		default:
			return 0, fmt.Errorf("unexpected open type %T", v)
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("unknown error getting kline for %s", symbol)
	}
	return 0, lastErr
}
