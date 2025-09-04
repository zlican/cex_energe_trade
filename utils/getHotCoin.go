package utils

import (
	"context"
	"encoding/json"
	"energe/types"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type MarketSource string // MarketSource 定义交易所来源

// GetHotCoins 获取热门交易对列表并适配为 Bitget 格式
func GetHotCoins() ([]types.Candidate, error) {
	const (
		maxRetries     = 3
		requestTimeout = 7 * time.Second
		baseURL        = "http://127.0.0.1:9000"
	)
	var lastErr error

	// 轻量的 http.Client
	client := &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	// 指数退避重试
	backoff := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		url := fmt.Sprintf("%s/api/hot_trade_volume", baseURL)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = fmt.Errorf("build request error: %v", err)
			fmt.Printf("[HotCoins] attempt %d build request error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "hotcoin-fetcher/1.0")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request error: %v", err)
			fmt.Printf("[HotCoins] attempt %d request error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read body error: %v", err)
			fmt.Printf("[HotCoins] attempt %d read body error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// 检查 HTTP 状态
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("http status %d body: %s", resp.StatusCode, string(body))
			fmt.Printf("[HotCoins] attempt %d http error: %v\n", attempt, lastErr)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return nil, lastErr
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// 解析 JSON，期望返回字符串数组
		var symbols []string
		if err := json.Unmarshal(body, &symbols); err != nil {
			lastErr = fmt.Errorf("json unmarshal error: %v", err)
			fmt.Printf("[HotCoins] attempt %d json unmarshal error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		if len(symbols) == 0 {
			return nil, fmt.Errorf("empty symbol list from hot_trade_volume")
		}

		// 构造 Candidate 数组，适配 Bitget  Ascending
		candidates := make([]types.Candidate, 0, len(symbols))
		for _, sym := range symbols {
			// 规范化符号：假设为 USDT 交易对，添加 USDT 后缀
			normalizedSymbol := strings.ToUpper(sym) + "USDT"
			// Bitget 原始符号：添加 _UMCBL 后缀
			rawSymbol := normalizedSymbol + "_UMCBL"

			candidates = append(candidates, types.Candidate{
				Source:    types.MarketBitget,
				Symbol:    normalizedSymbol,
				RawSymbol: rawSymbol,
				Volume24h: 0.0, // 暂时设为 0，需额外接口获取
			})
		}

		return candidates, nil
	}

	return nil, lastErr
}
