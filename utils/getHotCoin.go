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
func GetHotCoins(ticker24h []Ticker24h, slipCoin, banSymbols, newSymbols, topGainers, CGTopGainers []string) ([]types.Candidate, error) {
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

		// 构造 slipCoin 的币种集合（去掉 USDT/USDC 后缀）
		slipSet := make(map[string]struct{})
		for _, sc := range slipCoin {
			// 去掉 USDT 或 USDC 后缀，提取纯币种名称
			coin := strings.TrimSuffix(strings.TrimSuffix(sc, "USDT"), "USDC")
			slipSet[strings.ToUpper(coin)] = struct{}{}
		}

		// 过滤 symbols，排除 slipCoin 中的币种
		filteredSymbols := make([]string, 0, len(symbols))
		for _, sym := range symbols {
			// 规范化符号为大写
			normalizedSym := strings.ToUpper(sym)
			// 检查是否在 slipSet 中
			if _, exists := slipSet[normalizedSym]; !exists {
				filteredSymbols = append(filteredSymbols, normalizedSym)
			}
		}

		if len(filteredSymbols) == 0 {
			return nil, fmt.Errorf("all symbols filtered out by slipCoin")
		}

		// 构造 Candidate 数组，适配 Bitget Ascending
		candidates := make([]types.Candidate, 0, len(filteredSymbols)+30)
		symbolSet := make(map[string]struct{}) // To track symbols and avoid duplicates
		for _, sym := range filteredSymbols {
			// 移除符号中的 "OFFICIAL"（不区分大小写）
			normalizedSymbol := strings.ReplaceAll(strings.ToUpper(sym), "OFFICIAL", "") + "USDT"
			// Bitget 原始符号：添加 _UMCBL 后缀
			rawSymbol := normalizedSymbol + "_UMCBL"

			//移除BAN标的
			banNow := false
			for _, ban := range banSymbols {
				if ban == normalizedSymbol {
					banNow = true
				}
			}
			if banNow {
				continue
			}

			if !CheckVolumeCMCC(ticker24h, sym) {
				continue
			}

			candidates = append(candidates, types.Candidate{
				Symbol:    normalizedSymbol,
				RawSymbol: rawSymbol,
				Volume24h: 0.0, // 暂时设为 0，需额外接口获取
			})
			symbolSet[normalizedSymbol] = struct{}{} // Track added symbols
		}

		// 确保 八大主流存在
		var BE = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "HYPEUSDT", "PAXGUSDT"}
		for _, mustHave := range BE {
			if _, exists := symbolSet[mustHave]; !exists {
				candidates = append(candidates, types.Candidate{
					Symbol:    mustHave,
					RawSymbol: mustHave + "_UMCBL",
					Volume24h: 0.0,
				})
				symbolSet[mustHave] = struct{}{}
			}
		}

		// 确保新币合约存在
		for _, mustHave := range newSymbols {
			//移除BAN标的
			banNow := false
			for _, ban := range banSymbols {
				if ban == mustHave {
					banNow = true
				}
			}
			if banNow {
				continue
			}

			if _, exists := symbolSet[mustHave]; !exists {
				candidates = append(candidates, types.Candidate{
					Symbol:    mustHave,
					RawSymbol: mustHave + "_UMCBL",
					Volume24h: 0.0,
				})
				symbolSet[mustHave] = struct{}{}
			}
		}

		// 确保涨幅榜存在
		for _, mustHave := range topGainers {
			//移除BAN标的
			banNow := false
			for _, ban := range banSymbols {
				if ban == mustHave {
					banNow = true
				}
			}
			if banNow {
				continue
			}
			if _, exists := symbolSet[mustHave]; !exists {
				candidates = append(candidates, types.Candidate{
					Symbol:    mustHave,
					RawSymbol: mustHave + "_UMCBL",
					Volume24h: 0.0,
				})
				symbolSet[mustHave] = struct{}{}
			}
		}

		// 确保CG涨幅榜存在
		for _, mustHave := range CGTopGainers {
			//移除BAN标的
			banNow := false
			for _, ban := range banSymbols {
				if ban == mustHave {
					banNow = true
				}
			}
			if banNow {
				continue
			}
			if _, exists := symbolSet[mustHave]; !exists {
				candidates = append(candidates, types.Candidate{
					Symbol:    mustHave,
					RawSymbol: mustHave + "_UMCBL",
					Volume24h: 0.0,
				})
				symbolSet[mustHave] = struct{}{}
			}
		}

		return candidates, nil
	}

	return nil, lastErr
}
