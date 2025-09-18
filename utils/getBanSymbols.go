package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

// 从 ban 服务返回的内容是一个字符串数组，例如：["BTCUSDT","ETHUSDT"]
func GetBanList() []string {
	const (
		baseURL        = "http://127.0.0.1:9001/cex/ban/list"
		maxRetries     = 3
		requestTimeout = 10 * time.Second
	)

	var lastErr error
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

	backoff := time.Second // 初始退避时间 1 秒

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL, nil)
		if err != nil {
			lastErr = fmt.Errorf("build request error: %v", err)
			log.Printf("[GetBanList] attempt %d build request error: %v", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// 模拟浏览器请求
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Go-http-client)")
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("request error: %v", err)
			log.Printf("[GetBanList] attempt %d request error: %v", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read body error: %v", err)
			log.Printf("[GetBanList] attempt %d read body error: %v", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("http status %d body: %s", resp.StatusCode, string(body))
			log.Printf("[GetBanList] attempt %d http error: %v", attempt, lastErr)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		var symbols []string
		if err := json.Unmarshal(body, &symbols); err != nil {
			lastErr = fmt.Errorf("json unmarshal error: %v, body=%s", err, string(body))
			log.Printf("[GetBanList] attempt %d json unmarshal error: %v", attempt, lastErr)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// 成功返回
		return symbols
	}

	log.Printf("[GetBanList] failed after %d attempts, last error: %v", maxRetries, lastErr)
	return []string{}
}

// 定期获取 ban 列表并发送到 channel
func StartBanListFetcher(ch chan<- []string) {
	go func() {
		// 启动时立即取一次
		symbols := GetBanList()
		select {
		case ch <- symbols:
		default:
			fmt.Println("Warning: channel blocked, skipping first update")
		}

		// 每隔 1 分钟取一次
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			symbols := GetBanList()
			select {
			case ch <- symbols:
			default:
				fmt.Println("Warning: channel blocked, skipping update")
			}
		}
	}()
}
