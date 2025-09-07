package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type BitgetKline struct {
	// Bitget 返回的 timestamp 为毫秒
	Timestamp int64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Vol       float64
}

// GetKlinesByAPI_Bitget 获取 Bitget 永续合约（mix）K线数据（V2 接口）
// symbol 示例：BTCUSDT（不含业务线后缀）
// productType 示例：umcbl（U本位永续合约）
// tf 示例："1m","5m","15m","1h","4h","1d"
// limit 建议 1-1000 之间

// 正确实例：https://api.bitget.com/api/v2/mix/market/candles?symbol=BTCUSDT&productType=umcbl&granularity=1H&limit=100
func GetKlinesByAPI_Bitget(symbol string, productType string, tf string, limit int) ([]*BitgetKline, []float64, []float64, error) {

	const (
		maxRetries      = 3
		requestTimeout  = 7 * time.Second
		baseURL         = "https://api.bitget.com"
		proxyURL        = "http://127.0.0.1:10809"
		maxLimitAllowed = 1000
		minLimitAllowed = 1
	)
	var lastErr error
	// Symbol filtering
	// 1. Remove leading "1000" if present
	if strings.HasPrefix(symbol, "1000") {
		symbol = strings.TrimPrefix(symbol, "1000")
	}
	// 2. Remove trailing "OFFICIAL" if present
	if strings.HasSuffix(symbol, "OFFICIAL") {
		symbol = strings.TrimSuffix(symbol, "OFFICIAL")
	}

	// 验证 symbol（仅为交易对，如 BTCUSDT）
	if strings.Contains(symbol, "_") {
		return nil, nil, nil, fmt.Errorf("invalid symbol for V2 API: %s, expected like BTCUSDT without suffix", symbol)
	}

	// 验证 productType（支持 umcbl 等）
	validProductTypes := map[string]bool{
		"umcbl": true, // U本位永续合约
		"dmcbl": true, // 币本位永续合约
		// 可根据文档扩展其他业务线
	}
	if !validProductTypes[productType] {
		return nil, nil, nil, fmt.Errorf("invalid productType: %s, expected like umcbl, dmcbl", productType)
	}

	// K线周期映射（V2 使用字符串格式，如 1h）
	// should be [1m,3m,5m,15m,30m,1H,4H,6H,12H,1D,1W,1M,6Hutc,12Hutc,1Dutc,3Dutc,1Wutc,1Mutc]
	periodMap := map[string]string{
		"1m":  "1m",
		"5m":  "5m",
		"15m": "15m",
		"1h":  "1H",
		"4h":  "4H",
		"1d":  "1D",
	}
	granularity, ok := periodMap[tf]
	if !ok {
		return nil, nil, nil, fmt.Errorf("unsupported timeframe: %s", tf)
	}

	// 规范 limit
	if limit < minLimitAllowed {
		limit = minLimitAllowed
	}
	if limit > maxLimitAllowed {
		limit = maxLimitAllowed
	}

	proxy, err := url.Parse(proxyURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid proxy URL %s: %v", proxyURL, err)
	}

	// 轻量的 http.Client，可复用到全局
	client := &http.Client{

		Timeout: requestTimeout,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy), // 设置代理
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
		// 在本轮请求结束前释放
		defer cancel()

		// V2 接口路径，新增 productType 参数
		url := fmt.Sprintf("%s/api/v2/mix/market/candles?symbol=%s&productType=%s&granularity=%s&limit=%d",
			baseURL, symbol, productType, granularity, limit)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			progressLogger.Printf("[Bitget][KLINES] attempt %d build request error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "kline-fetcher/1.0")
		req.Header.Set("locale", "en-US") // V2 推荐添加 locale 请求头

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			progressLogger.Printf("[Bitget][KLINES] attempt %d request error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			progressLogger.Printf("[Bitget][KLINES] attempt %d read body error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// 先检查 HTTP 状态
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("http status %d body: %s", resp.StatusCode, string(body))
			progressLogger.Printf("[Bitget][KLINES] attempt %d http error: %v\n", attempt, lastErr)
			// 对于 4xx 大概率不可重试，直接返回
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				return nil, nil, nil, lastErr
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		var raw struct {
			Code string     `json:"code"`
			Msg  string     `json:"msg"`
			Data [][]string `json:"data"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			lastErr = err
			progressLogger.Printf("[Bitget][KLINES] attempt %d json unmarshal error: %v\n", attempt, err)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		// 成功码以 "00000" 为主
		if raw.Code != "00000" {
			lastErr = fmt.Errorf("bitget api error: code=%s msg=%s", raw.Code, raw.Msg)
			progressLogger.Printf("[Bitget][KLINES] attempt %d api returned error: %s %s\n", attempt, raw.Code, raw.Msg)

			// ===== 新增：判断交易对不存在，直接返回，不再重试 =====
			if raw.Code == "40034" || strings.Contains(raw.Msg, "does not exist") {
				return nil, nil, nil, lastErr
			}

			// 常见 4xx 业务错误（参数错误等）可不重试
			if raw.Code != "" && strings.HasPrefix(raw.Code, "4") {
				return nil, nil, nil, lastErr
			}

			// 其他错误继续重试
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		if len(raw.Data) == 0 {
			return nil, nil, nil, errors.New("empty data from bitget")
		}

		klines := make([]*BitgetKline, 0, len(raw.Data))
		opens := make([]float64, 0, len(raw.Data))
		closes := make([]float64, 0, len(raw.Data))

		// raw.Data 已经是正序（最早在前），直接正序遍历
		for i := 0; i < len(raw.Data); i++ {
			row := raw.Data[i]
			// 期待字段：[timestamp(ms), open, high, low, close, volume, amount]
			if len(row) < 7 { // V2 返回 7 个字段，多了一个 amount
				continue
			}
			ts, errTs := strconv.ParseInt(row[0], 10, 64)
			o, errO := strconv.ParseFloat(row[1], 64)
			h, errH := strconv.ParseFloat(row[2], 64)
			l, errL := strconv.ParseFloat(row[3], 64)
			c, errC := strconv.ParseFloat(row[4], 64)
			vol, errV := strconv.ParseFloat(row[5], 64)
			if errTs != nil || errO != nil || errH != nil || errL != nil || errC != nil || errV != nil {
				// 遇到解析异常，跳过该行
				continue
			}

			klines = append(klines, &BitgetKline{
				Timestamp: ts, // 毫秒
				Open:      o,
				High:      h,
				Low:       l,
				Close:     c,
				Vol:       vol,
			})
			opens = append(opens, o)
			closes = append(closes, c)
		}

		return klines, opens, closes, nil
	}

	return nil, nil, nil, lastErr
}
