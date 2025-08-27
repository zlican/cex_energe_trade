package okx

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// -------------------------- 公共类型 --------------------------

type VolumeCache struct {
	M             sync.Map // canonicalSymbol -> 24h notional (float64)
	RawSym        sync.Map // canonicalSymbol -> raw instId (e.g. BTC-USDT-SWAP)
	Stop          chan struct{}
	ReadyOnce     sync.Once
	ReadyCh       chan struct{}
	SlipSet       map[string]struct{}
	LimitNotional float64

	restBase   string
	wsURL      string
	httpClient *http.Client
	wsDialer   *websocket.Dialer
}

func NewVolumeCacheOKX(proxy string, slipCoin []string, limitNotional float64) (*VolumeCache, error) {
	vc := &VolumeCache{
		Stop:          make(chan struct{}),
		ReadyCh:       make(chan struct{}),
		SlipSet:       make(map[string]struct{}),
		LimitNotional: limitNotional,
		restBase:      "https://www.okx.com",                // 生产 REST
		wsURL:         "wss://ws.okx.com:8443/ws/v5/public", // 生产 WS Public
	}
	for _, s := range slipCoin {
		vc.SlipSet[s] = struct{}{}
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	}
	vc.httpClient = &http.Client{Transport: tr, Timeout: 10 * time.Second}
	vc.wsDialer = &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 10 * time.Second,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: false},
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			vc.wsDialer.Proxy = http.ProxyURL(u)
		}
	}

	// ① 预热
	if err := vc.preheat(); err != nil {
		fmt.Printf("[OKX][PREHEAT] 预热失败: %v\n", err)
		return nil, err
	}
	// ② 启动 WS（含 ③ 自动重连）
	go vc.loop()
	return vc, nil
}

// Ready: 返回一个只会关闭一次的 chan，表示“至少收到过一次 WS 推送”。
func (vc *VolumeCache) Ready() <-chan struct{} { return vc.ReadyCh }

// Close: 关闭 WS 读循环。
func (vc *VolumeCache) Close() {
	if vc.Stop != nil {
		close(vc.Stop)
	}
}

// GetNotional: 获取 canonical 符号的 24h 名义成交额
func (vc *VolumeCache) GetNotional(symbol string) (float64, bool) {
	v, ok := vc.M.Load(symbol)
	if !ok {
		return 0, false
	}
	return v.(float64), true
}

// RawSymbol: 获取 canonical 对应的 OKX 原始 instId（如 BTC-USDT-SWAP）
func (vc *VolumeCache) RawSymbol(symbol string) (string, bool) {
	v, ok := vc.RawSym.Load(symbol)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// SymbolsAboveNotional: 返回所有 notional > limit 的 canonical 符号列表
func (vc *VolumeCache) SymbolsAboveNotional(limit float64) []string {
	var res []string
	vc.M.Range(func(k, val any) bool {
		if v, ok := val.(float64); ok && v > limit {
			res = append(res, k.(string))
		}
		return true
	})
	return res
}

// -------------------------- 内部实现 --------------------------

func (vc *VolumeCache) preheat() error {
	// GET /api/v5/market/tickers?instType=SWAP
	url := fmt.Sprintf("%s/api/v5/market/tickers?instType=SWAP", vc.restBase)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := vc.httpClient.Do(req)
	if err != nil {
		fmt.Printf("[OKX][PREHEAT] HTTP 请求错误: %v\n", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("[OKX][PREHEAT] 非 200 状态码: %d\n", resp.StatusCode)
		return fmt.Errorf("okx preheat http %d", resp.StatusCode)
	}
	var out struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstType  string `json:"instType"`
			InstId    string `json:"instId"`
			Last      string `json:"last"`
			VolCcy24h string `json:"volCcy24h"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		fmt.Printf("[OKX][PREHEAT] JSON 解码失败: %v\n", err)
		return err
	}
	if out.Code != "0" {
		fmt.Printf("[OKX][PREHEAT] 返回错误 code=%s, msg=%s\n", out.Code, out.Msg)
		return errors.New("okx preheat failed: code=" + out.Code + ", msg=" + out.Msg)
	}

	var (
		total      int
		kept       int
		skSlip     int
		skNotional int
	)
	for _, d := range out.Data {
		total++
		if !strings.HasSuffix(d.InstId, "-USDT-SWAP") {
			continue
		}
		canonical := CanonicalFromInstID(d.InstId)
		if vc.isSlip(canonical) {
			skSlip++
			continue
		}
		last := atof(d.Last)
		volBase := atof(d.VolCcy24h) // 衍生品：基币数量
		notional := last * volBase
		if notional < vc.LimitNotional {
			skNotional++
			continue
		}
		vc.M.Store(canonical, notional)
		vc.RawSym.Store(canonical, d.InstId)
		kept++
	}
	return nil
}

func (vc *VolumeCache) loop() {
	retry := time.Second * 5
	for {
		select {
		case <-vc.Stop:
			return
		default:
		}

		c, _, err := vc.wsDialer.Dial(vc.wsURL, nil)
		if err != nil {
			fmt.Printf("[OKX] WS 连接失败: %v, %v 后重试\n", err, retry)
			time.Sleep(retry)
			continue
		}

		// 订阅：tickers 使用具体 instId（OKX 不支持通过 instType=SWAP 订阅全量 tickers）
		// 从预热阶段保留的 RawSym 中提取 instId 列表，并分批订阅
		var instIds []string
		vc.RawSym.Range(func(k, v any) bool {
			if s, ok := v.(string); ok {
				instIds = append(instIds, s)
			}
			return true
		})
		if len(instIds) == 0 {
			fmt.Printf("[OKX][WS] 无可订阅 instId（RawSym 为空），将仅保持连接等待...\n")
		} else {
			batch := 20 // OKX 建议分批订阅，避免单条过大
			sent := 0
			for i := 0; i < len(instIds); i += batch {
				j := i + batch
				if j > len(instIds) {
					j = len(instIds)
				}
				var args []map[string]string
				for _, inst := range instIds[i:j] {
					args = append(args, map[string]string{
						"channel": "tickers",
						"instId":  inst,
					})
				}
				sub := map[string]any{
					"op":   "subscribe",
					"args": args,
				}
				if err := c.WriteJSON(sub); err != nil {
					fmt.Printf("[OKX] WS 分批订阅失败: %v\n", err)
					_ = c.Close()
					time.Sleep(retry)
					continue
				}
				sent += len(args)
			}
		}

		c.SetReadLimit(1 << 20) // 1MB
		_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
		c.SetPongHandler(func(string) error {
			_ = c.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})

		go func(conn *websocket.Conn) {
			// 心跳：每 20s 发送 ping
			t := time.NewTicker(20 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-vc.Stop:
					return
				case <-t.C:
					_ = conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
				}
			}
		}(c)

		msgCount := 0
		totalKept := 0
		totalSlip := 0
		totalLow := 0
		for {
			_, msg, err := c.ReadMessage()
			if err != nil { // 断线，重连
				fmt.Printf("[OKX] WS 读失败: %v，重连...\n", err)
				_ = c.Close()
				break
			}
			msgCount++

			// 解析消息（包含事件/数据推送）
			var base struct {
				Event string          `json:"event"`
				Arg   json.RawMessage `json:"arg"`
				Data  json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(msg, &base); err != nil {
				fmt.Printf("[OKX][WS] 消息 JSON 解析失败: %v (size=%d)\n", err, len(msg))
				continue
			}
			if len(base.Data) == 0 { // 非数据推送（订阅确认/notice 等），忽略
				if msgCount <= 3 || msgCount%200 == 0 {
					fmt.Printf("[OKX][WS] 收到非数据消息(第%d条)，忽略\n", msgCount)
				}
				continue
			}
			var arr []struct {
				InstType  string `json:"instType"`
				InstId    string `json:"instId"`
				Last      string `json:"last"`
				VolCcy24h string `json:"volCcy24h"`
			}
			if err := json.Unmarshal(base.Data, &arr); err != nil {
				fmt.Printf("[OKX][WS] data 字段解析失败: %v\n", err)
				continue
			}
			// 首次收到任何推送，触发 ready
			vc.ReadyOnce.Do(func() { close(vc.ReadyCh) })
			if msgCount == 1 {
				fmt.Printf("[OKX][WS] 首次收到数据推送，Ready\n")
			}

			for _, d := range arr {
				if !strings.HasSuffix(d.InstId, "-USDT-SWAP") {
					continue
				}
				canonical := CanonicalFromInstID(d.InstId)
				if vc.isSlip(canonical) {
					totalSlip++
					continue
				}
				last := atof(d.Last)
				volBase := atof(d.VolCcy24h)
				notional := last * volBase
				if notional < vc.LimitNotional {
					totalLow++
					continue
				}
				vc.M.Store(canonical, notional)
				vc.RawSym.Store(canonical, d.InstId)
				totalKept++
			}
		}
	}
}

func (vc *VolumeCache) isSlip(canonical string) bool {
	_, ok := vc.SlipSet[canonical]
	return ok
}

func CanonicalFromInstID(instId string) string {
	// e.g. "BTC-USDT-SWAP" -> "BTCUSDT"
	parts := strings.Split(instId, "-")
	if len(parts) >= 2 {
		return parts[0] + parts[1]
	}
	return strings.ReplaceAll(instId, "-", "")
}

func atof(s string) float64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
