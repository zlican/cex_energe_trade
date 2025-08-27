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
		restBase:      "https://www.okx.com",
	}
	for _, s := range slipCoin {
		vc.SlipSet[s] = struct{}{}
	}

	// http client
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			tr.Proxy = http.ProxyURL(u)
		}
	}
	vc.httpClient = &http.Client{Transport: tr, Timeout: 10 * time.Second}

	// ① 首次预热
	if err := vc.preheat(); err != nil {
		return nil, err
	}
	vc.ReadyOnce.Do(func() { close(vc.ReadyCh) })

	// ② 每 17 分钟刷新一次
	go func() {
		ticker := time.NewTicker(17 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := vc.preheat(); err != nil {
					fmt.Printf("[OKX][REFRESH] 定时刷新失败: %v\n", err)
				}
			case <-vc.Stop:
				return
			}
		}
	}()

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
