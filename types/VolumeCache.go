package types

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/adshao/go-binance/v2/futures"
)

// VolumeCache 通过单条 WS 链接实时维护 24 h QuoteVolume。
type VolumeCache struct {
	M           sync.Map
	Stop        chan struct{}
	ReadyOnce   sync.Once     // 首次推送到达的保护
	ReadyCh     chan struct{} // 外部等待用
	SlipCoin    []string      // ✅ 用 map 快速判断是否是 slipCoin
	LimitVolume float64       // ✅ volume 下限
}

// Get 返回最新 QuoteVolume；若还没数据 ok=false。
func (vc *VolumeCache) Get(sym string) (float64, bool) {
	v, ok := vc.M.Load(sym)
	if !ok {
		return 0, false
	}
	return v.(float64), true
}

// Ready 返回一个只会关闭一次的 chan，表示“至少收到过一次推送”。
func (vc *VolumeCache) Ready() <-chan struct{} { return vc.ReadyCh }

// Close 优雅关流。
func (vc *VolumeCache) Close() {
	if vc.Stop != nil {
		close(vc.Stop)
	}
}

// SymbolsAbove 返回所有 QuoteVolume > limit 的交易对 symbol 列表
func (vc *VolumeCache) SymbolsAbove(limit float64) []string {
	var result []string
	vc.M.Range(func(key, value any) bool {
		volume, ok := value.(float64)
		if !ok {
			return true
		}
		if volume > limit {
			result = append(result, key.(string))
		}
		return true
	})
	return result
}

func isSlipCoin(sym string, slipCoin []string) bool {
	for _, s := range slipCoin {
		if s == sym {
			return true
		}
	}
	return false
}

// Refresh 拉取 REST 并更新 vc.M
func (vc *VolumeCache) Refresh(cli *futures.Client) error {
	stats, err := cli.NewListPriceChangeStatsService().Do(context.Background())
	if err != nil {
		return err
	}

	tmp := sync.Map{}
	for _, s := range stats {
		if isSlipCoin(s.Symbol, vc.SlipCoin) {
			continue
		}
		if !strings.HasSuffix(s.Symbol, "USDT") {
			continue
		}
		if v, err := strconv.ParseFloat(s.QuoteVolume, 64); err == nil {
			if v >= vc.LimitVolume {
				tmp.Store(s.Symbol, v)
			}
		}
	}
	vc.M = tmp // ✅ 整体替换，避免旧数据残留
	vc.ReadyOnce.Do(func() { close(vc.ReadyCh) })
	return nil
}
