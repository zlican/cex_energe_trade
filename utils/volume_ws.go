package utils

import (
	"context"
	"energe/types"
	"strconv"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
)

// NewVolumeCache：① 先 REST 预热 → ② 启动 WS → ③ 自动重连。
func NewVolumeCache(restCli *futures.Client, slipCoin []string, limitVolume float64) (*types.VolumeCache, error) {
	vc := &types.VolumeCache{
		ReadyCh:     make(chan struct{}),
		SlipCoin:    slipCoin,
		LimitVolume: limitVolume,
	}

	// -------- ① 预热快照 --------
	stats, err := restCli.NewListPriceChangeStatsService().Do(context.Background())
	if err != nil {
		return nil, err
	}
	for _, s := range stats {
		if IsSlipCoin(s.Symbol, slipCoin) {
			continue
		}
		if v, err := strconv.ParseFloat(s.QuoteVolume, 64); err == nil {
			if !strings.HasSuffix(s.Symbol, "USDT") {
				continue
			}
			if v < limitVolume {
				continue
			}
			vc.M.Store(s.Symbol, v)
		}
	}

	// -------- ② 启动 WS (带 ③ 自动重连) --------
	go vc.Loop()

	return vc, nil
}
