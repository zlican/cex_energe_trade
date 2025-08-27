package utils

import (
	"energe/types"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

func NewVolumeCache(restCli *futures.Client, slipCoin []string, limitVolume float64) (*types.VolumeCache, error) {
	vc := &types.VolumeCache{
		ReadyCh:     make(chan struct{}),
		SlipCoin:    slipCoin,
		LimitVolume: limitVolume,
	}

	// 首次预热
	if err := vc.Refresh(restCli); err != nil {
		return nil, err
	}

	// 定时刷新（每17分钟）
	go func() {
		ticker := time.NewTicker(17 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = vc.Refresh(restCli) // 可以加 log 打印错误
			case <-vc.Stop:
				return
			}
		}
	}()

	return vc, nil
}
