package utils

import (
	"energe/types"
	"fmt"
	"strings"

	"github.com/adshao/go-binance/v2/futures"
)

func formatTFForExchange(tf, exchange string) string {
	if tf == "1M" {
		return tf
	}
	switch exchange {
	case types.MarketBinance, types.MarketBitget:
		// binance 和 bitget 全部小写
		return strings.ToLower(tf)
	case types.MarketOKX:
		// OKX 规则：m 小写，H/D/W 大写
		if strings.HasSuffix(tf, "m") || strings.HasSuffix(tf, "M") {
			return strings.ToLower(tf) // 1m, 15m 等
		}
		// 其他保持原样
		return strings.ToUpper(tf) // 1H, 1D, 1W
	default:
		return tf
	}
}

// 获取K线收盘价，带自动fallback
func GetClosesWithFallback(client *futures.Client, symbol, tf string) ([]float64, error) {

	var closes []float64
	var err error
	inst, _ := SymbolToInst(symbol)
	// 定义优先级：先用当前交易所，再尝试其他
	sources := []string{types.MarketBinance, types.MarketOKX, types.MarketBitget}

	for _, src := range sources {
		formattedTF := formatTFForExchange(tf, src)

		switch src {
		case types.MarketBinance:
			_, _, closes, err = GetKlinesByAPI(client, symbol, formattedTF, 200)
		case types.MarketOKX:
			if inst == "" {
				continue
			}
			_, _, closes, err = GetKlinesByAPI_OKX(inst, formattedTF, 200)
		case types.MarketBitget:
			_, _, closes, err = GetKlinesByAPI_Bitget(symbol, "umcbl", formattedTF, 200)
		}
		if err == nil && len(closes) > 0 {
			return closes, nil
		}
	}

	return nil, fmt.Errorf("所有数据源都失败了: %v", err)
}
