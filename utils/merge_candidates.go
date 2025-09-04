package utils

import (
	"energe/okx"
	"energe/types"
)

// BuildCandidates: 合并 Binance + OKX 的候选标的。
// - binanceLimit: 例如 300_000_000
// - okxLimit:     例如 200_000_000
// - 规则: 如果某个 Symbol 同时出现在 Binance 和 OKX 中，只保留 Binance 的
func BuildCandidates(binanceCache *types.VolumeCache, okxCache *okx.VolumeCache, binanceLimit, okxLimit float64) []types.Candidate {
	var out []types.Candidate
	seen := make(map[string]struct{}) // 记录已添加的 Symbol

	// 1) Binance
	if binanceCache != nil {
		syms := binanceCache.SymbolsAbove(binanceLimit)
		for _, s := range syms {
			if v, ok := binanceCache.Get(s); ok {
				out = append(out, types.Candidate{
					Source:    types.MarketBinance,
					Symbol:    s,
					RawSymbol: s,
					Volume24h: v,
				})
				seen[s] = struct{}{}
			}
		}
	}

	// 2) OKX（只添加 Binance 没有的 Symbol）
	if okxCache != nil {
		syms := okxCache.SymbolsAboveNotional(okxLimit)
		for _, s := range syms {
			// 如果 Binance 已有，就跳过
			if _, exists := seen[s]; exists {
				continue
			}
			if v, ok := okxCache.GetNotional(s); ok {
				raw, _ := okxCache.RawSymbol(s)
				out = append(out, types.Candidate{
					Source:    types.MarketOKX,
					Symbol:    s,
					RawSymbol: raw,
					Volume24h: v,
				})
				seen[s] = struct{}{}
			}
		}
	}

	return out
}
