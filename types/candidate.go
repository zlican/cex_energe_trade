package types

type MarketSource string

const (
	SourceBinance MarketSource = "BINANCE"
	SourceOKX     MarketSource = "OKX"
)

type Candidate struct {
	Source    MarketSource // 来源：BINANCE / OKX
	Symbol    string       // 规范化符号：BTCUSDT
	RawSymbol string       // 原始符号：BINANCE=BTCUSDT / OKX=BTC-USDT-SWAP
	Volume24h float64      // 24h Quote Notional（名义成交额）
}
