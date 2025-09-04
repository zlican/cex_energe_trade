package types

type MarketSource string

const (
	MarketBinance MarketSource = "BINANCE"
	MarketOKX     MarketSource = "OKX"
	MarketBitget  MarketSource = "BITGET"
)

// Candidate 定义热门交易对结构体
type Candidate struct {
	Source    MarketSource // 来源：BINANCE / OKX / BITGET
	Symbol    string       // 规范化符号：BTCUSDT
	RawSymbol string       // 原始符号：BINANCE=BTCUSDT / OKX=BTC-USDT-SWAP / BITGET=BTCUSDT_UMCBL
	Volume24h float64      // 24h Quote Notional（名义成交额）
}
