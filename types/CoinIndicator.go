package types

type CoinIndicator struct {
	Symbol       string
	Inst         string
	Price        float64
	TimeInternal string
	StochRSI     float64 // 只存最后一个值够用了
	Operation    string
	Status       string
	Source       MarketSource
}
