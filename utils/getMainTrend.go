package utils

import (
	"database/sql"
	"energe/types"
)

func GetBTCTrend(db *sql.DB) string {
	GT_BTC := GetPriceGT_EMA25FromDB(db, "BTCUSDT")
	ema25H1, ema50H1 := Get1HEMAFromDB(db, "BTCUSDT")

	TrendUP := GT_BTC && ema25H1 > ema50H1
	TrendDown := !GT_BTC && ema25H1 < ema50H1

	if TrendUP {
		return "up"
	} else if TrendDown {
		return "down"
	} else {
		return "range"
	}
}

func GetETHTrend(db *sql.DB) string {
	GT_ETH := GetPriceGT_EMA25FromDB(db, "ETHUSDT")
	ema25H1, ema50H1 := Get1HEMAFromDB(db, "ETHUSDT")

	TrendUP := GT_ETH && ema25H1 > ema50H1
	TrendDown := !GT_ETH && ema25H1 < ema50H1

	if TrendUP {
		return "up"
	} else if TrendDown {
		return "down"
	} else {
		return "range"
	}
}

func GetBETrend(db *sql.DB) types.BETrend {
	return types.BETrend{
		BTC: GetBTCTrend(db),
		ETH: GetETHTrend(db),
	}
}

func GetMainTrend(bes types.BETrend) string {
	if bes.BTC == "up" || bes.ETH == "up" {
		return "up"
	}
	if bes.BTC == "down" || bes.ETH == "down" {
		return "down"
	}
	return "none"
}
