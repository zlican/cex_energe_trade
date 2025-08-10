package utils

// 计算 MACD：12EMA快线，26EMA慢线，9MACD信号，返回MACD集合，信号集合，柱子集合
func CalculateMACD(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) (macdLine, signalLine, histogram []float64) {
	emaFast := CalculateEMA(closePrices, fastPeriod)
	emaSlow := CalculateEMA(closePrices, slowPeriod)
	macdLine = make([]float64, len(closePrices))
	for i := range closePrices {
		macdLine[i] = emaFast[i] - emaSlow[i]
	}
	signalLine = CalculateEMA(macdLine, signalPeriod) //信号只是MACD的EMA平均
	histogram = make([]float64, len(closePrices))
	for i := range closePrices {
		histogram[i] = macdLine[i] - signalLine[i]
	}
	return
}

// 判断是否即将金叉或柱子刚上0
func IsAboutToGoldenCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	macdLine, signalLine, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(macdLine) < 2 || len(signalLine) < 2 || len(histogram) < 2 {
		return false
	}

	macdNow := macdLine[len(macdLine)-1]
	macdPrev := macdLine[len(macdLine)-2]
	signalNow := signalLine[len(signalLine)-1]
	signalPrev := signalLine[len(signalLine)-2]
	histogramNow := histogram[len(histogram)-1]

	// 1. 即将金叉
	macdRate := macdNow - macdPrev
	signalRate := signalNow - signalPrev
	aboutToCross := macdNow < signalNow && macdRate > signalRate

	// 2. 柱子刚刚转正
	histogramUpZero := histogramNow >= 0

	return aboutToCross || histogramUpZero
}

// 判断是否柱子上0
func IsGolden(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	macdLine, signalLine, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(macdLine) < 2 || len(signalLine) < 2 || len(histogram) < 2 {
		return false
	}

	histogramNow := histogram[len(histogram)-1]

	histogramUpZero := histogramNow >= 0

	return histogramUpZero
}

// 判断是否即将死叉或柱子刚下0
func IsAboutToDeadCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	macdLine, signalLine, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(macdLine) < 2 || len(signalLine) < 2 || len(histogram) < 2 {
		return false
	}

	macdNow := macdLine[len(macdLine)-1]
	macdPrev := macdLine[len(macdLine)-2]
	signalNow := signalLine[len(signalLine)-1]
	signalPrev := signalLine[len(signalLine)-2]
	histogramNow := histogram[len(histogram)-1]

	// 1. 即将死叉：DIF 在 DEA 上方但下降速度更快
	macdRate := macdNow - macdPrev
	signalRate := signalNow - signalPrev
	aboutToCrossDown := macdNow > signalNow && macdRate < signalRate

	// 2. 柱子刚下0：当前柱子略小于0但很小（刚刚死叉）
	histogramBelowZero := histogramNow < 0

	return aboutToCrossDown || histogramBelowZero
}

// 判断是否即将死叉或柱子刚下0
func IsDead(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	macdLine, signalLine, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(macdLine) < 2 || len(signalLine) < 2 || len(histogram) < 2 {
		return false
	}
	histogramNow := histogram[len(histogram)-1]
	histogramBelowZero := histogramNow < 0

	return histogramBelowZero
}
