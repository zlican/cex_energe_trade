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

// 判断是否即将金叉或柱子为正
func IsAboutToGoldenCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	//去除未来函数影响，不用当下的一根
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	histNow := histogram[len(histogram)-2]
	histPrev := histogram[len(histogram)-3]

	// 1. 红柱缩短
	UPHist := histNow > histPrev

	// 2. 柱子为正
	histogramUpZero := histNow >= 0

	return UPHist || histogramUpZero
}

// 判断是否为正
func IsGolden(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	histogramNow := histogram[len(histogram)-2]

	histogramUpZero := histogramNow >= 0

	return histogramUpZero
}

// 判断是否即将死叉或柱子为负
func IsAboutToDeadCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	histNow := histogram[len(histogram)-2]
	histPrev := histogram[len(histogram)-3]

	// 1. 绿柱缩短
	DOWNHist := histNow < histPrev

	// 2. 柱子刚下0：当前柱子略小于0但很小（刚刚死叉）
	histogramBelowZero := histNow < 0

	return DOWNHist || histogramBelowZero
}

// 判断是否为负
func IsDead(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}
	histogramNow := histogram[len(histogram)-2]
	histogramBelowZero := histogramNow < 0

	return histogramBelowZero
}
