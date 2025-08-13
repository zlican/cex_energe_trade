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

// 判断是否做多
func IsAboutToGoldenCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 4 {
		return false
	}

	A := histogram[len(histogram)-4]
	B := histogram[len(histogram)-3]
	C := histogram[len(histogram)-2]
	D := histogram[len(histogram)-1]

	// 条件一：最新柱为正
	if D > 0 {
		return true
	}
	// 条件二：左三个不是连跌就行
	if !(A > B && B > C) {
		return true
	}
	return false
}

//为正
func IsGolden(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 4 {
		return false
	}

	D := histogram[len(histogram)-1]

	// 条件一：最新柱为正
	if D > 0 {
		return true
	}
	return false
}

// 判断是否做空
func IsAboutToDeadCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 4 {
		return false
	}

	A := histogram[len(histogram)-4]
	B := histogram[len(histogram)-3]
	C := histogram[len(histogram)-2]
	D := histogram[len(histogram)-1]

	// 条件一：最新柱为负（直接看空）
	if D < 0 {
		return true
	}
	// 条件二：三根不是连涨就行
	if !(A < B && B < C) {
		return true
	}
	return false
}

// 判断是否为负
func IsDead(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 4 {
		return false
	}

	D := histogram[len(histogram)-1]

	// 条件一：最新柱为负（直接看空）
	if D < 0 {
		return true
	}
	return false
}
