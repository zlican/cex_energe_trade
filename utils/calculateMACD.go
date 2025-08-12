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
		// 判断 A, B, C 是否连续递减
		if !(A > B && B > C) {
			return true
		}
	}
	// 条件二：三根都为负且逐步抬高（从更负到接近 0）：B < C < D
	if B < 0 && C < 0 && D < 0 && B < C && C < D {
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

	A := histogram[len(histogram)-4]
	B := histogram[len(histogram)-3]
	C := histogram[len(histogram)-2]
	D := histogram[len(histogram)-1]

	// 条件一：最新柱为正
	if D > 0 {
		// 判断 A, B, C 是否连续递减
		if !(A > B && B > C) {
			return true
		}
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
		if !(A < B && B < C) {
			return true
		}
	}
	// 条件二：三根都为正且逐步走低（从高到低）：A > B > C
	if B > 0 && C > 0 && D > 0 && B > C && C > D {
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

	A := histogram[len(histogram)-4]
	B := histogram[len(histogram)-3]
	C := histogram[len(histogram)-2]
	D := histogram[len(histogram)-1]

	// 条件一：最新柱为负（直接看空）
	if D < 0 {
		if !(A < B && B < C) {
			return true
		}
	}
	return false
}
