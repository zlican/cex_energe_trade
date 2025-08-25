package utils

/* // 计算 MACD：12EMA快线，26EMA慢线，9MACD信号，返回MACD集合，信号集合，柱子集合
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
} */
func CalculateMACD(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) (macdLine, signalLine, histogram []float64) {
	var scale = 2.0
	n := len(closePrices)
	if n == 0 {
		return nil, nil, nil
	}

	// 这里用改进版 EMA（不足周期时用现有均值）
	emaFast := CalculateEMA(closePrices, fastPeriod)
	emaSlow := CalculateEMA(closePrices, slowPeriod)

	macdLine = make([]float64, n)   // DIF
	signalLine = make([]float64, n) // DEA
	histogram = make([]float64, n)  // MACD柱

	// DIF = EMA(fast) - EMA(slow)
	for i := 0; i < n; i++ {
		macdLine[i] = emaFast[i] - emaSlow[i]
	}

	// DEA = EMA(DIF, signalPeriod)（不足周期时同样用现有均值）
	signalLine = CalculateEMA(macdLine, signalPeriod)

	// MACD柱 = (DIF - DEA) * scale
	for i := 0; i < n; i++ {
		histogram[i] = (macdLine[i] - signalLine[i]) * scale
	}

	return macdLine, signalLine, histogram
}

//为正
func IsGoldenUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	if E > 0 {
		return true
	}
	if D > 0 {
		return true
	}

	if C < 0 && D < 0 && C < D {
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
	if len(histogram) < 5 {
		return false
	}
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	if E > 0 {
		return true
	}
	if D > 0 {
		return true
	}

	return false
}

//为负
func IsDeadDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	if E < 0 {
		return true
	}
	if D < 0 {
		return true
	}

	if C > 0 && D > 0 && C > D {
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
	if len(histogram) < 5 {
		return false
	}
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	if E < 0 {
		return true
	}
	if D < 0 {
		return true
	}
	return false
}

// 判断DEA趋势
func IsDEAUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, DEA, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}
	return DEA[len(DEA)-1] > 0
}

// 判断DEA趋势
func IsDEADOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, DEA, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}
	return DEA[len(DEA)-1] < 0
}
