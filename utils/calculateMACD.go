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
func IsGoldenCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}

	A := histogram[len(histogram)-5]
	B := histogram[len(histogram)-4]
	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	// 一：新正旧负
	if D < 0 && E > 0 {
		return true
	}

	// 二：旧正 且不是4连降
	if D > 0 && !(A > 0 && B > 0 && C > 0 && D > 0 && A > B && B > C && C > D) {
		return true
	}
	// 三：（皆负）旧两个不是下跌就行
	if D > C {
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

	A := histogram[len(histogram)-5]
	B := histogram[len(histogram)-4]
	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	// 一：新正旧负
	if D < 0 && E > 0 {
		return true
	}
	// 二：旧正 且不是4连降
	if D > 0 && !(A > 0 && B > 0 && C > 0 && D > 0 && A > B && B > C && C > D) {
		return true
	}
	return false
}

// 判断是否做空
func IsDeadCross(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}
	A := histogram[len(histogram)-5]
	B := histogram[len(histogram)-4]
	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	//一： 新负旧正
	if D > 0 && E < 0 {
		return true
	}

	// 二：旧负 且不是4连涨
	if D < 0 && !(A < 0 && B < 0 && C < 0 && D < 0 && A < B && B < C && C < D) {
		return true
	}
	// 三：（皆正）旧两根不是连涨就行
	if D < C {
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

	A := histogram[len(histogram)-5]
	B := histogram[len(histogram)-4]
	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]
	//一： 新负旧正
	if D > 0 && E < 0 {
		return true
	}
	// 二：旧负 且不是4连涨
	if D < 0 && !(A < 0 && B < 0 && C < 0 && D < 0 && A < B && B < C && C < D) {
		return true
	}
	return false
}
