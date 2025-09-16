package utils

// 计算 MACD：12EMA快线，26EMA慢线，9MACD信号，返回MACD集合，信号集合，柱子集合
func CalculateMACD(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) (macdLine, signalLine, histogram []float64) {
	emaFast, _ := CalculateEMA(closePrices, fastPeriod)
	emaSlow, _ := CalculateEMA(closePrices, slowPeriod)
	macdLine = make([]float64, len(closePrices))
	for i := range closePrices {
		macdLine[i] = emaFast[i] - emaSlow[i]
	}
	signalLine, _ = CalculateEMA(macdLine, signalPeriod) //信号只是MACD的EMA平均
	histogram = make([]float64, len(closePrices))
	for i := range closePrices {
		histogram[i] = macdLine[i] - signalLine[i]
	}
	return
}

//绿柱
func IsGolden(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 2 {
		return true
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

// 红柱
func IsDead(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 2 {
		return true
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

// DIF正
func IsDIFUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}
	DIF, _, _ := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(DIF) < 1 {
		return true
	}
	return DIF[len(DIF)-1] > 0
}

// DIF负
func IsDIFDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}
	DIF, _, _ := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(DIF) < 1 {
		return true
	}
	return DIF[len(DIF)-1] < 0
}

//强升
func XSTRONGUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	DIF, _, _ := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(DIF) < 3 {
		return true
	}

	DIFPRE := DIF[len(DIF)-2]
	DIFPRE2 := DIF[len(DIF)-3]

	return DIFPRE > 0 && DIFPRE > DIFPRE2
}

//强降
func XSTRONGDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	DIF, _, _ := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(DIF) < 3 {
		return true
	}

	DIFPRE := DIF[len(DIF)-2]
	DIFPRE2 := DIF[len(DIF)-3]

	return DIFPRE < 0 && DIFPRE < DIFPRE2
}

//为绿柱或前升
func IsSmallTFUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 1 || len(DIF) < 3 {
		return true
	}

	E := histogram[len(histogram)-1]

	DIFPRE := DIF[len(DIF)-2]
	DIFPRE2 := DIF[len(DIF)-3]

	//当下绿（确定性）
	if E > 0 {
		return true
	}

	//前者大（确定性）
	if DIFPRE > DIFPRE2 {
		return true
	}

	return false
}

//为红柱或前降
func IsSmallTFDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 1 || len(DIF) < 3 {
		return true
	}

	E := histogram[len(histogram)-1]

	DIFPRE := DIF[len(DIF)-2]
	DIFPRE2 := DIF[len(DIF)-3]
	//当下红（确定性）
	if E < 0 {
		return true
	}

	//前者小（确定性）
	if DIFPRE < DIFPRE2 {
		return true
	}

	return false
}
