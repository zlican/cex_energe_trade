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

//为绿柱 + 红UP
func IsGoldenUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return true
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

	if D > C {
		return true
	}

	return false
}

//为绿柱
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

//为红柱 + 绿DOWN
func IsDeadDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return true
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

	if D < C {
		return true
	}

	return false
}

// 为红柱
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

// DEA UP
func IsDEAUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, DEA, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 2 {
		return false
	}
	return DEA[len(DEA)-1] > 0
}

// DEA DOWN
func IsDEADOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}
	_, DEA, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 2 {
		return false
	}
	return DEA[len(DEA)-1] < 0
}

// DIF UP
func IsDIFUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}
	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}
	return DIF[len(DIF)-1] > 0
}

// DEA DOWN
func IsDIFDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return true
	}
	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 5 {
		return false
	}
	return DIF[len(DIF)-1] < 0
}

//升
func UPUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	return E > D || D > C
}

//降
func DownDown(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	return E < D || D < C
}

//强绿升
func XSTRONGUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]

	return D > 0 && D > C
}

//强绿升
func XSTRONGUPNow(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-2]
	D := histogram[len(histogram)-1]

	return D > 0 && D > C
}

//强红降
func XSTRONGDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]

	return D < 0 && D < C
}

//强红降
func XSTRONGDOWNNow(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-2]
	D := histogram[len(histogram)-1]

	return D < 0 && D < C
}

//为正
func IsSmallTFUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	//当下绿（确定性）
	if E > 0 {
		return true
	}

	//前者大（确定性）
	if D > C {
		return true
	}

	return false
}

//为负
func IsSmallTFDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < slowPeriod+signalPeriod+1 {
		return false
	}

	_, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 {
		return false
	}

	C := histogram[len(histogram)-3]
	D := histogram[len(histogram)-2]
	E := histogram[len(histogram)-1]

	//当下红（确定性）
	if E < 0 {
		return true
	}

	//前者小（确定性）
	if D < C {
		return true
	}

	return false
}
