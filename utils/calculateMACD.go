package utils

import (
	"log"
	"os"
)

var progressLogger = log.New(os.Stdout, "[Screener] ", log.LstdFlags)

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

// DIF正 比较DIF与0值（100%正确）
func IsDIFUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < fastPeriod {
		return true
	}
	DIF, _, _ := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(DIF) < 2 {
		return true
	}
	return DIF[len(DIF)-2] > 0
}

// DIF负 比较DIF与0值（100%正确）
func IsDIFDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < fastPeriod {
		return true
	}
	DIF, _, _ := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(DIF) < 2 {
		return true
	}
	return DIF[len(DIF)-2] < 0
}

// 当下柱线同升	（100%正确）
func ColANDDIFUP(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < fastPeriod {
		return true
	}

	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 2 || len(DIF) < 2 {
		return true
	}

	E := histogram[len(histogram)-1]
	D := histogram[len(histogram)-2]

	DIFPRE := DIF[len(DIF)-1]
	DIFPRE2 := DIF[len(DIF)-2]

	//前者大
	if E > D && DIFPRE > DIFPRE2 {
		return true
	}

	return false
}
func ColANDDIFUPMicro(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < fastPeriod {
		return true
	}

	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 || len(DIF) < 3 {
		return true
	}

	E := histogram[len(histogram)-2]
	D := histogram[len(histogram)-3]

	DIFPRE := DIF[len(DIF)-2]
	DIFPRE2 := DIF[len(DIF)-3]

	//前者大
	if E > D && DIFPRE > DIFPRE2 {
		return true
	}

	return false
}

// 当下柱线同降 （100%正确）
func ColANDDIFDOWN(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < fastPeriod {
		return true
	}

	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 2 || len(DIF) < 2 {
		return true
	}

	E := histogram[len(histogram)-1]
	D := histogram[len(histogram)-2]

	DIFPRE := DIF[len(DIF)-1]
	DIFPRE2 := DIF[len(DIF)-2]

	//前者小
	if E < D && DIFPRE < DIFPRE2 {
		return true
	}

	return false
}
func ColANDDIFDOWNMicro(closePrices []float64, fastPeriod, slowPeriod, signalPeriod int) bool {
	if len(closePrices) < fastPeriod {
		return true
	}

	DIF, _, histogram := CalculateMACD(closePrices, fastPeriod, slowPeriod, signalPeriod)
	if len(histogram) < 3 || len(DIF) < 3 {
		return true
	}

	E := histogram[len(histogram)-2]
	D := histogram[len(histogram)-3]

	DIFPRE := DIF[len(DIF)-2]
	DIFPRE2 := DIF[len(DIF)-3]

	//前者小
	if E < D && DIFPRE < DIFPRE2 {
		return true
	}

	return false
}
