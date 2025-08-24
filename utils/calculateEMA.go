package utils

/* // 计算指数移动平均线
func CalculateEMA(data []float64, period int) []float64 {
	ema := make([]float64, len(data))
	multiplier := 2.0 / float64(period+1)
	ema[0] = data[0]
	for i := 1; i < len(data); i++ {
		ema[i] = (data[i]-ema[i-1])*multiplier + ema[i-1]
	}
	return ema
}

// CalculateEMADerivative 返回 ema 曲线的一阶导数（离散斜率）。
// derivative[i] = ema[i] - ema[i-1]，长度与 ema 相同，
// 其中 derivative[0] 约定为 0（或可改为 NaN / math.NaN()）。
func CalculateEMADerivative(ema []float64) []float64 {
	if len(ema) == 0 {
		return nil
	}
	derivative := make([]float64, len(ema))
	derivative[0] = 0 // 或 math.NaN()

	for i := 1; i < len(ema); i++ {
		derivative[i] = ema[i] - ema[i-1]
	}
	return derivative
}
*/

func CalculateEMA(data []float64, period int) []float64 {
	if len(data) == 0 {
		return nil
	}

	ema := make([]float64, len(data))
	multiplier := 2.0 / float64(period+1)

	// 初始化 EMA[0]
	if len(data) < period {
		// 用所有数据的均值作为起点
		sum := 0.0
		for _, v := range data {
			sum += v
		}
		ema[0] = sum / float64(len(data))
	} else {
		// 用前 period 个数据的均值作为起点
		sum := 0.0
		for i := 0; i < period; i++ {
			sum += data[i]
		}
		ema[period-1] = sum / float64(period)

		// 前 period-1 个点：直接等于 data（保证有值）
		for i := 0; i < period-1; i++ {
			ema[i] = data[i]
		}
	}

	// 正常递推 EMA
	for i := 1; i < len(data); i++ {
		ema[i] = (data[i]-ema[i-1])*multiplier + ema[i-1]
	}

	return ema
}

// 计算 EMA 的一阶导数（宽松模式，少数据也能形成值）
func CalculateEMADerivative(ema []float64) []float64 {
	if len(ema) == 0 {
		return nil
	}

	derivative := make([]float64, len(ema))
	derivative[0] = 0 // 第一根没有前值，用 0 代替

	for i := 1; i < len(ema); i++ {
		derivative[i] = ema[i] - ema[i-1]
	}

	return derivative
}
