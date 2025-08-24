package utils

import "math"

/* // 计算简单移动平均线：输入数据和数量，返回当下MA数值
func CalculateMA(data []float64, period int) float64 {
	if len(data) < period || period <= 0 {
		return 0
	}

	sum := 0.0
	for i := len(data) - period; i < len(data); i++ {
		sum += data[i]
	}
	return sum / float64(period)
}
*/

// CalculateMA 计算简单移动平均线：输入数据和周期，返回当前MA数值。
// - 如果数据长度 < period，则返回已有数据的均值。
// - 如果 period <= 0，则返回 NaN。
func CalculateMA(data []float64, period int) float64 {
	if period <= 0 || len(data) == 0 {
		return math.NaN()
	}

	if len(data) < period {
		// 数据不够，取全部数据的均值
		sum := 0.0
		for _, v := range data {
			sum += v
		}
		return sum / float64(len(data))
	}

	// 正常情况：取最后 period 个数据
	sum := 0.0
	for i := len(data) - period; i < len(data); i++ {
		sum += data[i]
	}
	return sum / float64(period)
}
