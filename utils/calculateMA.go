package utils

import "math"

func CalculateMA(data []float64, period int) float64 {
	if period <= 0 || len(data) == 0 {
		return math.NaN()
	}
	if len(data) == 1 {
		return 0
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
