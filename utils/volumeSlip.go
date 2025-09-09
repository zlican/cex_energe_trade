package utils

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// volumeSlip 过滤掉 24H成交量小于 2000万的标的
func VolumeSlip(ticker24h []Ticker24h, symbols []string) []string {
	if len(ticker24h) == 0 {
		log.Println("24小时数据获取异常tick24h")
	}
	result := make([]string, 0, len(symbols))

	// 构造一个 map，加快查询速度
	volumeMap := make(map[string]float64, len(ticker24h))
	for _, t := range ticker24h {
		vol, err := strconv.ParseFloat(t.Volume, 64)
		if err != nil {
			continue // 忽略解析失败的
		}
		volumeMap[strings.ToUpper(t.Symbol)] = vol
	}

	// 遍历 symbols 并过滤
	for _, sym := range symbols {
		if vol, ok := volumeMap[strings.ToUpper(sym)]; ok {
			if sym == "CUDISUSDT" {
				fmt.Print(sym, vol)
			}
			if vol >= 30000000 {
				result = append(result, sym)
			}
		}
	}

	return result
}

func CheckVolume(ticker24h []Ticker24h, symbol string, vcount float64) bool {
	// 构造 map（也可以直接遍历 ticker24h，这里为了复用逻辑）
	volumeMap := make(map[string]float64, len(ticker24h))
	for _, t := range ticker24h {
		vol, err := strconv.ParseFloat(t.Volume, 64)
		if err != nil {
			continue
		}
		volumeMap[strings.ToUpper(t.Symbol)] = vol
	}

	// 检查指定 symbol
	if vol, ok := volumeMap[strings.ToUpper(symbol)]; ok {
		if vol >= vcount {
			return true
		}
		return false
	}

	// symbol 没有在 ticker24h 里找到，默认返回 false
	return false
}
