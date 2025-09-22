package utils

import (
	"log"
	"strconv"
	"strings"
)

// volumeSlip 过滤掉 24H成交量小于 3000万的标的
func VolumeCMCCSlip(ticker24h []Ticker24h, symbols []string) []string {
	if len(ticker24h) == 0 {
		log.Println("24小时数据获取异常tick24h")
	}
	result := make([]string, 0, len(symbols))

	// 构造一个 map，加快查询速度
	volumeMap := make(map[string]float64, len(ticker24h))
	priceMap := make(map[string]float64, len(ticker24h))
	for _, t := range ticker24h {
		vol, err := strconv.ParseFloat(t.Volume, 64)
		pri, _ := strconv.ParseFloat(t.LastPrice, 64)
		if err != nil {
			continue // 忽略解析失败的
		}
		volumeMap[strings.ToUpper(t.Symbol)] = vol
		priceMap[strings.ToUpper(t.Symbol)] = pri

	}

	// 遍历 symbols 并过滤
	for _, sym := range symbols {
		if vol, ok := volumeMap[strings.ToUpper(sym)]; ok {
			if vol >= 30000000 { // 先过滤掉小成交量
				cmcc, err := GetCMCCSupply(sym)
				if (err != nil && strings.Contains(err.Error(), "no CMCCirculatingSupply data")) || (err == nil && cmcc == float64(0)) {
					if vol > 30000000 {
						result = append(result, sym)
					}
					continue
				} else if err != nil {
					log.Println("获取流通量错误", err)
					continue
				}
				if vol >= cmcc*priceMap[strings.ToUpper(sym)]/5 {
					result = append(result, sym)
				}
			}
		}
	}

	return result
}

func CheckVolume(ticker24h []Ticker24h, symbol string, vcount float64) bool {
	// 构造 map（也可以直接遍历 ticker24h，这里为了复用逻辑）

	//短线vcount 30000000
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

// CheckVolumeCMCC 检查单个 symbol 的成交量是否 >= CMCC/5
func CheckVolumeCMCC(ticker24h []Ticker24h, symbol string) bool {
	// 1. 查找 symbol 的 24H 成交量
	var vol, pri float64
	found := false
	for _, t := range ticker24h {
		if strings.ToUpper(t.Symbol) == strings.ToUpper(symbol) {
			v, err := strconv.ParseFloat(t.Volume, 64)
			p, err := strconv.ParseFloat(t.LastPrice, 64)
			if err != nil {
				return false // 解析失败直接 false
			}
			vol = v
			pri = p
			found = true
			break
		}
	}
	if !found {
		return false // ticker24h 里没有这个 symbol
	}

	// 2. 获取 CMCC 流通量
	cmcc, err := GetCMCCSupply(symbol)
	if (err != nil && strings.Contains(err.Error(), "no CMCCirculatingSupply data")) || (err == nil && cmcc == float64(0)) {
		if vol > 30000000 {
			return true
		}
		return false
	} else if err != nil {
		log.Println("获取流通量错误", err)
		return false
	}

	return vol >= cmcc*pri/5
}
