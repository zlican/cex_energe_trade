package utils

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
)

var (
	// 缓存：按 symbol 保存 supply，避免重复请求
	cmccCache   = make(map[string]float64)
	cacheLocker sync.RWMutex
)

// GetCMCCSupply 获取某个 symbol 的流通量 (CMCCirculatingSupply)
// 逻辑：先查缓存 -> 无缓存则调用 API -> 更新缓存后返回
func GetCMCCSupply(symbol string) (float64, error) {
	// 1. 先查缓存
	cacheLocker.RLock()
	if val, ok := cmccCache[symbol]; ok {
		cacheLocker.RUnlock()
		return val, nil
	}
	cacheLocker.RUnlock()

	// 2. 请求 API
	oiURL := "https://fapi.binance.com/futures/data/openInterestHist?symbol=" + symbol + "&period=1d&limit=1"
	resp, err := http.Get(oiURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var oi []struct {
		CMCCirculatingSupply string `json:"CMCCirculatingSupply"`
	}
	if err := json.Unmarshal(b, &oi); err != nil {
		return 0, err
	}
	if len(oi) == 0 || oi[0].CMCCirculatingSupply == "" {
		return 0, errors.New("no CMCCirculatingSupply data")
	}

	supply, err := strconv.ParseFloat(oi[0].CMCCirculatingSupply, 64)
	if err != nil {
		return 0, err
	}

	// 3. 写入缓存
	cacheLocker.Lock()
	cmccCache[symbol] = supply
	cacheLocker.Unlock()

	return supply, nil
}
