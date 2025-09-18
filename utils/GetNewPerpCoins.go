package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"
)

// 定义结构
type SymbolInfo struct {
	Symbol      string `json:"symbol"`
	OnboardDate int64  `json:"onboardDate"`
}

type ExchangeInfo struct {
	Symbols []SymbolInfo `json:"symbols"`
}

func StartNewPereFetcher(ch chan<- []string) {
	go func() {
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()

		for {
			symbols := GetNewPerpCoins()
			// 防止阻塞
			select {
			case ch <- symbols:
			default:
				fmt.Println("Warning: channel blocked, skipping update")
			}

			<-ticker.C
		}
	}()
}

func GetNewPerpCoins() []string {
	resp, err := http.Get("https://fapi.binance.com/fapi/v1/exchangeInfo")
	if err != nil {
		log.Fatalf("http get failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var ex ExchangeInfo
	if err := json.Unmarshal(body, &ex); err != nil {
		log.Fatalf("json unmarshal failed: %v", err)
	}

	// 筛选以 USDT 结尾的合约
	var usdtSymbols []SymbolInfo
	for _, s := range ex.Symbols {
		if strings.HasSuffix(s.Symbol, "USDT") {
			usdtSymbols = append(usdtSymbols, s)
		}
	}

	// 按 onboardDate 倒序
	sort.Slice(usdtSymbols, func(i, j int) bool {
		return usdtSymbols[i].OnboardDate > usdtSymbols[j].OnboardDate
	})

	limit := 10
	if len(usdtSymbols) < limit {
		limit = len(usdtSymbols)
	}

	// 只要 symbol
	var result []string
	for i := 0; i < limit; i++ {
		result = append(result, usdtSymbols[i].Symbol)
	}

	return result
}
