package utils

import (
	"database/sql"
	"fmt"
)

// 从数据库获取最新的趋势结果
func GetTrendResult(db *sql.DB, symbol, interval string) (string, error) {
	// 根据 interval 选择表名
	var tableName string
	switch interval {
	case "5m":
		tableName = "symbol_5m"
	case "15m":
		tableName = "symbol_15m"
	case "1h":
		tableName = "symbol_1h"
	case "4h":
		tableName = "symbol_4h"
	case "1d":
		tableName = "symbol_1d"
	case "3d":
		tableName = "symbol_3d"
	default:
		return "", fmt.Errorf("不支持的 interval: %s", interval)
	}

	query := fmt.Sprintf(`
		SELECT status
		FROM %s
		WHERE symbol = ?
		ORDER BY timestamp DESC
		LIMIT 1
	`, tableName)

	row := db.QueryRow(query, symbol)

	var status string
	if err := row.Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("没有找到数据: symbol=%s, interval=%s", symbol, interval)
		}
		return "", err
	}

	return status, nil
}
