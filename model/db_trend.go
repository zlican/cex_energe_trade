package model

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

var DBTrend *sql.DB

func InitDBTrend() {
	// 连接配置
	username := "root"
	password := "Aa123456"
	host := "127.0.0.1"
	port := 3306
	dbname := "trend_trade_mysql"

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		username, password, host, port, dbname)

	var err error
	DBTrend, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}

	// 测试连接
	if err = DBTrend.Ping(); err != nil {
		log.Fatalf("数据库 ping 失败: %v", err)
	}

	log.Println("✅ 成功连接 MySQL 数据库")
}
