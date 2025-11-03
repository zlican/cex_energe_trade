package utils

import (
	"io"
	"log"
	"os"
)

var progressLogger = log.New(os.Stdout, "[UTILS] ", log.LstdFlags|log.Lshortfile)

// SetLoggerOutput 允许外部指定日志输出位置（例如文件+控制台）
func SetLoggerOutput(w io.Writer) {
	progressLogger.SetOutput(w)
}
