package logger

import (
  "fmt"
  "log"
  //"os"
  "strings"
)

// LogLevel はログのレベルを表す型です。
type LogLevel int

const (
  LevelDebug LogLevel = iota
  LevelInfo
  LevelWarn
  LevelError
  LevelFatal
)

var logLevel = LevelInfo

// SetLogLevel はログレベルを設定します。
func SetLogLevel(level string) {
  switch strings.ToUpper(level) {
  case "DEBUG":
    logLevel = LevelDebug
  case "INFO":
    logLevel = LevelInfo
  case "WARN":
    logLevel = LevelWarn
  case "ERROR":
    logLevel = LevelError
  case "FATAL":
    logLevel = LevelFatal
  default:
    fmt.Printf("警告: 不明なログレベル '%s' が指定されました。INFO レベルで続行します。\n", level)
    logLevel = LevelInfo
  }
}

// Debugf は DEBUG レベルのログを出力します。
func Debugf(format string, v ...interface{}) {
  if logLevel <= LevelDebug {
    log.Printf("[DEBUG] "+format, v...)
  }
}

// Infof は INFO レベルのログを出力します。
func Infof(format string, v ...interface{}) {
  if logLevel <= LevelInfo {
    log.Printf("[INFO] "+format, v...)
  }
}

// Warnf は WARN レベルのログを出力します。
func Warnf(format string, v ...interface{}) {
  if logLevel <= LevelWarn {
    log.Printf("[WARN] "+format, v...)
  }
}

// Errorf は ERROR レベルのログを出力します。
func Errorf(format string, v ...interface{}) {
  if logLevel <= LevelError {
    log.Printf("[ERROR] "+format, v...)
  }
}

// Fatalf は FATAL レベルのログを出力し、プログラムを終了します。
func Fatalf(format string, v ...interface{}) {
  log.Fatalf("[FATAL] "+format, v...)
}
