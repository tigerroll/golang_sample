package config

import (
  "fmt"
  "strings"
)

type DatabaseConfig struct {
  Type      string `yaml:"type"`
  Host      string `yaml:"host"`
  Port      int    `yaml:"port"`
  Database  string `yaml:"database"`
  User      string `yaml:"user"`
  Password  string `yaml:"password"`
  Sslmode   string `yaml:"sslmode"`
  ProjectID string `yaml:"project_id"`
  DatasetID string `yaml:"dataset_id"`
  TableID   string `yaml:"table_id"`
}

func (c DatabaseConfig) ConnectionString() string {
  switch strings.ToLower(c.Type) {
  case "postgres", "redshift":
    // golang-migrate/migrate が期待する形式に合わせる
    return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
      c.User, c.Password, c.Host, c.Port, c.Database, c.Sslmode)
  case "mysql":
    return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
      c.User, c.Password, c.Host, c.Port, c.Database)
  default:
    return ""
  }
}

// RetryConfig は既に存在し、RetryListener に渡すのに適しています。
type RetryConfig struct {
  MaxAttempts            int `yaml:"max_attempts"`
  InitialInterval        int `yaml:"initial_interval"`
  MaxInterval            int `yaml:"max_interval"`
  Factor                 float64 `yaml:"factor"`
  CircuitBreakerThreshold int `yaml:"circuit_breaker_threshold"`
  CircuitBreakerResetInterval int `yaml:"circuit_breaker_reset_interval"`
}

type BatchConfig struct {
  PollingIntervalSeconds int    `yaml:"polling_interval_seconds"`
  APIEndpoint            string `yaml:"api_endpoint"`
  APIKey                 string `yaml:"api_key"`
  JobName                string `yaml:"job_name"`
  Retry                  RetryConfig `yaml:"retry"`
  ChunkSize              int    `yaml:"chunk_size"` // ★ 追加
}

// LoggingConfig は既に存在し、LoggingListener に渡すのに適しています。
type LoggingConfig struct {
  Level string `yaml:"level"`
}

type SystemConfig struct {
  Timezone string        `yaml:"timezone"`
  Logging  LoggingConfig `yaml:"logging"`
}

type Config struct {
  Database DatabaseConfig `yaml:"database"`
  Batch    BatchConfig    `yaml:"batch"`
  System   SystemConfig   `yaml:"system"`
}

// WeatherReader に必要な設定のみを持つ構造体
type WeatherReaderConfig struct {
  APIEndpoint string
  APIKey      string
}

// WeatherProcessor に必要な設定のみを持つ構造体 (現時点ではなし、必要に応じて追加)
// type WeatherProcessorConfig struct {}

// WeatherWriter に必要な設定のみを持つ構造体 (現時点ではなし、Repository に依存)
// type WeatherWriterConfig struct {}


// NewConfig は Config の新しいインスタンスを返します。
func NewConfig() *Config {
  return &Config{
    System: SystemConfig{
      Timezone: "UTC", // デフォルト値を UTC に設定
      Logging:  LoggingConfig{Level: "INFO"},
    },
    Batch: BatchConfig{
      JobName: "weather", // デフォルトの Job 名を設定
      ChunkSize: 10, // ★ デフォルトのチャンクサイズ
    },
  }
}

// LoadConfig は loader.go で定義されているものを使用します。
// loadYamlConfig は loader.go で定義されているものを使用します。
// loadEnvVars は loader.go で定義されているものを使用します。
