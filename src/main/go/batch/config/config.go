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
    return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
      c.Host, c.Port, c.Database, c.User, c.Password, c.Sslmode)
  case "mysql":
    return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
      c.User, c.Password, c.Host, c.Port, c.Database)
  default:
    return ""
  }
}

type BatchConfig struct {
  PollingIntervalSeconds int    `yaml:"polling_interval_seconds"`
  APIEndpoint            string `yaml:"api_endpoint"`
  APIKey                 string `yaml:"api_key"`
  JobName                string `yaml:"job_name"` // yaml タグを追加
}

type LoggingConfig struct {
  Level string `yaml:"level"`
}

type Config struct {
  Database DatabaseConfig `yaml:"database"`
  Batch    BatchConfig    `yaml:"batch"`
  System   SystemConfig   `yaml:"system"`
}

type SystemConfig struct {
  Timezone string        `yaml:"timezone"`
  Logging  LoggingConfig `yaml:"logging"`
}

// NewConfig は Config の新しいインスタンスを返します。
func NewConfig() *Config {
  return &Config{
    System: SystemConfig{
      Timezone: "UTC", // デフォルト値を UTC に設定
      Logging:  LoggingConfig{Level: "INFO"},
    },
    Batch: BatchConfig{
      JobName: "weather", // デフォルトの Job 名を設定
    },
  }
}
