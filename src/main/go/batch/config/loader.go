package config

import (
  "fmt"
  "os"
  "strconv"

  "gopkg.in/yaml.v3"
)

// LoadConfigFromBytes はバイトスライスから設定をロードします。
// この関数は main.go から埋め込みファイルの内容を受け取ることを想定しています。
func LoadConfigFromBytes(data []byte) (*Config, error) {
	// Config, NewConfig は config.go で定義されているものを使用
	cfg := NewConfig()

	yamlCfg, err := loadYamlConfig(data)
	if err != nil {
		return nil, fmt.Errorf("YAML設定のパースに失敗しました: %w", err)
	}

	// ロードした設定をベース Config にコピー
	cfg.Database = yamlCfg.Database
	cfg.Batch = yamlCfg.Batch
	cfg.System = yamlCfg.System

	// 環境変数で個別の設定値を上書き
	loadEnvVars(cfg)

	return cfg, nil
}

// YAMLデータを Config 構造体にパースする関数
func loadYamlConfig(data []byte) (Config, error) {
  var cfg Config
  err := yaml.Unmarshal(data, &cfg)
  if err != nil {
    return Config{}, err // yaml.Unmarshal が返すエラーをそのまま返す
  }
  return cfg, nil
}

// 環境変数で個別の設定値を上書きする関数
func loadEnvVars(cfg *Config) {
  // Database 設定
  if dbType := os.Getenv("DATABASE_TYPE"); dbType != "" {
    cfg.Database.Type = dbType
  }
  if dbHost := os.Getenv("DATABASE_HOST"); dbHost != "" {
    cfg.Database.Host = dbHost
  }
  if dbPortStr := os.Getenv("DATABASE_PORT"); dbPortStr != "" {
    if dbPort, err := strconv.Atoi(dbPortStr); err == nil {
      cfg.Database.Port = dbPort
    }
  }
  if dbName := os.Getenv("DATABASE_DATABASE"); dbName != "" {
    cfg.Database.Database = dbName
  }
  if dbUser := os.Getenv("DATABASE_USER"); dbUser != "" {
    cfg.Database.User = dbUser
  }
  if dbPassword := os.Getenv("DATABASE_PASSWORD"); dbPassword != "" {
    cfg.Database.Password = dbPassword
  }
  if dbSSLMode := os.Getenv("DATABASE_SSLMODE"); dbSSLMode != "" {
    // フィールド名を Sslmode に修正
    cfg.Database.Sslmode = dbSSLMode
  }

  // Batch 設定
  if pollingIntervalStr := os.Getenv("BATCH_POLLING_INTERVAL_SECONDS"); pollingIntervalStr != "" {
    if pollingInterval, err := strconv.Atoi(pollingIntervalStr); err == nil {
      cfg.Batch.PollingIntervalSeconds = pollingInterval
    }
  }
  if apiEndpoint := os.Getenv("BATCH_API_ENDPOINT"); apiEndpoint != "" {
    cfg.Batch.APIEndpoint = apiEndpoint
  }
  if apiKey := os.Getenv("BATCH_API_KEY"); apiKey != "" {
    cfg.Batch.APIKey = apiKey
  }

  // BATCH_JOB_NAME 環境変数をロード (存在すれば設定ファイルの設定を上書き)
  if jobName := os.Getenv("BATCH_JOB_NAME"); jobName != "" {
    cfg.Batch.JobName = jobName
  }
  // BATCH_CHUNK_SIZE 環境変数をロード (存在すれば設定ファイルの設定を上書き)
  if chunkSizeStr := os.Getenv("BATCH_CHUNK_SIZE"); chunkSizeStr != "" {
    if chunkSize, err := strconv.Atoi(chunkSizeStr); err == nil {
      cfg.Batch.ChunkSize = chunkSize
    } else {
      fmt.Printf("警告: BATCH_CHUNK_SIZE の値 '%s' が無効です。デフォルト値または設定ファイルの値を使用します。", chunkSizeStr)
    }
  }

  // System 設定
  if logLevel := os.Getenv("SYSTEM_LOGGING_LEVEL"); logLevel != "" {
    cfg.System.Logging.Level = logLevel
  }
}

// Config, NewConfig, DatabaseConfig, BatchConfig, RetryConfig, SystemConfig, LoggingConfig
// の型定義は src/main/go/batch/config/config.go にのみ存在するようにしてください。
// このファイルからは削除します。
