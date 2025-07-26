package config

import (
  "fmt"
  "os"
  "strconv"

  "gopkg.in/yaml.v3"
)

// BytesConfigLoader はバイトスライスから設定をロードする ConfigLoader の実装です。
type BytesConfigLoader struct {
	data []byte
}

// NewBytesConfigLoader は新しい BytesConfigLoader のインスタンスを作成します。
func NewBytesConfigLoader(data []byte) *BytesConfigLoader {
	return &BytesConfigLoader{data: data}
}

// Load は埋め込まれたバイトスライスから設定をロードします。
func (l *BytesConfigLoader) Load() (*Config, error) {
	// Config, NewConfig は config.go で定義されているものを使用
	cfg := NewConfig()

	yamlCfg, err := loadYamlConfig(l.data)
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
  // ★ 追加: コネクションプール設定
  if maxOpenConnsStr := os.Getenv("DATABASE_MAX_OPEN_CONNS"); maxOpenConnsStr != "" {
    if maxOpenConns, err := strconv.Atoi(maxOpenConnsStr); err == nil {
      cfg.Database.ConnectionPool.MaxOpenConns = maxOpenConns
    } else {
      fmt.Printf("警告: DATABASE_MAX_OPEN_CONNS の値 '%s' が無効です。デフォルト値または設定ファイルの値を使用します。", maxOpenConnsStr)
    }
  }
  if maxIdleConnsStr := os.Getenv("DATABASE_MAX_IDLE_CONNS"); maxIdleConnsStr != "" {
    if maxIdleConns, err := strconv.Atoi(maxIdleConnsStr); err == nil {
      cfg.Database.ConnectionPool.MaxIdleConns = maxIdleConns
    } else {
      fmt.Printf("警告: DATABASE_MAX_IDLE_CONNS の値 '%s' が無効です。デフォルト値または設定ファイルの値を使用します。", maxIdleConnsStr)
    }
  }
  if connMaxLifetimeStr := os.Getenv("DATABASE_CONN_MAX_LIFETIME_SECONDS"); connMaxLifetimeStr != "" {
    if connMaxLifetime, err := strconv.Atoi(connMaxLifetimeStr); err == nil {
      cfg.Database.ConnectionPool.ConnMaxLifetimeSeconds = connMaxLifetime
    } else {
      fmt.Printf("警告: DATABASE_CONN_MAX_LIFETIME_SECONDS の値 '%s' が無効です。デフォルト値または設定ファイルの値を使用します。", connMaxLifetimeStr)
    }
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
// の型定義は pkg/batch/config/config.go にのみ存在するようにしてください。
// このファイルからは削除します。
