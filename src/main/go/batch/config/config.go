package config

import (
	"fmt"
	"strings"

	core "sample/src/main/go/batch/job/core"
)

// EmbeddedConfig は、設定ファイルの内容を保持するためのフィールドです。
// main.go から渡される埋め込み設定を格納します。
type EmbeddedConfig []byte

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
	// ★ 追加: アプリケーション固有のマイグレーションファイルのパス
	AppMigrationPath string `yaml:"app_migration_path"`
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

// ItemRetryConfig はアイテムレベルのリトライ設定です。
type ItemRetryConfig struct {
	MaxAttempts        int      `yaml:"max_attempts"`
	RetryableExceptions []string `yaml:"retryable_exceptions"` // リトライ可能な例外のリスト (文字列)
}

// ItemSkipConfig はアイテムレベルのスキップ設定です。
type ItemSkipConfig struct {
	SkipLimit          int      `yaml:"skip_limit"`
	SkippableExceptions []string `yaml:"skippable_exceptions"` // スキップ可能な例外のリスト (文字列)
}

type BatchConfig struct {
	PollingIntervalSeconds int    `yaml:"polling_interval_seconds"`
	APIEndpoint            string `yaml:"api_endpoint"`
	APIKey                 string `yaml:"api_key"`
	JobName                string `yaml:"job_name"`
	Retry                  RetryConfig `yaml:"retry"`
	ChunkSize              int    `yaml:"chunk_size"` // ★ 追加
	ItemRetry              ItemRetryConfig `yaml:"item_retry"` // ★ 追加
	ItemSkip               ItemSkipConfig `yaml:"item_skip"`   // ★ 追加
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
	Database       DatabaseConfig `yaml:"database"`
	Batch          BatchConfig    `yaml:"batch"`
	System         SystemConfig   `yaml:"system"`
	EmbeddedConfig EmbeddedConfig `yaml:"-"` // 埋め込み設定を格納するためのフィールド。YAMLからは読み込まない。
}

// NewConfig は Config の新しいインスタンスを返します。
func NewConfig() *Config {
	return &Config{
		System: SystemConfig{
			Timezone: "UTC", // デフォルト値を UTC に設定
			Logging:  LoggingConfig{Level: "INFO"},
		},
		Batch: BatchConfig{
			JobName: "", // デフォルトの Job 名を空文字列に設定。アプリケーション側で設定するか、JSLからロードされることを期待。
			ChunkSize: 10, // ★ デフォルトのチャンクサイズ
			ItemRetry: ItemRetryConfig{ // デフォルトのアイテムリトライ設定
				MaxAttempts: 3,
				RetryableExceptions: []string{}, // デフォルトは空
			},
			ItemSkip: ItemSkipConfig{ // デフォルトのアイテムスキップ設定
				SkipLimit: 0, // デフォルトはスキップなし
				SkippableExceptions: []string{}, // デフォルトは空
			},
		},
	}
}

// NewJobParameters は JobParameters の新しいインスタンスを作成します。
func NewJobParameters() *core.JobParameters {
	return core.NewJobParameters()
}

// LoadConfig は loader.go で定義されているものを使用します。
// loadYamlConfig は loader.go で定義されているものを使用します。
// loadEnvVars は loader.go で定義されているものを使用します。
