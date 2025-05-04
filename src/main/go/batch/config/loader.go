package config

import (
  _ "embed"
  "fmt"
  "os"
  "strconv"

  "gopkg.in/yaml.v3"
)

//go:embed application.yaml
var embeddedConfig []byte

// LoadConfig は application.yaml ファイルを読み込み、環境変数で上書きした Config 構造体を返します。
func LoadConfig() (*Config, error) {
  cfg := NewConfig()
  
  configPath := os.Getenv("CONFIG_PATH")
  if configPath != "" {
    // 環境変数で指定されたパスのファイルを読み込む
    yamlFile, err := os.ReadFile(configPath)
    if err != nil {
      return nil, fmt.Errorf("設定ファイルの読み込みに失敗しました (%s): %w", configPath, err)
    }
    yamlCfg, err := loadYamlConfig(yamlFile)
    if err != nil {
      return nil, err
    }
    cfg.Database = yamlCfg.Database
    cfg.Batch = yamlCfg.Batch
    cfg.System = yamlCfg.System
  } else {
    // 埋め込みファイルからロード
    yamlCfg, err := loadYamlConfig(embeddedConfig)
    if err != nil {
      return nil, err
    }
    cfg.Database = yamlCfg.Database
    cfg.Batch = yamlCfg.Batch
    cfg.System = yamlCfg.System
  }
  
  // 環境変数で個別の設定値を上書き
  loadEnvVars(cfg)
  
  return cfg, nil
}

func loadYamlConfig(yamlFile []byte) (Config, error) {
  cfg := NewConfig()
  err := yaml.Unmarshal(yamlFile, cfg)
  if err != nil {
    return Config{}, fmt.Errorf("YAML のパースに失敗しました: %w", err)
  }
  return *cfg, nil
}

func loadEnvVars(cfg *Config) {
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
  if dbName := os.Getenv("DATABASE_NAME"); dbName != "" {
    cfg.Database.Database = dbName
  }
  if dbUser := os.Getenv("DATABASE_USER"); dbUser != "" {
    cfg.Database.User = dbUser
  }
  if dbPassword := os.Getenv("DATABASE_PASSWORD"); dbPassword != "" {
    cfg.Database.Password = dbPassword
  }
  if projectID := os.Getenv("PROJECT_ID"); projectID != "" {
    cfg.Database.ProjectID = projectID
  }
  if datasetID := os.Getenv("DATASET_ID"); datasetID != "" {
    cfg.Database.DatasetID = datasetID
  }
  if tableID := os.Getenv("TABLE_ID"); tableID != "" {
    cfg.Database.TableID = tableID
  }
  // 他の環境変数も同様にロード
  if batchIntervalStr := os.Getenv("BATCH_POLLING_INTERVAL_SECONDS"); batchIntervalStr != "" {
    if interval, err := strconv.Atoi(batchIntervalStr); err == nil {
      cfg.Batch.PollingIntervalSeconds = interval
    }
  }
  if apiEndpoint := os.Getenv("BATCH_API_ENDPOINT"); apiEndpoint != "" {
    cfg.Batch.APIEndpoint = apiEndpoint
  }
  if apiKey := os.Getenv("BATCH_API_KEY"); apiKey != "" {
    cfg.Batch.APIKey = apiKey
  }
  if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
    cfg.System.Logging.Level = logLevel
  }
}
