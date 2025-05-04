package repository

import (
  "context"
  "database/sql"
  "fmt"
  "time"

  _ "github.com/go-sql-driver/mysql" // MySQL ドライバ (適切なドライバ)
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/domain/entity"
  "sample/src/main/go/batch/util/logger" // logger を直接インポート
  "sample/src/main/go/batch/util/exception" // exception を直接インポート
)

type MySQLRepository struct {
  db *sql.DB
}

func NewMySQLRepository(db *sql.DB) *MySQLRepository {
  return &MySQLRepository{db: db}
}

func NewMySQLRepositoryFromConfig(cfg config.DatabaseConfig) (*MySQLRepository, error) {
  connStr := cfg.ConnectionString()

  db, err := sql.Open("mysql", connStr)
  if err != nil {
    return nil, exception.NewBatchError("repository", "MySQL への接続に失敗しました", err)
  }

  err = db.Ping()
  if err != nil {
    return nil, exception.NewBatchError("repository", "MySQL への Ping に失敗しました", err)
  }

  logger.Debugf("MySQL に正常に接続しました。")
  return &MySQLRepository{db: db}, nil
}

// SaveWeatherData は加工済みの Open Meteo の天気予報データを MySQL に保存します。
func (r *MySQLRepository) SaveWeatherData(ctx context.Context, forecast entity.OpenMeteoForecast) error {
  tx, err := r.db.BeginTx(ctx, nil)
  if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
  }
  defer tx.Rollback()

  stmt, err := tx.PrepareContext(ctx, `
    CREATE TABLE IF NOT EXISTS hourly_forecast (
      time TIMESTAMP,
      weather_code INTEGER,
      temperature_2m DOUBLE PRECISION,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      collected_at TIMESTAMP
    );
  `)
  if err != nil {
    return fmt.Errorf("failed to prepare table creation statement: %w", err)
  }
  defer stmt.Close()

  _, err = stmt.ExecContext(ctx)
  if err != nil {
    return fmt.Errorf("failed to create table: %w", err)
  }

  insertStmt, err := tx.PrepareContext(ctx, `
    INSERT INTO hourly_forecast (time, weather_code, temperature_2m, latitude, longitude, collected_at)
    VALUES (?, ?, ?, ?, ?, ?);
  `)
  if err != nil {
    return fmt.Errorf("failed to prepare insert statement: %w", err)
  }
  defer insertStmt.Close()

  cfg, err := config.LoadConfig() // 設定をロード
  if err != nil {
    return fmt.Errorf("設定のロードに失敗しました: %w", err)
  }
  loc, err := time.LoadLocation(cfg.System.Timezone) // タイムゾーンを取得
  if err != nil {
    return fmt.Errorf("タイムゾーン '%s' のロードに失敗しました: %w", cfg.System.Timezone, err)
  }

  collectedAt := time.Now().In(loc).Format(time.RFC3339) // 現在時刻を取得し、指定されたタイムゾーンに変換

  for i := range forecast.Hourly.Time {
    parsedTime, err := time.Parse(time.RFC3339, forecast.Hourly.Time[i])
    if err != nil {
      parsedTime, err = time.Parse("2006-01-02T15:04", forecast.Hourly.Time[i])
      if err != nil {
        return fmt.Errorf("failed to parse time '%s': %w", forecast.Hourly.Time[i], err)
      }
    }

    _, err = insertStmt.ExecContext(
      ctx,
      parsedTime,
      forecast.Hourly.WeatherCode[i],
      forecast.Hourly.Temperature2M[i],
      forecast.Latitude,
      forecast.Longitude,
      collectedAt,
    )
    if err != nil {
      return fmt.Errorf("failed to insert data for time %s: %w", forecast.Hourly.Time[i], err)
    }
  }

  if err := tx.Commit(); err != nil {
    return fmt.Errorf("failed to commit transaction: %w", err)
  }

  logger.Debugf("Open Meteo の天気予報データを MySQL に保存しました: 緯度=%f, 経度=%f, データ数=%d",
    forecast.Latitude, forecast.Longitude, len(forecast.Hourly.Time))
  return nil
}

func (r *MySQLRepository) Close() error {
  if r.db != nil {
    err := r.db.Close()
    if err != nil {
      return exception.NewBatchError("repository", "MySQL の接続を閉じるのに失敗しました", err)
    }
    logger.Debugf("MySQL の接続を閉じました。")
  }
  return nil
}

var _ WeatherRepository = (*MySQLRepository)(nil)
