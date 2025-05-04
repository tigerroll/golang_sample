package repository

import (
  "context"
  "database/sql"
  "fmt"
  "time"

  _ "github.com/lib/pq" // PostgreSQL ドライバ (適切なドライバ)
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/domain/entity"
  "sample/src/main/go/batch/util/exception" // exception を直接インポート
  "sample/src/main/go/batch/util/logger"    // logger を直接インポート
)

type RedshiftRepository struct {
  db *sql.DB
}

func NewRedshiftRepository(db *sql.DB) *RedshiftRepository {
  return &RedshiftRepository{db: db}
}

func NewRedshiftRepositoryFromConfig(cfg config.DatabaseConfig) (*RedshiftRepository, error) {
  connStr := cfg.ConnectionString() // ← ここを使用

  db, err := sql.Open("postgres", connStr)
  if err != nil {
    return nil, exception.NewBatchError("repository", "Redshift への接続に失敗しました", err)
  }

  err = db.Ping()
  if err != nil {
    return nil, exception.NewBatchError("repository", "Redshift への Ping に失敗しました", err)
  }

  logger.Infof("Redshift に正常に接続しました。")
  return &RedshiftRepository{db: db}, nil
}

// SaveWeatherData は加工済みの Open Meteo の天気予報データを PostgreSQL に保存します。
func (r *RedshiftRepository) SaveWeatherData(ctx context.Context, forecast entity.OpenMeteoForecast) error {
  tx, err := r.db.BeginTx(ctx, nil)
  if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
  }
  defer tx.Rollback()

  stmt, err := tx.PrepareContext(ctx, `
    CREATE TABLE IF NOT EXISTS hourly_forecast (
      time TIMESTAMP WITHOUT TIME ZONE,
      weather_code INTEGER,
      temperature_2m DOUBLE PRECISION,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      collected_at TIMESTAMP WITHOUT TIME ZONE
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
    VALUES ($1, $2, $3, $4, $5, $6);
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
    _, err = insertStmt.ExecContext(
      ctx,
      forecast.Hourly.Time[i],
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

  logger.Debugf("Open Meteo の天気予報データを Redshift に保存しました: 緯度=%f, 経度=%f, データ数=%d",
    forecast.Latitude, forecast.Longitude, len(forecast.Hourly.Time))
  return nil
}

func (r *RedshiftRepository) Close() error {
  if r.db != nil {
    err := r.db.Close()
    if err != nil {
      return exception.NewBatchError("repository", "Redshift の接続を閉じるのに失敗しました", err)
    }
    logger.Debugf("Redshift の接続を閉じました。")
  }
  return nil
}

var _ WeatherRepository = (*RedshiftRepository)(nil)
