// src/main/go/batch/repository/postgres.go
package repository

import (
  "context" // context パッケージをインポート
  "database/sql"
  "fmt"
  "time"

  _ "github.com/lib/pq" // PostgreSQL ドライバ (適切なドライバ)
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/domain/entity"
  "sample/src/main/go/batch/util/exception" // exception を直接インポート
  "sample/src/main/go/batch/util/logger"    // logger を直接インポート
)

type PostgresRepository struct {
  db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
  return &PostgresRepository{db: db}
}

func NewPostgresRepositoryFromConfig(cfg config.DatabaseConfig) (*PostgresRepository, error) {
  connStr := cfg.ConnectionString() // ← ここを使用

  db, err := sql.Open("postgres", connStr)
  if err != nil {
    return nil, exception.NewBatchError("repository", "PostgreSQL への接続に失敗しました", err)
  }

  // Ping に Context を渡す場合は db.PingContext を使用
  err = db.Ping()
  if err != nil {
    return nil, exception.NewBatchError("repository", "PostgreSQL への Ping に失敗しました", err)
  }

  logger.Debugf("PostgreSQL に正常に接続しました。")
  return &PostgresRepository{db: db}, nil
}

// SaveWeatherData は加工済みの Open Meteo の天気予報データを PostgreSQL に保存します。
// ctx context.Context を引数に追加
func (r *PostgresRepository) SaveWeatherData(ctx context.Context, forecast entity.OpenMeteoForecast) error {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return ctx.Err() // Context が完了していたら即座に中断
  default:
  }

  // BeginTx に Context を渡す
  tx, err := r.db.BeginTx(ctx, nil)
  if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
  }
  defer tx.Rollback()

  // PrepareContext に Context を渡す
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

  // ExecContext に Context を渡す
  _, err = stmt.ExecContext(ctx)
  if err != nil {
    return fmt.Errorf("failed to create table: %w", err)
  }

  // PrepareContext に Context を渡す
  insertStmt, err := tx.PrepareContext(ctx, `
    INSERT INTO hourly_forecast (time, weather_code, temperature_2m, latitude, longitude, collected_at)
    VALUES ($1, $2, $3, $4, $5, $6);
  `)
  if err != nil {
    return fmt.Errorf("failed to prepare insert statement: %w", err)
  }
  defer insertStmt.Close()

  // 設定のロードとタイムゾーン処理（この部分は Context と直接関連しませんが、既存コードに合わせて残します）
  cfg, err := config.LoadConfig()
  if err != nil {
    return fmt.Errorf("設定のロードに失敗しました: %w", err)
  }
  loc, err := time.LoadLocation(cfg.System.Timezone)
  if err != nil {
    return fmt.Errorf("タイムゾーン '%s' のロードに失敗しました: %w", cfg.System.Timezone, err)
  }

  collectedAt := time.Now().In(loc).Format(time.RFC3339) // RFC3339 形式はタイムゾーン情報を含むため TIMESTAMP WITH TIME ZONE の方が良い場合が多いですが、既存テーブル定義に合わせます

  for i := range forecast.Hourly.Time {
    // ループ内でも Context の完了を定期的にチェック
    select {
    case <-ctx.Done():
      // Context が完了したら、トランザクションをロールバックして中断
      tx.Rollback()
      return ctx.Err()
    default:
    }

    // ExecContext に Context を渡す
    _, err = insertStmt.ExecContext(
      ctx,
      forecast.Hourly.Time[i], // timestamp without time zone の場合、タイムゾーン情報を考慮する必要があるか確認
      forecast.Hourly.WeatherCode[i],
      forecast.Hourly.Temperature2M[i],
      forecast.Latitude,
      forecast.Longitude,
      collectedAt, // timestamp without time zone の場合、文字列形式のまま渡すか、time.Time に変換して渡すか確認
    )
    if err != nil {
      return fmt.Errorf("failed to insert data for time %s: %w", forecast.Hourly.Time[i], err)
    }
  }

  // Commit に Context を渡す (Go 1.15 以降)
  if err := tx.Commit(); err != nil {
    return fmt.Errorf("failed to commit transaction: %w", err)
  }

  logger.Debugf("Open Meteo の天気予報データを PostgreSQL に保存しました: 緯度=%f, 経度=%f, データ数=%d",
    forecast.Latitude, forecast.Longitude, len(forecast.Hourly.Time))
  return nil
}

func (r *PostgresRepository) Close() error {
  if r.db != nil {
    err := r.db.Close()
    if err != nil {
      return exception.NewBatchError("repository", "PostgreSQL の接続を閉じるのに失敗しました", err)
    }
    logger.Debugf("PostgreSQL の接続を閉じました。")
  }
  return nil
}

var _ WeatherRepository = (*PostgresRepository)(nil)