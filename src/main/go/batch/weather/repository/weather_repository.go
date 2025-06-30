package weather_repository

import (
	"context" // context パッケージをインポート
	"database/sql" // sql パッケージをインポート
	"fmt"

	"sample/src/main/go/batch/config"
	weather_entity "sample/src/main/go/batch/weather/domain/entity"
	"sample/src/main/go/batch/util/exception"
	"sample/src/main/go/batch/util/logger"
	"sample/src/main/go/batch/repository" // 汎用リポジトリをインポート
)

type WeatherRepository interface {
	// BulkInsertWeatherData にトランザクションを渡すように変更
	BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []weather_entity.WeatherDataToStore) error
	// Close メソッドも Context を受け取るように変更することも検討
	Close() error
	// 他のデータアクセス操作 (GetWeatherData など) があれば定義
}

// NewWeatherRepository は WeatherRepository を生成する関数です。
// この関数は weather アプリケーション固有のリポジトリファクトリとして機能します。
// main.go から呼ばれることを想定。
func NewWeatherRepository(ctx context.Context, cfg config.Config, db *sql.DB) (WeatherRepository, error) {
	module := "weather_repository_factory"
	logger.Debugf("WeatherRepository の生成を開始します (Type: %s).", cfg.Database.Type)

	switch cfg.Database.Type {
	case "postgres", "redshift":
		return NewPostgresWeatherRepository(db), nil
	case "mysql":
		return NewMySQLWeatherRepository(db), nil
	default:
		errMsg := fmt.Sprintf("サポートされていないデータベースタイプです: %s", cfg.Database.Type)
		logger.Errorf("%s", errMsg)
		return nil, exception.NewBatchError(module, errMsg, nil, false, false)
	}
}

// PostgresRepositoryWrapper は repository.PostgresRepository を WeatherRepository として適応させます。
type PostgresRepositoryWrapper struct {
	*repository.PostgresRepository
}

func (w *PostgresRepositoryWrapper) BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []weather_entity.WeatherDataToStore) error {
	if len(items) == 0 {
		return nil
	}

	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO hourly_forecast (time, weather_code, temperature_2m, latitude, longitude, collected_at)
		VALUES ($1, $2, $3, $4, $5, $6);
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement for hourly_forecast: %w", err)
	}
	defer insertStmt.Close()

	for _, item := range items {
		// ループ内でも Context の完了を定期的にチェック
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err = insertStmt.ExecContext(
			ctx,
			item.Time,
			item.WeatherCode,
			item.Temperature2M,
			item.Latitude,
			item.Longitude,
			item.CollectedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert hourly_forecast data for time %s: %w", item.Time, err)
		}
	}
	logger.Debugf("PostgresRepositoryWrapper: hourly_forecast に天気データアイテム %d 件を保存しました。", len(items))
	return nil
}

func (w *PostgresRepositoryWrapper) Close() error {
	return w.PostgresRepository.Close()
}

// MySQLRepositoryWrapper は repository.MySQLRepository を WeatherRepository として適応させます。
type MySQLRepositoryWrapper struct {
	*repository.MySQLRepository
}

func (w *MySQLRepositoryWrapper) BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []weather_entity.WeatherDataToStore) error {
	if len(items) == 0 {
		return nil
	}

	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO hourly_forecast (time, weather_code, temperature_2m, latitude, longitude, collected_at)
		VALUES (?, ?, ?, ?, ?, ?);
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement for hourly_forecast: %w", err)
	}
	defer insertStmt.Close()

	for _, item := range items {
		// ループ内でも Context の完了を定期的にチェック
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err = insertStmt.ExecContext(
			ctx,
			item.Time,
			item.WeatherCode,
			item.Temperature2M,
			item.Latitude,
			item.Longitude,
			item.CollectedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert hourly_forecast data for time %s: %w", item.Time, err)
		}
	}
	logger.Debugf("MySQLRepositoryWrapper: hourly_forecast に天気データアイテム %d 件を保存しました。", len(items))
	return nil
}

func (w *MySQLRepositoryWrapper) Close() error {
	return w.MySQLRepository.Close()
}

// NewPostgresWeatherRepository は既存の repository.PostgresRepository をラップして WeatherRepository を返します。
func NewPostgresWeatherRepository(db *sql.DB) WeatherRepository {
	return &PostgresRepositoryWrapper{repository.NewPostgresRepository(db)}
}

// NewMySQLWeatherRepository は既存の repository.MySQLRepository をラップして WeatherRepository を返します。
func NewMySQLWeatherRepository(db *sql.DB) WeatherRepository {
	return &MySQLRepositoryWrapper{repository.NewMySQLRepository(db)}
}
