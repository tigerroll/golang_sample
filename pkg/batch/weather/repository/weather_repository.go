package weather_repository

import (
	"context" // context パッケージをインポート
	"database/sql" // sql パッケージをインポート
	"fmt"

	"github.com/tigerroll/go_sample/pkg/batch/config"
	weather_entity "github.com/tigerroll/go_sample/pkg/batch/weather/domain/entity"
	"github.com/tigerroll/go_sample/pkg/batch/util/exception"
	"github.com/tigerroll/go_sample/pkg/batch/util/logger"
	"github.com/tigerroll/go_sample/pkg/batch/repository" // 汎用リポジトリをインポート
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
func NewWeatherRepository(ctx context.Context, cfg config.Config, client repository.SQLClient) (WeatherRepository, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
	module := "weather_repository_factory"
	logger.Debugf("WeatherRepository の生成を開始します (Type: %s).", cfg.Database.Type)

	switch cfg.Database.Type {
	case "postgres", "redshift":
		return NewPostgresWeatherRepository(client), nil // ★ 変更: db から client に変更
	case "mysql":
		return NewMySQLWeatherRepository(client), nil // ★ 変更: db から client に変更
	default:
		errMsg := fmt.Sprintf("サポートされていないデータベースタイプです: %s", cfg.Database.Type)
		logger.Errorf("%s", errMsg)
		return nil, exception.NewBatchError(module, errMsg, nil, false, false)
	}
}

// PostgresRepositoryWrapper は repository.PostgresRepository を WeatherRepository として適応させます。
type PostgresRepositoryWrapper struct {
	client repository.SQLClient // ★ 変更: *repository.PostgresRepository から repository.SQLClient に変更
}

func (w *PostgresRepositoryWrapper) BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []weather_entity.WeatherDataToStore) error {
	if len(items) == 0 {
		return nil
	}

	// tx.PrepareContext を使用するため、直接 *sql.DB は不要
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
	// 基盤の SQLClient は JobRepository が管理するため、ここでは閉じない
	return nil
}

// MySQLRepositoryWrapper は repository.MySQLRepository を WeatherRepository として適応させます。
type MySQLRepositoryWrapper struct {
	client repository.SQLClient // ★ 変更: *repository.MySQLRepository から repository.SQLClient に変更
}

func (w *MySQLRepositoryWrapper) BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []weather_entity.WeatherDataToStore) error {
	if len(items) == 0 {
		return nil
	}

	// tx.PrepareContext を使用するため、直接 *sql.DB は不要
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
	// 基盤の SQLClient は JobRepository が管理するため、ここでは閉じない
	return nil
}

// NewPostgresWeatherRepository は既存の repository.PostgresRepository をラップして WeatherRepository を返します。
func NewPostgresWeatherRepository(client repository.SQLClient) WeatherRepository { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
	return &PostgresRepositoryWrapper{client: client} // ★ 変更: repository.NewPostgresRepository(db) から client に変更
}

// NewMySQLWeatherRepository は既存の repository.MySQLRepository をラップして WeatherRepository を返します。
func NewMySQLWeatherRepository(client repository.SQLClient) WeatherRepository { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
	return &MySQLRepositoryWrapper{client: client} // ★ 変更: repository.NewMySQLRepository(db) から client に変更
}
