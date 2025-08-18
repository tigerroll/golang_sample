package repository

import (
	"context" // context パッケージをインポート
	_ "database/sql" // sql.Stmt のためにブランクインポート
	"fmt"

	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception"
	"sample/pkg/batch/util/logger" // logger パッケージをインポート
	batchRepo "sample/pkg/batch/repository/job" // 汎用リポジトリをインポート (エイリアスを batchRepo に変更)

	"sample/pkg/batch/database" // database パッケージをインポート
	weather_entity "sample/example/weather/domain/entity"
)

type WeatherRepository interface {
	// BulkInsertWeatherData にトランザクションを渡すように変更
	BulkInsertWeatherData(ctx context.Context, tx database.Tx, items []weather_entity.WeatherDataToStore) error // tx を database.Tx に変更
	// Close メソッドも Context を受け取るように変更することも検討
	Close() error
	// 他のデータアクセス操作 (GetWeatherData など) があれば定義
}

// NewWeatherRepository は WeatherRepository を生成する関数です。
// この関数は weather アプリケーション固有のリポジトリファクトリとして機能します。
// main.go から呼ばれることを想定。
func NewWeatherRepository(ctx context.Context, cfg config.Config, jobRepo batchRepo.JobRepository) (WeatherRepository, error) { // ★ 変更: db を jobRepo に変更
	module := "weather_repository_factory"
	logger.Debugf("WeatherRepository の生成を開始します (Type: %s).", cfg.Database.Type)

	switch cfg.Database.Type {
	case "postgres", "redshift":
		return NewPostgresWeatherRepository(jobRepo), nil // ★ 変更: db の代わりに jobRepo を渡す
	case "mysql":
		return NewMySQLWeatherRepository(jobRepo), nil // ★ 変更: db の代わりに jobRepo を渡す
	default:
		errMsg := fmt.Sprintf("サポートされていないデータベースタイプです: %s", cfg.Database.Type)
		logger.Errorf("%s", errMsg)
		return nil, exception.NewBatchError(module, errMsg, nil, false, false)
	}
}

// PostgresRepositoryWrapper は repository.PostgresRepository を WeatherRepository として適応させます。
type PostgresRepositoryWrapper struct {
	jobRepo batchRepo.JobRepository // ★ 変更: *repository.PostgresRepository の代わりに JobRepository を保持
}

func (w *PostgresRepositoryWrapper) BulkInsertWeatherData(ctx context.Context, tx database.Tx, items []weather_entity.WeatherDataToStore) error { // tx を database.Tx に変更
	if len(items) == 0 {
		return nil
	}

	// トランザクションが渡されるので、それを使用する
	// PrepareContext は tx から呼び出す
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
	// 基盤の JobRepository はフレームワークが管理するため、ここでは閉じない
	return nil
}

// MySQLRepositoryWrapper は repository.MySQLRepository を WeatherRepository として適応させます。
type MySQLRepositoryWrapper struct {
	jobRepo batchRepo.JobRepository // ★ 変更: *repository.MySQLRepository の代わりに JobRepository を保持
}

func (w *MySQLRepositoryWrapper) BulkInsertWeatherData(ctx context.Context, tx database.Tx, items []weather_entity.WeatherDataToStore) error { // tx を database.Tx に変更
	if len(items) == 0 {
		return nil
	}

	// トランザクションが渡されるので、それを使用する
	// PrepareContext は tx から呼び出す
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
	// 基盤の JobRepository はフレームワークが管理するため、ここでは閉じない
	return nil
}

// NewPostgresWeatherRepository は JobRepository をラップして WeatherRepository を返します。
func NewPostgresWeatherRepository(repo batchRepo.JobRepository) WeatherRepository { // ★ 変更: *sql.DB の代わりに batchRepo.JobRepository を受け取る
	return &PostgresRepositoryWrapper{jobRepo: repo} // ★ 変更: repo を直接保持
}

// NewMySQLWeatherRepository は JobRepository をラップして WeatherRepository を返します。
func NewMySQLWeatherRepository(repo batchRepo.JobRepository) WeatherRepository { // ★ 変更: *sql.DB の代わりに batchRepo.JobRepository を受け取る
	return &MySQLRepositoryWrapper{jobRepo: repo} // ★ 変更: repo を直接保持
}
