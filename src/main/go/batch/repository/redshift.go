// src/main/go/batch/repository/redshift.go
package repository

import (
	"context" // context パッケージをインポート
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL ドライバ (Redshift も互換性があるため使用)
	"sample/src/main/go/batch/config"
	"sample/src/main/go/batch/domain/entity"
	"sample/src/main/go/batch/util/exception" // exception を直接インポート
	"sample/src/main/go/batch/util/logger"    // logger を直接インポート
)

// RedshiftRepository 型を定義
type RedshiftRepository struct {
	db *sql.DB
}

// NewRedshiftRepository 関数を定義
func NewRedshiftRepository(db *sql.DB) *RedshiftRepository {
	return &RedshiftRepository{db: db}
}

// NewRedshiftRepositoryFromConfig 関数を定義
// この関数は factory.go では使用されていませんが、Redshift 用として存在させる場合に修正
func NewRedshiftRepositoryFromConfig(cfg config.DatabaseConfig) (*RedshiftRepository, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.NewBatchError("repository", "Redshift への接続に失敗しました", err, false, false) // ★ 修正
	}

	// Ping に Context を渡す場合は db.PingContext を使用
	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "Redshift への Ping に失敗しました", err, false, false) // ★ 修正
	}

	logger.Infof("Redshift に正常に接続しました。")
	return &RedshiftRepository{db: db}, nil
}

// BulkInsertWeatherData は加工済みの天気予報データアイテムのチャンクをRedshiftに保存します。
// このメソッドは、ItemWriterから呼び出されることを想定しています。
func (r *RedshiftRepository) BulkInsertWeatherData(ctx context.Context, items []entity.WeatherDataToStore) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err() // Context が完了していたら即座に中断
	default:
	}

	if len(items) == 0 {
		return nil // 書き込むデータがない場合は何もしない
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// PrepareContext に Context を渡す
	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO hourly_forecast (time, weather_code, temperature_2m, latitude, longitude, collected_at)
		VALUES ($1, $2, $3, $4, $5, $6);
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer insertStmt.Close()

	for _, item := range items {
		// ループ内でも Context の完了を定期的にチェック
		select {
		case <-ctx.Done():
			tx.Rollback() // 中断前にロールバック
			return ctx.Err()
		default:
		}

		// ExecContext に Context を渡す
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
			return fmt.Errorf("failed to insert data for time %s: %w", item.Time, err)
		}
	}

	// Commit に Context を渡す (Go 1.15 以降)
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Debugf("Redshift に天気データアイテム %d 件を保存しました。", len(items))
	return nil
}

// Close メソッドを定義
func (r *RedshiftRepository) Close() error {
	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			return exception.NewBatchError("repository", "Redshift の接続を閉じるのに失敗しました", err, false, false) // ★ 修正
		}
		logger.Debugf("Redshift の接続を閉じました。")
	}
	return nil
}

// RedshiftRepository が WeatherRepository インターフェースを満たすことを確認
var _ WeatherRepository = (*RedshiftRepository)(nil)
