// src/main/go/batch/repository/postgres.go
package repository

import (
	"context" // context パッケージをインポート
	"database/sql"
	"fmt"
	// "time" // ★ 不要なインポートを削除

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
		return nil, exception.NewBatchError("repository", "PostgreSQL への接続に失敗しました", err, false, false) // ★ 修正
	}

	// Ping に Context を渡す場合は db.PingContext を使用
	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "PostgreSQL への Ping に失敗しました", err, false, false) // ★ 修正
	}

	logger.Debugf("PostgreSQL に正常に接続しました。")
	return &PostgresRepository{db: db}, nil
}

// BulkInsertWeatherData は加工済みの天気予報データアイテムのチャンクをPostgreSQLに保存します。
// このメソッドは、ItemWriterから呼び出されることを想定しています。
func (r *PostgresRepository) BulkInsertWeatherData(ctx context.Context, items []entity.WeatherDataToStore) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err() // Context が完了していたら即座に中断
	default:
	}

	if len(items) == 0 {
		return nil // 書き込むデータがない場合は何もしない
	}

	// BeginTx に Context を渡す
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // エラー時はロールバック

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
			// Context が完了したら、トランザクションをロールバックして中断
			tx.Rollback()
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

	logger.Debugf("PostgreSQL に天気データアイテム %d 件を保存しました。", len(items))
	return nil
}

func (r *PostgresRepository) Close() error {
	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			return exception.NewBatchError("repository", "PostgreSQL の接続を閉じるのに失敗しました", err, false, false) // ★ 修正
		}
		logger.Debugf("PostgreSQL の接続を閉じました。")
	}
	return nil
}

// WeatherRepository インターフェースが実装されていることを確認
// この行は、WeatherRepositoryインターフェースがこのファイルと同じパッケージ、
// またはインポート可能なパッケージで定義されていることを前提としています。
// もしWeatherRepositoryが未定義であれば、別途定義が必要です。
var _ WeatherRepository = (*PostgresRepository)(nil)

