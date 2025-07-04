// src/main/go/batch/repository/mysql.go
package repository

import (
	"context" // context パッケージをインポート
	"database/sql"
	"fmt"
	// "time" // ★ 不要なインポートを削除

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
		return nil, exception.NewBatchError("repository", "MySQL への接続に失敗しました", err, false, false) // ★ 修正
	}

	// Ping に Context を渡す場合は db.PingContext を使用
	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "MySQL への Ping に失敗しました", err, false, false) // ★ 修正
	}

	logger.Debugf("MySQL に正常に接続しました。")
	return &MySQLRepository{db: db}, nil
}

// BulkInsertWeatherData は加工済みの天気予報データアイテムのチャンクをMySQLに保存します。
// このメソッドは、ItemWriterから呼び出されることを想定しています。
// トランザクションは呼び出し元 (JSLAdaptedStep) から渡されるように変更します。
func (r *MySQLRepository) BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []entity.WeatherDataToStore) error { // ★ tx を追加
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err() // Context が完了していたら即座に中断
	default:
	}

	if len(items) == 0 {
		return nil // 書き込むデータがない場合は何もしない
	}

	// ★ 以下のトランザクション開始・コミット・ロールバックのロジックを削除 ★
	// tx, err := r.db.BeginTx(ctx, nil)
	// if err != nil {
	// 	return fmt.Errorf("failed to begin transaction: %w", err)
	// }
	// defer tx.Rollback() // エラー時はロールバック

	// PrepareContext に Context を渡す (tx を使用)
	// MySQL の場合は ? プレースホルダを使用
	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO hourly_forecast (time, weather_code, temperature_2m, latitude, longitude, collected_at)
		VALUES (?, ?, ?, ?, ?, ?);
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
			// ★ ここでの tx.Rollback() も削除。呼び出し元で管理される。 ★
			// tx.Rollback()
			return ctx.Err()
		default:
		}

		// ExecContext に Context を渡す (tx を使用)
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

	// ★ 以下のトランザクションコミットのロジックを削除 ★
	// if err := tx.Commit(); err != nil {
	// 	return fmt.Errorf("failed to commit transaction: %w", err)
	// }

	logger.Debugf("MySQL に天気データアイテム %d 件を保存しました。", len(items))
	return nil
}

func (r *MySQLRepository) Close() error {
	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			return exception.NewBatchError("repository", "MySQL の接続を閉じるのに失敗しました", err, false, false) // ★ 修正
		}
		logger.Debugf("MySQL の接続を閉じました。")
	}
	return nil
}

// WeatherRepository インターフェースが実装されていることを確認
// この行は、WeatherRepositoryインターフェースがこのファイルと同じパッケージ、
// またはインポート可能なパッケージで定義されていることを前提としています。
// もしWeatherRepositoryが未定義であれば、別途定義が必要です。
var _ WeatherRepository = (*MySQLRepository)(nil)
