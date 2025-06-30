// src/main/go/batch/repository/redshift.go
package repository

import (
	"context" // context パッケージをインポート
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // PostgreSQL ドライバ (Redshift も互換性があるため使用)
	"sample/src/main/go/batch/config"
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
func NewRedshiftRepositoryFromConfig(cfg config.DatabaseConfig) (*RedshiftRepository, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.NewBatchError("repository", "Redshift への接続に失敗しました", err, false, false)
	}

	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "Redshift への Ping に失敗しました", err, false, false)
	}

	logger.Infof("Redshift に正常に接続しました。")
	return &RedshiftRepository{db: db}, nil
}

// Close メソッドを定義
func (r *RedshiftRepository) Close() error {
	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			return exception.NewBatchError("repository", "Redshift の接続を閉じるのに失敗しました", err, false, false)
		}
		logger.Debugf("Redshift の接続を閉じました。")
	}
	return nil
}

// RedshiftRepository が WeatherRepository インターフェースを満たすことを確認
/* var _ weather_repo.WeatherRepository = (*RedshiftRepository)(nil) */
