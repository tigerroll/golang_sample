// pkg/batch/repository/redshift.go
package repository

import (
	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL ドライバ (Redshift も互換性があるため使用)
	"github.com/tigerroll/go_sample/pkg/batch/config"
	"github.com/tigerroll/go_sample/pkg/batch/util/exception" // exception を直接インポート
	"github.com/tigerroll/go_sample/pkg/batch/util/logger"    // logger を直接インポート
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

// RedshiftRepository が WeatherRepository インターフェースを満たすことを確認
/* var _ weather_repo.WeatherRepository = (*RedshiftRepository)(nil) */
