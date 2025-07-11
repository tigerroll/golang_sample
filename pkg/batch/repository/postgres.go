// pkg/batch/repository/postgres.go
package repository

import (
	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL ドライバ (適切なドライバ)
	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception" // exception を直接インポート
	"sample/pkg/batch/util/logger"    // logger を直接インポート
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func NewPostgresRepositoryFromConfig(cfg config.DatabaseConfig) (*PostgresRepository, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.NewBatchError("repository", "PostgreSQL への接続に失敗しました", err, false, false)
	}

	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "PostgreSQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("PostgreSQL に正常に接続しました。")
	return &PostgresRepository{db: db}, nil
}

// WeatherRepository インターフェースが実装されていることを確認
// この行は、WeatherRepositoryインターフェースがこのファイルと同じパッケージ、
// またはインポート可能なパッケージで定義されていることを前提としています。
// もしWeatherRepositoryが未定義であれば、別途定義が必要です。
/* var _ weather_repo.WeatherRepository = (*PostgresRepository)(nil) */
