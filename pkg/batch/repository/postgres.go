// pkg/batch/repository/postgres.go
package repository

import (
	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL ドライバ (適切なドライバ)
	"sample/pkg/batch/config"
	"sample/pkg/batch/database" // database パッケージをインポート
	"sample/pkg/batch/util/exception" // exception を直接インポート
	"sample/pkg/batch/util/logger"    // logger を直接インポート
)

type PostgresRepository struct {
	db           *sql.DB
	dbConnection database.DBConnection // DBConnection インターフェースを追加
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{
		db:           db,
		dbConnection: database.NewSQLDBAdapter(db), // SQLDBAdapter でラップ
	}
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
	return &PostgresRepository{
		db:           db,
		dbConnection: database.NewSQLDBAdapter(db), // SQLDBAdapter でラップ
	}, nil
}

// GetDBConnection は JobRepository インターフェースの実装です。
func (p *PostgresRepository) GetDBConnection() database.DBConnection {
	return p.dbConnection
}
