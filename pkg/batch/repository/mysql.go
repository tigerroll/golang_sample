// pkg/batch/repository/mysql.go
package repository

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ (適切なドライバ)
	"sample/pkg/batch/config"
	"sample/pkg/batch/database" // database パッケージをインポート
	"sample/pkg/batch/util/logger" // logger を直接インポート
	"sample/pkg/batch/util/exception" // exception を直接インポート
)

type MySQLRepository struct {
	db           *sql.DB
	dbConnection database.DBConnection // DBConnection インターフェースを追加
}

func NewMySQLRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{
		db:           db,
		dbConnection: database.NewSQLDBAdapter(db), // SQLDBAdapter でラップ
	}
}

func NewMySQLRepositoryFromConfig(cfg config.DatabaseConfig) (*MySQLRepository, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, exception.NewBatchError("repository", "MySQL への接続に失敗しました", err, false, false)
	}

	// Ping に Context を渡す場合は db.PingContext を使用
	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "MySQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("MySQL に正常に接続しました。")
	return &MySQLRepository{
		db:           db,
		dbConnection: database.NewSQLDBAdapter(db), // SQLDBAdapter でラップ
	}, nil
}

// GetDBConnection は JobRepository インターフェースの実装です。
func (m *MySQLRepository) GetDBConnection() database.DBConnection {
	return m.dbConnection
}
