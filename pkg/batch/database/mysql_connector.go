// pkg/batch/database/mysql_connector.go
package database

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ
	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception"
	"sample/pkg/batch/util/logger"
)

// mysqlConnector はMySQLデータベースへの接続を確立するDBConnectorの実装です。
type mysqlConnector struct{}

// Connect はMySQLデータベースへの接続を確立し、*sql.DBを返します。
func (c *mysqlConnector) Connect(cfg config.DatabaseConfig) (*sql.DB, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, exception.NewBatchError("database", "MySQL への接続に失敗しました", err, false, false)
	}

	err = db.Ping()
	if err != nil {
		db.Close() // エラー時は接続を閉じる
		return nil, exception.NewBatchError("database", "MySQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("MySQL に正常に接続しました。")
	return db, nil
}

// init 関数でmysqlConnectorを登録します。
func init() {
	RegisterConnector("mysql", &mysqlConnector{})
}
