// pkg/batch/database/connector/postgres.go
package connector // パッケージ名を connector に変更

import (
	"database/sql"
	"time" // time パッケージを追加

	_ "github.com/lib/pq" // PostgreSQL ドライバ
	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception"
	"sample/pkg/batch/util/logger"
)

// postgresConnector はPostgreSQLデータベースへの接続を確立するDBConnectorの実装です。
type postgresConnector struct{}

// Connect はPostgreSQLデータベースへの接続を確立し、*sql.DBを返します。
func (c *postgresConnector) Connect(cfg config.DatabaseConfig) (*sql.DB, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.NewBatchError("database", "PostgreSQL への接続に失敗しました", err, false, false)
	}

	// 接続プール設定を適用
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeMinutes) * time.Minute)

	err = db.Ping()
	if err != nil {
		db.Close() // エラー時は接続を閉じる
		return nil, exception.NewBatchError("database", "PostgreSQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("PostgreSQL に正常に接続しました。MaxOpenConns: %d, MaxIdleConns: %d, ConnMaxLifetime: %d分", cfg.MaxOpenConns, cfg.MaxIdleConns, cfg.ConnMaxLifetimeMinutes)
	return db, nil
}

// init 関数でpostgresConnectorを登録します。
func init() {
	RegisterConnector("postgres", &postgresConnector{})
}
