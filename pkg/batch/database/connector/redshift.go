// pkg/batch/database/connector/redshift.go
package connector

import (
	"database/sql"
	"time"

	_ "github.com/lib/pq" // Redshift は PostgreSQL と互換性があるため、pq ドライバを使用
	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception"
	"sample/pkg/batch/util/logger"
)

// redshiftConnector はRedshiftデータベースへの接続を確立するDBConnectorの実装です。
type redshiftConnector struct{}

// Connect はRedshiftデータベースへの接続を確立し、*sql.DBを返します。
func (c *redshiftConnector) Connect(cfg config.DatabaseConfig) (*sql.DB, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr) // Redshift は PostgreSQL ドライバを使用
	if err != nil {
		return nil, exception.NewBatchError("database", "Redshift への接続に失敗しました", err, false, false)
	}

	// 接続プール設定を適用
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetimeMinutes) * time.Minute)

	err = db.Ping()
	if err != nil {
		db.Close() // エラー時は接続を閉じる
		return nil, exception.NewBatchError("database", "Redshift への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("Redshift に正常に接続しました。MaxOpenConns: %d, MaxIdleConns: %d, ConnMaxLifetime: %d分", cfg.MaxOpenConns, cfg.MaxIdleConns, cfg.ConnMaxLifetimeMinutes)
	return db, nil
}

// init 関数でredshiftConnectorを登録します。
func init() {
	RegisterConnector("redshift", &redshiftConnector{})
}
