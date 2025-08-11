// pkg/batch/database/database.go
package database

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ
	_ "github.com/lib/pq"              // PostgreSQL ドライバ

	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception"
	"sample/pkg/batch/util/logger"
)

// NewMySQLConnection はMySQLデータベースへの接続を確立し、*sql.DBを返します。
func NewMySQLConnection(cfg config.DatabaseConfig) (*sql.DB, error) {
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

// NewPostgresConnection はPostgreSQLデータベースへの接続を確立し、*sql.DBを返します。
func NewPostgresConnection(cfg config.DatabaseConfig) (*sql.DB, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.NewBatchError("database", "PostgreSQL への接続に失敗しました", err, false, false)
	}

	err = db.Ping()
	if err != nil {
		db.Close() // エラー時は接続を閉じる
		return nil, exception.NewBatchError("database", "PostgreSQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("PostgreSQL に正常に接続しました。")
	return db, nil
}

// NewDBConnectionFromConfig は設定に基づいて適切なデータベース接続を確立します。
func NewDBConnectionFromConfig(cfg config.DatabaseConfig) (*sql.DB, error) {
	switch cfg.Type {
	case "mysql":
		return NewMySQLConnection(cfg)
	case "postgres":
		return NewPostgresConnection(cfg)
	// 将来的に他のデータベースタイプが追加された場合、ここに追加します。
	default:
		return nil, exception.NewBatchError("database", fmt.Sprintf("未対応のデータベースタイプ: %s", cfg.Type), nil, false, false)
	}
}
