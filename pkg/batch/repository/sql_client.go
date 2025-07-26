package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tigerroll/go_sample/pkg/batch/config"
	"github.com/tigerroll/go_sample/pkg/batch/util/exception"
	"github.com/tigerroll/go_sample/pkg/batch/util/logger"
)

// SQLClient はデータベース操作のためのインターフェースです。
// 実際の *sql.DB をラップし、コネクションプール設定などを管理します。
type SQLClient interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	PingContext(ctx context.Context) error
	Close() error
	// GetDB は、必要に応じて基盤となる *sql.DB インスタンスを公開します。
	// これは、migrate ライブラリのように *sql.DB を直接必要とする外部ライブラリとの互換性のために提供されます。
	GetDB() *sql.DB
}

// DefaultSQLClient は SQLClient インターフェースのデフォルト実装です。
type DefaultSQLClient struct {
	db *sql.DB
}

// NewSQLClient は新しい DefaultSQLClient のインスタンスを作成します。
// データベース接続とコネクションプール設定を適用します。
func NewSQLClient(db *sql.DB, poolConfig config.ConnectionPoolConfig) (SQLClient, error) {
	if db == nil {
		return nil, exception.NewBatchErrorf("sql_client", "sql.DB インスタンスが nil です")
	}

	// コネクションプール設定の適用
	if poolConfig.MaxOpenConns > 0 {
		db.SetMaxOpenConns(poolConfig.MaxOpenConns)
		logger.Debugf("SQLClient: MaxOpenConns を %d に設定しました。", poolConfig.MaxOpenConns)
	}
	if poolConfig.MaxIdleConns > 0 {
		db.SetMaxIdleConns(poolConfig.MaxIdleConns)
		logger.Debugf("SQLClient: MaxIdleConns を %d に設定しました。", poolConfig.MaxIdleConns)
	}
	if poolConfig.ConnMaxLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(poolConfig.ConnMaxLifetimeSeconds) * time.Second)
		logger.Debugf("SQLClient: ConnMaxLifetime を %d秒 に設定しました。", poolConfig.ConnMaxLifetimeSeconds)
	}

	return &DefaultSQLClient{db: db}, nil
}

// BeginTx はトランザクションを開始します。
func (c *DefaultSQLClient) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.db.BeginTx(ctx, opts)
}

// ExecContext はクエリを実行します。
func (c *DefaultSQLClient) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// QueryRowContext は単一の行を返します。
func (c *DefaultSQLClient) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

// QueryContext は複数の行を返します。
func (c *DefaultSQLClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// PingContext はデータベースへの接続を確認します。
func (c *DefaultSQLClient) PingContext(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Close はデータベース接続を閉じます。
func (c *DefaultSQLClient) Close() error {
	logger.Debugf("SQLClient: データベース接続を閉じます。")
	return c.db.Close()
}

// GetDB は基盤となる *sql.DB インスタンスを返します。
func (c *DefaultSQLClient) GetDB() *sql.DB {
	return c.db
}

// DefaultSQLClient が SQLClient インターフェースを満たすことを確認
var _ SQLClient = (*DefaultSQLClient)(nil)
