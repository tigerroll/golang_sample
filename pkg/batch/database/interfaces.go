package database

import (
	"context"
	"database/sql"

	"sample/pkg/batch/config"
	"sample/pkg/batch/database/connector" // 新しく追加
)

// Tx はデータベーストランザクションのインターフェースです。
// sql.Tx の必要なメソッドを抽象化します。
type Tx interface {
	Commit() error
	Rollback() error
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// DBConnection はデータベース接続のインターフェースです。
// sql.DB の必要なメソッドを抽象化します。
type DBConnection interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
	Close() error
	PingContext(ctx context.Context) error
	// 追加: 直接的なクエリ実行メソッド
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// sqlTxAdapter は sql.Tx を database.Tx インターフェースに適合させるアダプターです。
type sqlTxAdapter struct {
	*sql.Tx
}

// Commit は sql.Tx の Commit メソッドを呼び出します。
func (a *sqlTxAdapter) Commit() error {
	return a.Tx.Commit()
}

// Rollback は sql.Tx の Rollback メソッドを呼び出します。
func (a *sqlTxAdapter) Rollback() error {
	return a.Tx.Rollback()
}

// PrepareContext は sql.Tx の PrepareContext メソッドを呼び出します。
func (a *sqlTxAdapter) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return a.Tx.PrepareContext(ctx, query)
}

// ExecContext は sql.Tx の ExecContext メソッドを呼び出します。
func (a *sqlTxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.Tx.ExecContext(ctx, query, args...)
}

// QueryContext は sql.Tx の QueryContext メソッドを呼び出します。
func (a *sqlTxAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.Tx.QueryContext(ctx, query, args...)
}

// QueryRowContext は sql.Tx の QueryRowContext メソッドを呼び出します。
func (a *sqlTxAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.Tx.QueryRowContext(ctx, query, args...) // 修正: args を渡す
}

// sqlDBAdapter は sql.DB を database.DBConnection インターフェースに適合させるアダプターです。
type sqlDBAdapter struct {
	db *sql.DB
}

// NewSQLDBAdapter は新しい sqlDBAdapter のインスタンスを作成します。
func NewSQLDBAdapter(db *sql.DB) DBConnection {
	return &sqlDBAdapter{db: db}
}

// BeginTx は sql.DB の BeginTx メソッドを呼び出し、結果を database.Tx でラップします。
func (a *sqlDBAdapter) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	tx, err := a.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &sqlTxAdapter{tx}, nil
}

// Close は sql.DB の Close メソッドを呼び出します。
func (a *sqlDBAdapter) Close() error {
	return a.db.Close()
}

// PingContext は sql.DB の PingContext メソッドを呼び出します。
func (a *sqlDBAdapter) PingContext(ctx context.Context) error {
	return a.db.PingContext(ctx)
}

// ExecContext は sql.DB の ExecContext メソッドを呼び出します。
func (a *sqlDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

// QueryContext は sql.DB の QueryContext メソッドを呼び出します。
func (a *sqlDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

// QueryRowContext は sql.DB の QueryRowContext メソッドを呼び出します。
func (a *sqlDBAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.db.QueryRowContext(ctx, query, args...) // 修正: args を渡す
}

// NewDBConnectionFromConfig は設定に基づいて適切なデータベース接続を確立します。
// 登録されたコネクタの中から適切なものを選択して接続します。
func NewDBConnectionFromConfig(cfg config.DatabaseConfig) (DBConnection, error) { // ★ 変更: 戻り値を DBConnection に
	rawDB, err := connector.GetSQLDB(cfg) // connector パッケージの GetSQLDB を呼び出す
	if err != nil {
		return nil, err
	}
	// 取得した *sql.DB を DBConnection インターフェースに適合させる
	return NewSQLDBAdapter(rawDB), nil // ★ 追加: アダプターでラップして返す
}
