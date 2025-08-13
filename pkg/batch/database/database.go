// pkg/batch/database/database.go
package database

import (
	"database/sql"

	"sample/pkg/batch/config"
	"sample/pkg/batch/database/connector" // 新しく追加
)

// NewDBConnectionFromConfig は設定に基づいて適切なデータベース接続を確立します。
// 登録されたコネクタの中から適切なものを選択して接続します。
func NewDBConnectionFromConfig(cfg config.DatabaseConfig) (*sql.DB, error) {
	return connector.GetSQLDB(cfg) // connector パッケージの GetSQLDB を呼び出す
}
