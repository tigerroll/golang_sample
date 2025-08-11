// pkg/batch/database/database.go
package database

import (
	"database/sql"
	"fmt"

	"sample/pkg/batch/config"
	"sample/pkg/batch/util/exception"
)

// DBConnector は特定のデータベースタイプへの接続を確立するためのインターフェースです。
type DBConnector interface {
	Connect(cfg config.DatabaseConfig) (*sql.DB, error)
}

// connectors は登録されたDBConnectorの実装を保持するマップです。
var connectors = make(map[string]DBConnector)

// RegisterConnector は指定されたタイプ名でDBConnectorを登録します。
func RegisterConnector(dbType string, connector DBConnector) {
	if _, exists := connectors[dbType]; exists {
		// 既に登録されている場合は警告などを出すことも可能
		// logger.Warnf("DBConnector for type '%s' is already registered. Overwriting.", dbType)
	}
	connectors[dbType] = connector
}

// NewDBConnectionFromConfig は設定に基づいて適切なデータベース接続を確立します。
// 登録されたコネクタの中から適切なものを選択して接続します。
func NewDBConnectionFromConfig(cfg config.DatabaseConfig) (*sql.DB, error) {
	connector, ok := connectors[cfg.Type]
	if !ok {
		return nil, exception.NewBatchError("database", fmt.Sprintf("未対応のデータベースタイプ: %s", cfg.Type), nil, false, false)
	}
	return connector.Connect(cfg)
}
