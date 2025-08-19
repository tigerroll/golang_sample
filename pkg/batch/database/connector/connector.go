package connector

import (
	"context"
	"database/sql"
	"fmt"
	"strings" // ★ 追加: strings パッケージをインポート

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"sample/pkg/batch/config"
	"sample/pkg/batch/database"
	"sample/pkg/batch/util/exception"
	"sample/pkg/batch/util/logger"
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

// GetSQLDB は設定に基づいて適切なデータベース接続を確立します。
// 登録されたコネクタの中から適切なものを選択して接続します。
func GetSQLDB(cfg config.DatabaseConfig) (*sql.DB, error) {
	connector, ok := connectors[cfg.Type]
	if !ok {
		return nil, exception.NewBatchError("database", fmt.Sprintf("未対応のデータベースタイプ: %s", cfg.Type), nil, false, false)
	}
	return connector.Connect(cfg)
}

// NewDBConnectionFromConfig は設定に基づいて適切なデータベース接続を確立します。
// 登録されたコネクタの中から適切なものを選択して接続します。
func NewDBConnectionFromConfig(ctx context.Context, cfg config.DatabaseConfig) (database.DBConnection, error) {
	rawDB, err := GetSQLDB(cfg)
	if err != nil {
		return nil, err
	}
	// 取得した *sql.DB を database.DBConnection インターフェースに適合させる
	// PingContext を呼び出して接続を確認
	if err := rawDB.PingContext(ctx); err != nil {
		return nil, exception.NewBatchError("database", "データベースへのPingに失敗しました", err, true, false)
	}
	return database.NewSQLDBAdapter(rawDB), nil
}

// RunMigrations は指定されたデータベースURLとマイグレーションパスに対してマイグレーションを実行します。
// dbType: データベースの種類 (e.g., "postgres")
// dbDSN: データベース接続文字列
// migrationPath: マイグレーションファイルが配置されているパス (e.g., "file://./migrations")
// migrationsTable: マイグレーション履歴を記録するテーブル名 (オプション)。空の場合はデフォルトの "schema_migrations" が使用されます。
func RunMigrations(dbType, dbDSN, migrationPath, migrationsTable string) error { // ★ 変更: migrationsTable 引数を追加
	if migrationPath == "" {
		logger.Infof("マイグレーションパスが指定されていません。スキップします。")
		return nil
	}

	// カスタムマイグレーションテーブル名が指定されている場合、DSNに追加
	if migrationsTable != "" {
		// PostgreSQLの場合のみ対応 (他のDBタイプは別途考慮が必要)
		if dbType == "postgres" {
			if !strings.Contains(dbDSN, "?") {
				dbDSN = fmt.Sprintf("%s?x-migrations-table=%s", dbDSN, migrationsTable)
			} else {
				dbDSN = fmt.Sprintf("%s&x-migrations-table=%s", dbDSN, migrationsTable)
			}
		} else {
			logger.Warnf("カスタムマイグレーションテーブル名 '%s' は現在、DBタイプ '%s' ではサポートされていません。デフォルトのテーブルが使用されます。", migrationsTable, dbType)
		}
	}

	logger.Infof("データベースマイグレーションを開始します。DBタイプ: %s, マイグレーションパス: %s", dbType, migrationPath)
	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationPath),
		dbDSN,
	)
	if err != nil {
		return exception.NewBatchError("database_migration", fmt.Sprintf("マイグレーションインスタンスの作成に失敗しました: %s", migrationPath), err, false, false)
	}

	// ★ 削除: 開発環境向けの m.Down() ロジックを削除

	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		return exception.NewBatchError("database_migration", fmt.Sprintf("マイグレーションの適用に失敗しました: %s", migrationPath), err, false, false)
	}

	if err == migrate.ErrNoChange {
		logger.Infof("マイグレーションは不要です。データベースは最新の状態です。")
	} else {
		logger.Infof("マイグレーションが正常に完了しました。")
	}
	return nil
}
