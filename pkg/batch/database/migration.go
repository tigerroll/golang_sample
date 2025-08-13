package database // パッケージ名を database に変更

import (
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"    // MySQL ドライバを登録
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // PostgreSQL および Redshift ドライバを登録
	_ "github.com/golang-migrate/migrate/v4/source/file"       // ファイルソースドライバを登録

	"sample/pkg/batch/util/exception" // モジュール名からの完全な相対パスに修正
	"sample/pkg/batch/util/logger"    // モジュール名からの完全な相対パスに修正
)

// RunMigrations は指定されたデータベースにマイグレーションを実行します。
//
// dbType: データベースの種類 (例: "postgres", "mysql", "redshift")
// connectionString: データベースへの接続文字列 (config.DatabaseConfig.ConnectionString() から取得される形式)
// migrationsPath: SQLマイグレーションファイルが配置されているパス (例: "file://./migrations")
func RunMigrations(dbType, connectionString, migrationsPath string) error {
	logger.Infof("データベースマイグレーションを開始します。DBタイプ: %s, マイグレーションパス: %s", dbType, migrationsPath)

	// golang-migrate/migrate が期待するデータベースURL形式に調整
	// バッチフレームワークのマイグレーションは 'batch_schema_migrations' テーブルを使用
	databaseURL := connectionString
	switch strings.ToLower(dbType) {
	case "postgres", "redshift":
		// postgres://... の形式に x-migrations-table を追加
		if !strings.Contains(databaseURL, "?") {
			databaseURL += "?"
		} else {
			databaseURL += "&"
		}
		databaseURL += "x-migrations-table=batch_schema_migrations"
	case "mysql":
		// mysql://user:pass@tcp(host:port)/db の形式に x-migrations-table を追加
		databaseURL = "mysql://" + connectionString
		if !strings.Contains(databaseURL, "?") {
			databaseURL += "?"
		} else {
			databaseURL += "&"
		}
		databaseURL += "x-migrations-table=batch_schema_migrations"
	default:
		return exception.NewBatchErrorf("migration", "サポートされていないデータベースタイプ: %s", dbType)
	}

	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationsPath),
		databaseURL,
	)
	if err != nil {
		return exception.NewBatchError("migration", "マイグレーションインスタンスの作成に失敗しました", err, false, false)
	}

	// すべてのアップマイグレーションを実行
	if err = m.Up(); err != nil {
		if err == migrate.ErrNoChange {
			logger.Infof("マイグレーションは不要です。データベースは最新の状態です。")
			return nil // 変更がない場合はエラーではない
		}
		return exception.NewBatchError("migration", "マイグレーションの実行に失敗しました", err, false, false)
	}

	logger.Infof("データベースマイグレーションが正常に完了しました。")
	return nil
}
