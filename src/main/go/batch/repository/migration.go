package repository

import (
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"    // MySQL ドライバを登録
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // PostgreSQL および Redshift ドライバを登録
	_ "github.com/golang-migrate/migrate/v4/source/file"       // ファイルソースドライバを登録

	"sample/src/main/go/batch/util/exception" // モジュール名からの完全な相対パスに修正
	"sample/src/main/go/batch/util/logger"    // モジュール名からの完全な相対パスに修正
)

// RunMigrations は指定されたデータベースにマイグレーションを実行します。
//
// dbType: データベースの種類 (例: "postgres", "mysql", "redshift")
// connectionString: データベースへの接続文字列 (config.DatabaseConfig.ConnectionString() から取得される形式)
// migrationsPath: SQLマイグレーションファイルが配置されているパス (例: "file://./migrations")
func RunMigrations(dbType, connectionString, migrationsPath string) error {
	logger.Infof("データベースマイグレーションを開始します。DBタイプ: %s, マイグレーションパス: %s", dbType, migrationsPath)

	// golang-migrate/migrate が期待するデータベースURL形式に調整
	// config.ConnectionString() が返す形式は、PostgreSQL/Redshift の場合は "postgres://" で適切ですが、
	// MySQL の場合は "user:pass@tcp(host:port)/db" 形式なので、"mysql://" プレフィックスを追加する必要があります。
	databaseURL := connectionString
	switch strings.ToLower(dbType) {
	case "postgres", "redshift":
		// config.ConnectionString() が既に "postgres://" プレフィックスを含むため、そのまま使用
	case "mysql":
		// config.ConnectionString() は "user:pass@tcp(host:port)/db" 形式なので、"mysql://" プレフィックスを追加
		databaseURL = "mysql://" + connectionString
	default:
		return exception.NewBatchErrorf("migration", "サポートされていないデータベースタイプ: %s", dbType)
	}

	m, err := migrate.New(
		migrationsPath,
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
