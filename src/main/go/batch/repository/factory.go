package repository

import (
	"context"
	"database/sql"
	"fmt"

	"sample/src/main/go/batch/config"
	"sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"

	_ "github.com/lib/pq"              // PostgreSQL/Redshift ドライバ
	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ
	// "cloud.google.com/go/bigquery"
)

// NewJobRepository は JobRepository のインスタンスを作成します。
// アプリケーションの設定を基にデータベース接続を確立します。
func NewJobRepository(ctx context.Context, cfg config.Config) (JobRepository, error) {
	module := "repository_factory"
	logger.Debugf("JobRepository の生成を開始します (Type: %s).", cfg.Database.Type)

	// ここでデータベース接続を確立します。
	// 既存の DatabaseConfig と ConnectionString() メソッドを利用します。
	db, err := sql.Open(cfg.Database.Type, cfg.Database.ConnectionString()) // ドライバ名は config.Database.Type をそのまま使用
	if err != nil {
		logger.Errorf("JobRepository 用のデータベース接続確立に失敗しました (Type: %s): %v", cfg.Database.Type, err)
		return nil, exception.NewBatchError(module, fmt.Sprintf("JobRepository 用のデータベース接続確立に失敗しました (Type: %s)", cfg.Database.Type), err, false, false)
	}

	// データベースへの疎通確認 (Ping)
	if err = db.PingContext(ctx); err != nil {
		db.Close() // Ping に失敗したら接続を閉じる
		logger.Errorf("JobRepository 用のデータベースへの Ping に失敗しました (Type: %s): %v", cfg.Database.Type, err)
		return nil, exception.NewBatchError(module, fmt.Sprintf("JobRepository 用のデータベースへの Ping に失敗しました (Type: %s)", cfg.Database.Type), err, false, false)
	}

	// バッチフレームワークのマイグレーションを実行
	// このパスは、create_batch_tables.sql が golang-migrate 形式で配置されているディレクトリを指します。
	// 例: src/main/go/batch/resources/migrations/batch_framework
	batchFrameworkMigrationPath := "src/main/go/batch/resources/migrations/batch_framework" // ★ この行が正しいことを確認
	if err := RunMigrations(cfg.Database.Type, cfg.Database.ConnectionString(), batchFrameworkMigrationPath); err != nil {
		db.Close() // マイグレーション失敗時はDB接続を閉じる
		logger.Errorf("バッチフレームワークのマイグレーションに失敗しました: %v", err)
		return nil, exception.NewBatchError(module, "バッチフレームワークのマイグレーションに失敗しました", err, false, false)
	}
	logger.Infof("バッチフレームワークのマイグレーションが正常に完了しました。")

	// Simplification: 一旦 SQLJobRepository を返す前提で進めます。
	// 実際のプロダクションコードでは、データベースタイプごとに異なる JobRepository 実装を用意し、
	// ここで適切な実装を選択する必要があります。
	logger.Debugf("SQLJobRepository を生成しました。")
	return NewSQLJobRepository(db), nil
}

// TODO: Job Repository が使用するデータベース接続を閉じるための関数や、
//       アプリケーション終了時に適切に Close() が呼び出される仕組みが必要です。
//       main 関数で JobRepository を生成した後、defer で Close() を呼び出すなどが考えられます。
