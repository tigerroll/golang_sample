package repository

import (
	"context"
	"fmt"

	"sample/pkg/batch/config"
	"sample/pkg/batch/database"
	"sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"

	_ "github.com/lib/pq"              // PostgreSQL/Redshift ドライバ
	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ
	// "cloud.google.com/go/bigquery"
)

// NewJobRepository は JobRepository のインスタンスを作成します。
// アプリケーションの設定を基にデータベース接続を確立します。
func NewJobRepository(ctx context.Context, cfg config.Config) (JobRepository, error) {
	module := "repository_factory"
	logger.Debugf("JobRepository の生成を開始します (Type: %s).", cfg.Database.Type)

	dbConn, err := database.NewDBConnectionFromConfig(cfg.Database)
	if err != nil {
		logger.Errorf("JobRepository 用のデータベース接続確立に失敗しました (Type: %s): %v", cfg.Database.Type, err)
		return nil, exception.NewBatchError(module, fmt.Sprintf("JobRepository 用のデータベース接続確立に失敗しました (Type: %s)", cfg.Database.Type), err, false, false)
	}

	if err = dbConn.PingContext(ctx); err != nil {
		dbConn.Close()
		logger.Errorf("JobRepository 用のデータベースへの Ping に失敗しました (Type: %s): %v", cfg.Database.Type, err)
		return nil, exception.NewBatchError(module, fmt.Sprintf("JobRepository 用のデータベースへの Ping に失敗しました (Type: %s)", cfg.Database.Type), err, false, false)
	}

	// SQLJobRepository に DBConnection インターフェースを渡す
	logger.Debugf("SQLJobRepository を生成しました。")
	return NewSQLJobRepository(dbConn), nil // 新しい NewSQLJobRepository を呼び出す
}

// TODO: Job Repository が使用するデータベース接続を閉じるための関数や、
//       アプリケーション終了時に適切に Close() が呼び出される仕組みが必要です。
//       main 関数で JobRepository を生成した後、defer で Close() を呼び出すなどが考えられます。
