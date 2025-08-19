package repository

import (
	"context"
	"fmt"

	"sample/pkg/batch/config"
	"sample/pkg/batch/database/connector" // database/connector パッケージをインポート
	"sample/pkg/batch/repository/job" // job インターフェースをインポート
	"sample/pkg/batch/repository/sql" // sql 実装をインポート
	"sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"

	_ "github.com/lib/pq"              // PostgreSQL/Redshift ドライバ
	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ
	// "cloud.google.com/go/bigquery"
)

// NewJobRepository は JobRepository のインスタンスを作成します。
// アプリケーションの設定を基にデータベース接続を確立します。
func NewJobRepository(ctx context.Context, cfg config.Config) (job.JobRepository, error) { // 戻り値を job.JobRepository に変更
	module := "repository_factory"
	logger.Debugf("JobRepository の生成を開始します (Type: %s).", cfg.Database.Type)

	dbConn, err := connector.NewDBConnectionFromConfig(ctx, cfg.Database) // ctx を追加
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
	logger.Debugf("SQLJobRepository の実装を生成しました。")
	return sql.NewSQLJobRepository(dbConn), nil // sql.NewSQLJobRepository を呼び出す
}

// TODO: Job Repository が使用するデータベース接続を閉じるための関数や、
//       アプリケーション終了時に適切に Close() が呼び出される仕組みが必要です。
//       main 関数で JobRepository を生成した後、defer で Close() を呼び出すなどが考えられます。
