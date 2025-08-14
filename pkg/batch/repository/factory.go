package repository

import (
	"context"
	"fmt"

	"sample/pkg/batch/config"
	"sample/pkg/batch/database" // database パッケージをインポート
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

	// ここでデータベース接続を確立します。
	// database.NewDBConnectionFromConfig は *sql.DB を返します。
	rawDB, err := database.NewDBConnectionFromConfig(cfg.Database) // ★ 変更: rawDB を受け取る
	if err != nil {
		logger.Errorf("JobRepository 用のデータベース接続確立に失敗しました (Type: %s): %v", cfg.Database.Type, err)
		return nil, exception.NewBatchError(module, fmt.Sprintf("JobRepository 用のデータベース接続確立に失敗しました (Type: %s)", cfg.Database.Type), err, false, false)
	}

	// データベースへの疎通確認 (Ping)
	if err = rawDB.PingContext(ctx); err != nil { // rawDB を使用
		rawDB.Close() // Ping に失敗したら接続を閉じる
		logger.Errorf("JobRepository 用のデータベースへの Ping に失敗しました (Type: %s): %v", cfg.Database.Type, err)
		return nil, exception.NewBatchError(module, fmt.Sprintf("JobRepository 用のデータベースへの Ping に失敗しました (Type: %s)", cfg.Database.Type), err, false, false)
	}

	// 取得した *sql.DB を DBConnection インターフェースに適合させる
	dbConn := database.NewSQLDBAdapter(rawDB) // ★ 追加: アダプターでラップ

	// SQLJobRepository に DBConnection インターフェースを渡す
	logger.Debugf("SQLJobRepository を生成しました。") // ログメッセージはそのまま
	return NewSQLJobRepository(dbConn), nil // ★ 変更: dbConn を渡す
}

// TODO: Job Repository が使用するデータベース接続を閉じるための関数や、
//       アプリケーション終了時に適切に Close() が呼び出される仕組みが必要です。
//       main 関数で JobRepository を生成した後、defer で Close() を呼び出すなどが考えられます。
