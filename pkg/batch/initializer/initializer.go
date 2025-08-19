package initializer

import (
	"context"
	"fmt"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/snowflakedb/gosnowflake"

	batch_joblauncher "sample/pkg/batch/job/joblauncher" // joblauncher をインポート
	config "sample/pkg/batch/config"
	factory "sample/pkg/batch/job/factory"
	jsl "sample/pkg/batch/job/jsl" // jsl パッケージをインポート
	batch_joboperator "sample/pkg/batch/job/joboperator" // joboperator をインポート
	batch_database_connector "sample/pkg/batch/database/connector" // database/connector パッケージをインポート
	"sample/pkg/batch/repository" // ★ この行が重要です。repository パッケージを再度インポート
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// BatchInitializer はバッチアプリケーションの初期化処理を担当します。
type BatchInitializer struct {
	Config             *config.Config
	JSLDefinitionBytes []byte // JSL定義のバイトスライス
	JobRepository      job.JobRepository // job.JobRepository に変更
	JobFactory         *factory.JobFactory // ポインタ型を維持
	JobLauncher        batch_joblauncher.JobLauncher // JobLauncher を追加
	JobOperator        batch_joboperator.JobOperator
}

// NewBatchInitializer は新しい BatchInitializer のインスタンスを作成します。
func NewBatchInitializer(cfg *config.Config) *BatchInitializer {
	return &BatchInitializer{
		Config: cfg,
	}
}

// Initialize はバッチアプリケーションの初期化処理を実行します。
// .env ファイルのロードは呼び出し元 (application.go) で行われるため、ここでは削除
func (bi *BatchInitializer) Initialize(ctx context.Context) (batch_joblauncher.JobLauncher, batch_joboperator.JobOperator, error) { // ★ 変更: *factory.JobFactory を削除
	logger.Debugf("BatchInitializer.Initialize が呼び出されました。")

	// Step 1: 設定のロード (main.go から移動)
	// BytesConfigLoader を使用して埋め込み設定をロード
	bytesLoader := config.NewBytesConfigLoader(bi.Config.EmbeddedConfig)
	cfg, err := bytesLoader.Load()
	if err != nil {
		return nil, nil, exception.NewBatchError("initializer", "設定のロードに失敗しました", err, false, false)
	}
	// ロードされた設定を BatchInitializer の Config に反映
	bi.Config = cfg

	// ロギングレベルの設定
	logger.SetLogLevel(bi.Config.System.Logging.Level)
	logger.Infof("ロギングレベルを '%s' に設定しました。", bi.Config.System.Logging.Level)

	// Step 2: データベース接続とマイグレーション
	dbDSN := bi.Config.Database.ConnectionString()
	if dbDSN == "" {
		return nil, nil, exception.NewBatchErrorf("initializer", "データベース接続文字列の構築に失敗しました。")
	}

	dbDriverName := ""
	switch bi.Config.Database.Type {
	case "postgres", "redshift":
		dbDriverName = "postgres"
	case "mysql":
		dbDriverName = "mysql"
	default:
		return nil, nil, exception.NewBatchErrorf("initializer", "未対応のデータベースタイプです: %s", bi.Config.Database.Type)
	}

	// バッチフレームワークのマイグレーションを実行
	// pkg/batch/resources/migrations/batch_framework にあるマイグレーションを適用
	frameworkMigrationPath := "pkg/batch/resources/migrations/batch_framework"
	logger.Infof("データベースマイグレーションを開始します。DBタイプ: %s, マイグレーションパス: %s", dbDriverName, frameworkMigrationPath)
	// ★ 変更: RunMigrations に migrationsTable 引数を追加
	if err := batch_database_connector.RunMigrations(dbDriverName, dbDSN, frameworkMigrationPath, "batch_schema_migrations"); err != nil {
		logger.Errorf("バッチフレームワークのデータベースマイグレーション中にエラーが発生しました: %v", err)
		return nil, nil, exception.NewBatchError("initializer", "バッチフレームワークのマイグレーションに失敗しました", err, false, false)
	}
	logger.Infof("バッチフレームワークのマイグレーションが正常に完了しました。")

	// アプリケーション固有のマイグレーションを実行 (設定されていれば)
	if bi.Config.Database.AppMigrationPath != "" {
		logger.Infof("データベースマイグレーションを開始します。DBタイプ: %s, マイグレーションパス: %s", dbDriverName, bi.Config.Database.AppMigrationPath)
		// ★ 変更: RunMigrations に migrationsTable 引数を追加
		if err := batch_database_connector.RunMigrations(dbDriverName, dbDSN, bi.Config.Database.AppMigrationPath, "app_schema_migrations"); err != nil {
			logger.Errorf("アプリケーション固有のデータベースマイグレーション中にエラーが発生しました: %v", err)
			return nil, nil, exception.NewBatchError("initializer", "アプリケーションのマイグレーションに失敗しました", err, false, false)
		}
		logger.Infof("アプリケーション固有のマイグレーションが正常に完了しました。")
	}

	// Step 3: Job Repository の生成
	jobRepository, err := repository.NewJobRepository(ctx, *bi.Config)
	if err != nil {
		return nil, nil, exception.NewBatchError("initializer", "Job Repository の生成に失敗しました", err, false, false)
	}
	bi.JobRepository = jobRepository
	logger.Infof("Job Repository を生成しました。")

	// Step 4: JSL 定義のロード
	if err := jsl.LoadJSLDefinitionFromBytes(bi.JSLDefinitionBytes); err != nil {
		return nil, nil, exception.NewBatchError("initializer", "JSL 定義のロードに失敗しました", err, false, false)
	}
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", jsl.GetLoadedJobCount())

	// Step 5: JobFactory の生成とコンポーネント/ジョブビルダーの登録
	jobFactory := factory.NewJobFactory(bi.Config, bi.JobRepository)
	bi.JobFactory = jobFactory
	logger.Debugf("JobFactory を Job Repository と共に作成しました。")

	// Step 6: JobOperator の生成
	jobOperator := batch_joboperator.NewDefaultJobOperator(bi.JobRepository, *bi.JobFactory)
	bi.JobOperator = jobOperator
	logger.Infof("DefaultJobOperator を生成しました。")

	// Step 7: JobLauncher の生成
	jobLauncher := batch_joblauncher.NewSimpleJobLauncher(bi.JobRepository, *bi.JobFactory)
	bi.JobLauncher = jobLauncher
	logger.Infof("SimpleJobLauncher を生成しました。")

	return bi.JobLauncher, bi.JobOperator, nil // ★ 変更: *factory.JobFactory を削除
}

// Close は BatchInitializer が保持するリソースを解放します。
func (bi *BatchInitializer) Close() error {
	var errs []error
	if bi.JobRepository != nil {
		if closeErr := bi.JobRepository.Close(); closeErr != nil {
			logger.Errorf("Job Repository のクローズに失敗しました: %v", closeErr)
			errs = append(errs, fmt.Errorf("Job Repository クローズエラー: %w", closeErr))
		} else {
			logger.Infof("Job Repository を正常にクローズしました。")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("複数のクローズエラーが発生しました: %v", errs)
	}
	return nil
}
