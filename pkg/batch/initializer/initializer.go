package initializer

import (
	"context"
	"database/sql"
	"fmt"
	"os" // Keep os import for os.Getenv
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/snowflakedb/gosnowflake"
	// godotenv は application.go でロードするため削除

	config "sample/pkg/batch/config"
	factory "sample/pkg/batch/job/factory"
	jsl "sample/pkg/batch/job/jsl"
	batch_joboperator "sample/pkg/batch/job/joboperator"
	repository "sample/pkg/batch/repository"
	// "sample/pkg/batch/repository/sql_job_repository" // sql_job_repository をインポート
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// BatchInitializer はバッチアプリケーションの初期化処理を担当します。
type BatchInitializer struct {
	Config             *config.Config
	JSLDefinitionBytes []byte // JSL定義のバイトスライス
	JobRepository      repository.JobRepository
	JobFactory         *factory.JobFactory
	JobOperator        batch_joboperator.JobOperator
}

// NewBatchInitializer は新しい BatchInitializer のインスタンスを作成します。
func NewBatchInitializer(cfg *config.Config) *BatchInitializer {
	return &BatchInitializer{
		Config: cfg,
	}
}

// connectWithRetry は指定されたデータベースにリトライ付きで接続を試みます。
func connectWithRetry(ctx context.Context, driverName, dataSourceName string, maxRetries int, delay time.Duration) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < maxRetries; i++ {
		logger.Debugf("データベース接続を試行中 (試行 %d/%d)...", i+1, maxRetries)
		db, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			logger.Warnf("データベース接続のオープンに失敗しました: %v", err)
			time.Sleep(delay)
			continue
		}

		err = db.PingContext(ctx)
		if err == nil {
			logger.Infof("データベース接続に成功しました。")
			return db, nil
		}

		// Pingに失敗した場合、接続を閉じてからリトライ
		db.Close()
		logger.Warnf("データベースへのPingに失敗しました: %v", err)
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("データベースへの接続に最大試行回数 (%d) 失敗しました", maxRetries)
}

// applyMigrations はマイグレーションを実行するヘルパー関数
func applyMigrations(databaseURL string, migrationPath string, dbDriverName string) error {
	if migrationPath == "" {
		logger.Infof("マイグレーションパスが指定されていません。スキップします。")
		return nil
	}

	var migrateURL string
	switch dbDriverName {
	case "postgres":
		migrateURL = databaseURL // postgres://... の形式を想定。デフォルトの schema_migrations を使用。
	case "mysql":
		migrateURL = fmt.Sprintf("mysql://%s", databaseURL) // mysql://user:pass@tcp(host:port)/db の形式を想定。デフォルトの schema_migrations を使用。
	default:
		return fmt.Errorf("未対応のデータベースドライバです: %s", dbDriverName)
	}

	logger.Infof("マイグレーションを実行中: パス '%s'", migrationPath)
	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationPath),
		migrateURL, // x-migrations-table を含まない URL を使用
	)
	if err != nil {
		return exception.NewBatchError("initializer", fmt.Sprintf("マイグレーションインスタンスの作成に失敗しました: %s", migrationPath), err, false, false)
	}

	// 開発環境向け: 既存のマイグレーションを一度ダウンさせてからアップする (テーブルを再作成するため)
	// 本番環境ではこのロジックは使用しないでください。
	if os.Getenv("APP_ENV") == "development" {
		if err := m.Down(); err != nil && err != migrate.ErrNoChange {
			logger.Warnf("既存のマイグレーションのダウンに失敗しました (開発環境のみ): %v", err)
		} else if err == nil {
			logger.Debugf("既存のマイグレーションをダウンしました: %s", migrationPath)
		}
	}

	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		return exception.NewBatchError("initializer", fmt.Sprintf("マイグレーションの適用に失敗しました: %s", migrationPath), err, false, false)
	}

	if err == migrate.ErrNoChange {
		logger.Infof("マイグレーションは不要です: %s", migrationPath)
	} else {
		logger.Infof("マイグレーションが正常に完了しました: %s", migrationPath)
	}
	return nil
}

// Initialize はバッチアプリケーションの初期化処理を実行します。
// .env ファイルのロードは呼び出し元 (application.go) で行われるため、ここでは削除
func (bi *BatchInitializer) Initialize(ctx context.Context) (batch_joboperator.JobOperator, *factory.JobFactory, error) {
	logger.Debugf("BatchInitializer.Initialize が呼び出されました。")

	// Step 1: 設定のロード (main.go から移動)
	// BytesConfigLoader を使用して埋め込み設定をロード
	bytesLoader := config.NewBytesConfigLoader(bi.Config.EmbeddedConfig) // Config に EmbeddedConfig フィールドを追加
	cfg, err := bytesLoader.Load() // cfg はここで初期化される
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
	
	// マイグレーション用のDB接続を確立 (リトライ付き)
	dbForMigrate, err := connectWithRetry(ctx, dbDriverName, dbDSN, 10, 5*time.Second)
	if err != nil {
		return nil, nil, exception.NewBatchError("initializer", "データベースへの接続に失敗しました", err, false, false)
	}
	// マイグレーション用DB接続を main 関数終了時にクローズするように defer を設定
	// ただし、この Initialize 関数内でクローズするのは適切ではないため、main 関数側で管理する
	// defer func() { ... }() はここでは使用しない

	// バッチフレームワークのマイグレーションを実行
	if err := applyMigrations(dbDSN, "", dbDriverName); err != nil { // デフォルトのマイグレーションパスは空文字列で指定
		return nil, nil, exception.NewBatchError("initializer", "バッチフレームワークのマイグレーションに失敗しました", err, false, false)
	}

	// アプリケーション固有のマイグレーションを実行 (設定されていれば)
	if bi.Config.Database.AppMigrationPath != "" {
		if err := applyMigrations(dbDSN, bi.Config.Database.AppMigrationPath, dbDriverName); err != nil { // bi.Config.Database.AppMigrationPath を使用
			return nil, nil, exception.NewBatchError("initializer", "アプリケーションのマイグレーションに失敗しました", err, false, false)
		}
	}

	// マイグレーション完了後、マイグレーション用DB接続をクローズ
	if err := dbForMigrate.Close(); err != nil {
		logger.Errorf("マイグレーション用データベース接続のクローズに失敗しました: %v", err)
	} else {
		logger.Debugf("マイグレーション用データベース接続を閉じました。")
	}

	// Step 3: Job Repository の生成
	jobRepository, err := repository.NewJobRepository(ctx, *bi.Config)
	if err != nil {
		return nil, nil, exception.NewBatchError("initializer", "Job Repository の生成に失敗しました", err, false, false)
	}
	bi.JobRepository = jobRepository
	logger.Infof("Job Repository を生成しました。")

	// JobRepository から基盤となる *sql.DB 接続を取得
	sqlJobRepo, ok := jobRepository.(*repository.SQLJobRepository) // 型アサーションを修正
	if !ok {
		return nil, nil, exception.NewBatchErrorf("initializer", "JobRepository の実装が予期された型ではありません。*sql.DB 接続を取得できません。")
	}
	dbConnectionForComponents := sqlJobRepo.GetDB()
	if dbConnectionForComponents == nil {
		return nil, nil, exception.NewBatchErrorf("initializer", "JobRepository からデータベース接続を取得できませんでした。")
	}

	// Step 4: JSL 定義のロード
	if err := jsl.LoadJSLDefinitionFromBytes(bi.JSLDefinitionBytes); err != nil {
		return nil, nil, exception.NewBatchError("initializer", "JSL 定義のロードに失敗しました", err, false, false)
	}
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", jsl.GetLoadedJobCount())

	// Step 5: JobFactory の生成とコンポーネント/ジョブビルダーの登録
	jobFactory := factory.NewJobFactory(bi.Config, bi.JobRepository) // JobFactory を生成
	bi.JobFactory = jobFactory // BatchInitializer の JobFactory フィールドに設定
	logger.Debugf("JobFactory を Job Repository と共に作成しました。")

	// Step 6: JobOperator の生成
	jobOperator := batch_joboperator.NewDefaultJobOperator(bi.JobRepository, *bi.JobFactory) // JobFactory を渡す
	bi.JobOperator = jobOperator // BatchInitializer の JobOperator フィールドに設定
	logger.Infof("DefaultJobOperator を生成しました。")

	return bi.JobOperator, bi.JobFactory, nil // JobOperator と JobFactory を両方返す
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
