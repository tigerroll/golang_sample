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
	"github.com/joho/godotenv" // Import godotenv here

	config "github.com/tigerroll/go_sample/pkg/batch/config"
	core "github.com/tigerroll/go_sample/pkg/batch/job/core"
	factory "github.com/tigerroll/go_sample/pkg/batch/job/factory"
	jsl "github.com/tigerroll/go_sample/pkg/batch/job/jsl"
	jobListener "github.com/tigerroll/go_sample/pkg/batch/job/listener"
	batch_joboperator "github.com/tigerroll/go_sample/pkg/batch/job/joboperator"
	repository "github.com/tigerroll/go_sample/pkg/batch/repository"
	exception "github.com/tigerroll/go_sample/pkg/batch/util/exception"
	logger "github.com/tigerroll/go_sample/pkg/batch/util/logger"

	// JSLでコンポーネントを動的に解決するため、Reader/Processor/Writerのパッケージをインポート
	_ "github.com/tigerroll/go_sample/pkg/batch/weather/step/processor"
	_ "github.com/tigerroll/go_sample/pkg/batch/weather/step/reader"
	_ "github.com/tigerroll/go_sample/pkg/batch/weather/step/writer"
	_ "github.com/tigerroll/go_sample/pkg/batch/step/processor"
	_ "github.com/tigerroll/go_sample/pkg/batch/step/reader"
	_ "github.com/tigerroll/go_sample/pkg/batch/step/writer"
	_ "github.com/tigerroll/go_sample/pkg/batch/step"

	// dummy imports for resolving build errors
	dummyProcessor "github.com/tigerroll/go_sample/pkg/batch/step/processor"
	dummyReader "github.com/tigerroll/go_sample/pkg/batch/step/reader"
	dummyWriter "github.com/tigerroll/go_sample/pkg/batch/step/writer"
	dummyTasklet "github.com/tigerroll/go_sample/pkg/batch/step"
	executionContextReader "github.com/tigerroll/go_sample/pkg/batch/step/reader"
	executionContextWriter "github.com/tigerroll/go_sample/pkg/batch/step/writer"

	// weather 関連のパッケージをインポート (JobFactory への登録用)
	weather_config "github.com/tigerroll/go_sample/pkg/batch/weather/config"
	weather_repo "github.com/tigerroll/go_sample/pkg/batch/weather/repository"
	weather_processor "github.com/tigerroll/go_sample/pkg/batch/weather/step/processor"
	weather_reader "github.com/tigerroll/go_sample/pkg/batch/weather/step/reader"
	weather_writer "github.com/tigerroll/go_sample/pkg/batch/weather/step/writer"
	weather_job "github.com/tigerroll/go_sample/pkg/batch/weather/job"
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
// この関数はマイグレーションのために raw *sql.DB を返します。
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
	case "postgres", "redshift":
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
// .env ファイルのロード、設定の読み込み、データベースマイグレーション、
// リポジトリ、ファクトリ、オペレータの生成を行います。
// envFilePath: .env ファイルのパスを指定します。空の場合はロードしません。
func (bi *BatchInitializer) Initialize(ctx context.Context, envFilePath string) (batch_joboperator.JobOperator, error) {
	// Step 1: .env ファイルのロード
	if envFilePath != "" {
		if err := godotenv.Load(envFilePath); err != nil {
			logger.Warnf(".env ファイル '%s' のロードに失敗しました (本番環境では環境変数を使用): %v", envFilePath, err)
		} else {
			logger.Infof(".env ファイル '%s' をロードしました。", envFilePath)
		}
	} else {
		logger.Debugf(".env ファイルのパスが指定されていないため、ロードをスキップします。")
	}

	// Step 2: 設定のロード (main.go から移動)
	// BytesConfigLoader を使用して埋め込み設定をロード
	bytesLoader := config.NewBytesConfigLoader(bi.Config.EmbeddedConfig) // Config に EmbeddedConfig フィールドを追加
	cfg, err := bytesLoader.Load()
	if err != nil {
		return nil, exception.NewBatchError("initializer", "設定のロードに失敗しました", err, false, false)
	}
	// ロードされた設定を BatchInitializer の Config に反映
	bi.Config = cfg

	// ロギングレベルの設定
	logger.SetLogLevel(bi.Config.System.Logging.Level)
	logger.Infof("ロギングレベルを '%s' に設定しました。", bi.Config.System.Logging.Level)

	// Step 3: データベース接続とマイグレーション
	dbDSN := bi.Config.Database.ConnectionString()
	if dbDSN == "" {
		return nil, exception.NewBatchErrorf("initializer", "データベース接続文字列の構築に失敗しました。")
	}

	dbDriverName := ""
	switch bi.Config.Database.Type {
	case "postgres", "redshift":
		dbDriverName = "postgres"
	case "mysql":
		dbDriverName = "mysql"
	default:
		return nil, exception.NewBatchErrorf("initializer", "未対応のデータベースタイプです: %s", bi.Config.Database.Type)
	}

	// マイグレーション用のDB接続を確立 (リトライ付き)
	// applyMigrations は raw *sql.DB ではなく connection string を受け取るため、
	// ここで dbForMigrate を直接使う必要はないが、Pingのために接続は必要。
	dbForMigrate, err := connectWithRetry(ctx, dbDriverName, dbDSN, 10, 5*time.Second)
	if err != nil {
		return nil, exception.NewBatchError("initializer", "データベースへの接続に失敗しました", err, false, false)
	}
	// マイグレーション完了後、マイグレーション用DB接続をクローズ
	defer func() { // defer でクローズするように変更
		if closeErr := dbForMigrate.Close(); closeErr != nil {
			logger.Errorf("マイグレーション用データベース接続のクローズに失敗しました: %v", closeErr)
		} else {
			logger.Debugf("マイグレーション用データベース接続を閉じました。")
		}
	}()

	// バッチフレームワークのマイグレーションを実行
	if err := applyMigrations(dbDSN, "pkg/batch/resources/migrations/batch_framework", dbDriverName); err != nil { // ★ 変更: マイグレーションパスを明示的に指定
		return nil, exception.NewBatchError("initializer", "バッチフレームワークのマイグレーションに失敗しました", err, false, false)
	}

	// アプリケーション固有のマイグレーションを実行 (設定されていれば)
	if bi.Config.Database.AppMigrationPath != "" {
		if err := applyMigrations(dbDSN, bi.Config.Database.AppMigrationPath, dbDriverName); err != nil {
			return nil, exception.NewBatchError("initializer", "アプリケーションのマイグレーションに失敗しました", err, false, false)
		}
	}

	// Step 4: Job Repository の生成
	// NewJobRepository は内部で SQLClient を作成し、コネクションプール設定を適用する
	jobRepository, err := repository.NewJobRepository(ctx, *bi.Config)
	if err != nil {
		return nil, exception.NewBatchError("initializer", "Job Repository の生成に失敗しました", err, false, false)
	}
	bi.JobRepository = jobRepository
	logger.Infof("Job Repository を生成しました。")

	// JobRepository から基盤となる SQLClient 接続を取得
	sqlClientForComponents := jobRepository.GetClient() // ★ 変更: GetDB() から GetClient() に変更
	if sqlClientForComponents == nil {
		return nil, exception.NewBatchErrorf("initializer", "JobRepository からデータベースクライアントを取得できませんでした。")
	}

	// Step 5: JSL 定義のロード
	if err := jsl.LoadJSLDefinitionFromBytes(bi.JSLDefinitionBytes); err != nil {
		return nil, exception.NewBatchError("initializer", "JSL 定義のロードに失敗しました", err, false, false)
	}
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", jsl.GetLoadedJobCount())

	// Step 6: JobFactory の生成とコンポーネント/ジョブビルダーの登録
	jobFactory := factory.NewJobFactory(bi.Config, bi.JobRepository)
	bi.registerComponentBuilders(jobFactory, sqlClientForComponents) // ★ 変更: dbConnectionForComponents から sqlClientForComponents に変更
	bi.registerJobBuilders(jobFactory)                                 // JobBuilder の登録
	bi.JobFactory = jobFactory
	logger.Debugf("JobFactory を Job Repository と共に作成し、ビルダーを登録しました。")

	// Step 7: JobOperator の生成
	jobOperator := batch_joboperator.NewDefaultJobOperator(bi.JobRepository, *bi.JobFactory)
	bi.JobOperator = jobOperator
	logger.Infof("DefaultJobOperator を生成しました。")

	return bi.JobOperator, nil
}

// registerComponentBuilders は、利用可能な全てのコンポーネントのビルド関数を JobFactory に登録します。
func (bi *BatchInitializer) registerComponentBuilders(jobFactory *factory.JobFactory, client repository.SQLClient) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
	// Weather 関連コンポーネントの登録
	jobFactory.RegisterComponentBuilder("weatherReader", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		weatherReaderCfg := &weather_config.WeatherReaderConfig{
			APIEndpoint: cfg.Batch.APIEndpoint,
			APIKey:      cfg.Batch.APIKey,
		}
		return weather_reader.NewWeatherReader(weatherReaderCfg), nil
	})
	jobFactory.RegisterComponentBuilder("weatherProcessor", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return weather_processor.NewWeatherProcessor(), nil
	})
	jobFactory.RegisterComponentBuilder("weatherWriter", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		var weatherSpecificRepo weather_repo.WeatherRepository
		switch cfg.Database.Type {
		case "postgres", "redshift":
			weatherSpecificRepo = weather_repo.NewPostgresWeatherRepository(client) // ★ 変更: db から client に変更
		case "mysql":
			weatherSpecificRepo = weather_repo.NewMySQLWeatherRepository(client) // ★ 変更: db から client に変更
		default:
			return nil, fmt.Errorf("未対応のデータベースタイプです: %s", cfg.Database.Type)
		}
		return weather_writer.NewWeatherWriter(weatherSpecificRepo), nil
	})

	// ダミーコンポーネントの登録
	jobFactory.RegisterComponentBuilder("dummyReader", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return dummyReader.NewDummyReader(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyProcessor", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return dummyProcessor.NewDummyProcessor(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyWriter", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return dummyWriter.NewDummyWriter(), nil
	})
	jobFactory.RegisterComponentBuilder("executionContextReader", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return executionContextReader.NewExecutionContextReader(), nil
	})
	jobFactory.RegisterComponentBuilder("executionContextWriter", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return executionContextWriter.NewExecutionContextWriter(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyTasklet", func(cfg *config.Config, client repository.SQLClient) (any, error) { // ★ 変更: db *sql.DB から client repository.SQLClient に変更
		return dummyTasklet.NewDummyTasklet(), nil
	})

	logger.Debugf("全てのコンポーネントビルダーを登録しました。")
}

// registerJobBuilders は、利用可能な全てのジョブのビルド関数を JobFactory に登録します。
func (bi *BatchInitializer) registerJobBuilders(jobFactory *factory.JobFactory) {
	// Weather Job のビルダー登録
	jobFactory.RegisterJobBuilder("weather", func(
		jobRepository repository.JobRepository,
		cfg *config.Config,
		listeners []jobListener.JobExecutionListener,
		flow *core.FlowDefinition,
	) (core.Job, error) {
		return weather_job.NewWeatherJob(jobRepository, cfg, listeners, flow), nil
	})

	logger.Debugf("全てのジョブビルダーを登録しました。")
}

// Close は BatchInitializer が保持するリソースを解放します。
func (bi *BatchInitializer) Close() error {
	var err error
	if bi.JobRepository != nil {
		closeErr := bi.JobRepository.Close()
		if closeErr != nil {
			logger.Errorf("Job Repository のクローズに失敗しました: %v", closeErr)
			err = fmt.Errorf("Job Repository クローズエラー: %w", closeErr)
		} else {
			logger.Infof("Job Repository を正常にクローズしました。")
		}
	}
	// 他のリソースがあればここに追加
	return err
}
