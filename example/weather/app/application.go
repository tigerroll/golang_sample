package app

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	config "sample/pkg/batch/config"
	factory "sample/pkg/batch/job/factory"
	joblistener "sample/pkg/batch/job/listener" // エイリアスを joblistener に変更
	joboperator "sample/pkg/batch/job/joboperator" // joboperator パッケージをインポート
	initializer "sample/pkg/batch/initializer"
	joblauncher "sample/pkg/batch/job/joblauncher" // joblauncher パッケージをインポート
	repository "sample/pkg/batch/repository" // エイリアスを削除し、デフォルトの repository を使用
	exception "sample/pkg/batch/util/exception"
	godotenv "github.com/joho/godotenv" // godotenv をインポート
	logger "sample/pkg/batch/util/logger"
	core "sample/pkg/batch/job/core"

	// weather 関連のパッケージをインポート
	appConfig "sample/example/weather/config"
	appRepo "sample/example/weather/repository"
	appProcessor "sample/example/weather/step/processor"
	appReader "sample/example/weather/step/reader"
	appWriter "sample/example/weather/step/writer"
	appJob "sample/example/weather/job"
	appTasklet "sample/example/weather/step/tasklet" // 新しい tasklet パッケージをインポート

	// pkg/batch に残る汎用コンポーネントのインポート
	executionContextReader "sample/pkg/batch/step/reader"
	executionContextWriter "sample/pkg/batch/step/writer"
)

// registerApplicationComponents はアプリケーション固有のコンポーネントとジョブを JobFactory に登録します。
func registerApplicationComponents(jobFactory *factory.JobFactory, cfg *config.Config, db *sql.DB) {
	// Weather 関連コンポーネントの登録
	jobFactory.RegisterComponentBuilder("weatherReader", func(cfg *config.Config, db *sql.DB) (any, error) {
		weatherReaderCfg := &appConfig.WeatherReaderConfig{
			APIEndpoint: cfg.Batch.APIEndpoint,
			APIKey:      cfg.Batch.APIKey,
		}
		return appReader.NewWeatherReader(weatherReaderCfg), nil
	})
	jobFactory.RegisterComponentBuilder("weatherProcessor", func(cfg *config.Config, db *sql.DB) (any, error) {
		return appProcessor.NewWeatherProcessor(), nil
	})
	jobFactory.RegisterComponentBuilder("weatherWriter", func(cfg *config.Config, db *sql.DB) (any, error) {
		var weatherSpecificRepo appRepo.WeatherRepository
		switch cfg.Database.Type {
		case "postgres", "redshift":
			weatherSpecificRepo = appRepo.NewPostgresWeatherRepository(db)
		case "mysql":
			weatherSpecificRepo = appRepo.NewMySQLWeatherRepository(db)
		default:
			return nil, fmt.Errorf("未対応のデータベースタイプです: %s", cfg.Database.Type)
		}
		return appWriter.NewWeatherWriter(weatherSpecificRepo), nil
	})

	// ダミーコンポーネントの登録
	jobFactory.RegisterComponentBuilder("dummyReader", func(cfg *config.Config, db *sql.DB) (any, error) {
		return appReader.NewDummyReader(), nil // appReader パッケージから呼び出す
	})
	jobFactory.RegisterComponentBuilder("dummyProcessor", func(cfg *config.Config, db *sql.DB) (any, error) {
		return appProcessor.NewDummyProcessor(), nil // appProcessor パッケージから呼び出す
	})
	jobFactory.RegisterComponentBuilder("dummyWriter", func(cfg *config.Config, db *sql.DB) (any, error) {
		return appWriter.NewDummyWriter(), nil // appWriter パッケージから呼び出す
	})
	jobFactory.RegisterComponentBuilder("executionContextReader", func(cfg *config.Config, db *sql.DB) (any, error) {
		return executionContextReader.NewExecutionContextReader(), nil
	})
	jobFactory.RegisterComponentBuilder("executionContextWriter", func(cfg *config.Config, db *sql.DB) (any, error) {
		return executionContextWriter.NewExecutionContextWriter(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyTasklet", func(cfg *config.Config, db *sql.DB) (any, error) {
		return appTasklet.NewDummyTasklet(), nil // appTasklet パッケージから呼び出す
	})

	// JobExecutionListener の登録
	jobFactory.RegisterJobListenerBuilder("loggingJobListener", func(cfg *config.Config) (joblistener.JobExecutionListener, error) {
		return joblistener.NewLoggingJobListener(&cfg.System.Logging), nil
	})

	logger.Debugf("全てのアプリケーションコンポーネントビルダーを登録しました。")

	// Weather Job のビルダー登録
	jobFactory.RegisterJobBuilder("weather", func(
		jobRepository repository.JobRepository, // エイリアスを削除し、デフォルトの repository を使用
		cfg *config.Config,
		listeners []joblistener.JobExecutionListener, // エイリアスを joblistener に変更
		flow *core.FlowDefinition,
	) (core.Job, error) {
		return appJob.NewWeatherJob(jobRepository, cfg, listeners, flow), nil
	})

	logger.Debugf("全てのアプリケーションジョブビルダーを登録しました。")
}

// setupApplication はアプリケーションの初期化処理を実行し、必要なコンポーネントを返します。
func setupApplication(ctx context.Context, envFilePath string, embeddedConfig, embeddedJSL []byte) (*initializer.BatchInitializer, joblauncher.JobLauncher, joboperator.JobOperator, *factory.JobFactory, error) { // ★ 戻り値の型と順序を変更
	// .env ファイルのロード
	if envFilePath != "" {
		if err := godotenv.Load(envFilePath); err != nil {
			logger.Warnf(".env ファイル '%s' のロードに失敗しました (本番環境では環境変数を使用): %v", envFilePath, err)
		} else {
			logger.Infof(".env ファイル '%s' をロードしました。", envFilePath)
		}
	} else {
		logger.Debugf(".env ファイルのパスが指定されていないため、ロードをスキップします。")
	}

	initialCfg := &config.Config{
		EmbeddedConfig: embeddedConfig,
	}

	batchInitializer := initializer.NewBatchInitializer(initialCfg)
	batchInitializer.JSLDefinitionBytes = embeddedJSL

	// バッチアプリケーションの初期化処理を実行 (JobOperator と JobFactory を受け取る)
	jobLauncher, jobOperator, jobFactory, initErr := batchInitializer.Initialize(ctx) // ★ 戻り値の順序と型を変更
	if initErr != nil {
		return nil, nil, nil, nil, exception.NewBatchError("app", "バッチアプリケーションの初期化に失敗しました", initErr, false, false)
	}
	logger.Infof("バッチアプリケーションの初期化が完了しました。")

	// JobFactory からデータベース接続を取得し、アプリケーションコンポーネントの登録に渡す
	sqlJobRepo, ok := batchInitializer.JobRepository.(*repository.SQLJobRepository)
	if !ok {
		return nil, nil, nil, nil, exception.NewBatchErrorf("app", "JobRepository の実装が予期された型ではありません。*sql.DB 接続を取得できません。")
	}
	dbConnection := sqlJobRepo.GetDB()
	if dbConnection == nil {
		return nil, nil, nil, nil, exception.NewBatchErrorf("app", "JobRepository からデータベース接続を取得できませんでした。")
	}

	registerApplicationComponents(jobFactory, batchInitializer.Config, dbConnection)

	return batchInitializer, jobLauncher, jobOperator, jobFactory, nil // ★ 戻り値の順序と型を変更
}

// executeJob は指定されたジョブを実行し、その結果に基づいて終了コードを返します。
func executeJob(ctx context.Context, jobLauncher joblauncher.JobLauncher, appConfig *config.Config) int { // ★ jobOperator を jobLauncher に変更
	jobName := appConfig.Batch.JobName
	if jobName == "" {
		logger.Errorf("設定ファイルにジョブ名が指定されていません。")
		return 1
	}
	logger.Infof("実行する Job: '%s'", jobName)

	// JobParameters を作成 (必要に応じてパラメータを設定)
	jobParams := core.NewJobParameters() // config.NewJobParameters() から core.NewJobParameters() に変更
	jobParams.Put("input.file", "/path/to/input.csv") // 例としてパラメータを設定
	jobParams.Put("output.dir", "/path/to/output")    // 例としてパラメータを設定
	jobParams.Put("process.date", time.Now().Format("2006-01-02")) // 例としてパラメータを設定

	// JobOperator を使用してジョブを起動
	jobExecution, startErr := jobLauncher.Launch(ctx, jobName, jobParams) // ★ JobLauncher.Launch を呼び出す

	if startErr != nil {
		return handleApplicationError(startErr, jobExecution, jobName)
	}

	if jobExecution == nil {
		logger.Errorf("JobOperator.Start がエラーなしで nil の JobExecution を返しました。")
		return 1
	}

	// ジョブの最終状態に基づいて終了コードを決定
	return handleApplicationError(nil, jobExecution, jobName)
}

// RunApplication はアプリケーションのメインロジックを実行します。
func RunApplication(ctx context.Context, envFilePath string, embeddedConfig, embeddedJSL []byte) int {
	// 設定の初期ロード (embeddedConfig を渡すため)
	initialCfg := &config.Config{
		EmbeddedConfig: embeddedConfig,
	}

	// BatchInitializer の生成
	batchInitializer := initializer.NewBatchInitializer(initialCfg)
	batchInitializer.JSLDefinitionBytes = embeddedJSL

	// バッチアプリケーションの初期化処理を実行 (JobOperator と JobFactory を受け取る)
	batchInitializer, jobLauncher, _, _, _ := setupApplication(ctx, envFilePath, embeddedConfig, embeddedJSL) // ★ 戻り値の順序と型を変更
	if batchInitializer == nil { // setupApplication がエラーを返した場合のチェック
		return 1
	}

	// 初期化完了後、リソースのクローズ処理を defer で登録
	defer func() {
		if closeErr := batchInitializer.Close(); closeErr != nil {
			logger.Errorf("バッチアプリケーションのリソースクローズ中にエラーが発生しました: %v", closeErr)
		} else {
			logger.Infof("バッチアプリケーションのリソースを正常にクローズしました。")
		}
	}()

	return executeJob(ctx, jobLauncher, batchInitializer.Config) // ★ jobOperator を jobLauncher に変更
}

// handleApplicationError はアプリケーションのエラーを処理し、適切な終了コードを返します。
func handleApplicationError(err error, jobExecution *core.JobExecution, jobName string) int {
	if err != nil {
		// ジョブ起動処理自体でエラーが発生した場合 (jobExecution が nil の可能性あり)
		if jobExecution != nil {
			logger.Errorf("Job '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
				jobName, jobExecution.ID, err)
			logger.Errorf("Job '%s' (Execution ID: %s) の最終状態: %s, ExitStatus: %s",
				jobName, jobExecution.ID, jobExecution.Status, jobExecution.ExitStatus)
		} else {
			logger.Errorf("Job '%s' の起動処理中にエラーが発生しました: %v", jobName, err)
		}

		// BatchError の詳細をログ出力
		if be, ok := err.(*exception.BatchError); ok {
			logger.Errorf("BatchError 詳細: Module=%s, Message=%s, OriginalErr=%v", be.Module, be.Message, be.OriginalErr)
			if be.StackTrace != "" {
				logger.Debugf("BatchError StackTrace:\n%s", be.StackTrace)
			}
		}
	}

	// err が nil でも jobExecution のステータスが失敗の場合
	if jobExecution != nil && (jobExecution.Status == core.BatchStatusFailed || jobExecution.Status == core.BatchStatusAbandoned) {
		logger.Errorf(
			"Job '%s' は失敗しました。詳細は JobExecution (ID: %s) およびログを確認してください。",
			jobExecution.JobName,
			jobExecution.ID,
		)
	}

	// JobExecution に記録された全ての失敗をログ出力
	if jobExecution != nil && len(jobExecution.Failures) > 0 {
		for i, f := range jobExecution.Failures {
			logger.Errorf("  - 失敗 %d: %v", i+1, f)
		}
	}

	// エラーがあった場合は終了コード 1 を返す
	if err != nil || (jobExecution != nil && (jobExecution.Status == core.BatchStatusFailed || jobExecution.Status == core.BatchStatusAbandoned)) {
		return 1
	}

	// 正常終了
	logger.Infof("アプリケーションを正常に完了しました。Job '%s' (Execution ID: %s) は %s で終了しました。", jobName, jobExecution.ID, jobExecution.Status)
	return 0
}
