package app

import (
	"context"
	"database/sql"
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
	appJob "sample/example/weather/job"
	appTasklet "sample/example/weather/step/tasklet" // 新しい tasklet パッケージをインポート
	weatherprocessor "sample/example/weather/step/processor" // パッケージ名を変更
	weatherreader "sample/example/weather/step/reader"       // パッケージ名を変更
	weatherwriter "sample/example/weather/step/writer"       // パッケージ名を変更

	// pkg/batch に残る汎用コンポーネントのインポート
	executionContextItemReader "sample/pkg/batch/step/reader" // Renamed import
	executionContextItemWriter "sample/pkg/batch/step/writer" // Renamed import
	steplistener "sample/pkg/batch/step/listener" // stepListener をインポート
)

// registerApplicationComponents はアプリケーション固有のコンポーネントとジョブを JobFactory に登録します。
func registerApplicationComponents(jobFactory *factory.JobFactory, cfg *config.Config, db *sql.DB) {
	// Weather 関連コンポーネントの登録
	jobFactory.RegisterComponentBuilder("weatherItemReader", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewWeatherReader のシグネチャ変更に合わせて引数を渡す
		return weatherreader.NewWeatherReader(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("weatherItemProcessor", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewWeatherProcessor のシグネチャ変更に合わせて引数を渡す
		return weatherprocessor.NewWeatherProcessor(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("weatherItemWriter", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewWeatherWriter のシグネチャ変更に合わせて引数を渡す
		return weatherwriter.NewWeatherWriter(cfg, db, properties) // ★ 変更
	})

	// ダミーコンポーネントの登録
	jobFactory.RegisterComponentBuilder("dummyItemReader", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewDummyReader のシグネチャ変更に合わせて引数を渡す
		return weatherreader.NewDummyReader(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("dummyItemProcessor", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewDummyProcessor のシグネチャ変更に合わせて引数を渡す
		return weatherprocessor.NewDummyProcessor(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("dummyItemWriter", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewDummyWriter のシグネチャ変更に合わせて引数を渡す
		return weatherwriter.NewDummyWriter(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("executionContextItemReader", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewExecutionContextReader のシグネチャ変更に合わせて引数を渡す
		return executionContextItemReader.NewExecutionContextReader(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("executionContextItemWriter", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) { // Renamed component name
		// NewExecutionContextWriter のシグネチャ変更に合わせて引数を渡す
		return executionContextItemWriter.NewExecutionContextWriter(cfg, db, properties) // ★ 変更
	})
	jobFactory.RegisterComponentBuilder("dummyTasklet", func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error) {
		// NewDummyTasklet のシグネチャ変更に合わせて引数を渡す
		return appTasklet.NewDummyTasklet(cfg, db, properties) // ★ 変更
	})

	// Step-level listeners の登録
	jobFactory.RegisterStepExecutionListenerBuilder("loggingStepListener", func(cfg *config.Config) (steplistener.StepExecutionListener, error) {
		return steplistener.NewLoggingListener(&cfg.System.Logging), nil
	})
	jobFactory.RegisterStepExecutionListenerBuilder("retryStepListener", func(cfg *config.Config) (steplistener.StepExecutionListener, error) {
		return steplistener.NewRetryListener(&cfg.Batch.Retry), nil
	})

	// Item-level listeners の登録 (既存の実装のみ)
	jobFactory.RegisterSkipListenerBuilder("loggingSkipListener", func(cfg *config.Config) (steplistener.SkipListener, error) {
		return steplistener.NewLoggingSkipListener(), nil
	})
	jobFactory.RegisterRetryItemListenerBuilder("loggingRetryItemListener", func(cfg *config.Config) (steplistener.RetryItemListener, error) {
		return steplistener.NewLoggingRetryItemListener(), nil
	})
	// 新しいアイテムレベルリスナーの登録
	jobFactory.RegisterItemReadListenerBuilder("loggingItemReadListener", func(cfg *config.Config) (core.ItemReadListener, error) {
		return steplistener.NewLoggingItemReadListener(), nil
	})
	jobFactory.RegisterItemProcessListenerBuilder("loggingItemProcessListener", func(cfg *config.Config) (core.ItemProcessListener, error) {
		return steplistener.NewLoggingItemProcessListener(), nil
	})
	jobFactory.RegisterItemWriteListenerBuilder("loggingItemWriteListener", func(cfg *config.Config) (core.ItemWriteListener, error) {
		return steplistener.NewLoggingItemWriteListener(), nil
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
