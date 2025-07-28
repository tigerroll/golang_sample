package app

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	config "sample/pkg/batch/config"
	batch_factory "sample/pkg/batch/job/factory"
	batch_joblistener "sample/pkg/batch/job/listener"
	initializer "sample/pkg/batch/initializer"
	batch_repository "sample/pkg/batch/repository"
	exception "sample/pkg/batch/util/exception"
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
func registerApplicationComponents(jobFactory *batch_factory.JobFactory, cfg *config.Config, db *sql.DB) {
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

	logger.Debugf("全てのアプリケーションコンポーネントビルダーを登録しました。")

	// Weather Job のビルダー登録
	jobFactory.RegisterJobBuilder("weather", func(
		jobRepository batch_repository.JobRepository,
		cfg *config.Config,
		listeners []batch_joblistener.JobExecutionListener,
		flow *core.FlowDefinition,
	) (core.Job, error) {
		return appJob.NewWeatherJob(jobRepository, cfg, listeners, flow), nil
	})

	logger.Debugf("全てのアプリケーションジョブビルダーを登録しました。")
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
	jobOperator, jobFactory, initErr := batchInitializer.Initialize(ctx, envFilePath)
	if initErr != nil {
		logger.Errorf("バッチアプリケーションの初期化に失敗しました: %v", exception.NewBatchError("app", "バッチアプリケーションの初期化に失敗しました", initErr, false, false))
		return 1
	}
	logger.Infof("バッチアプリケーションの初期化が完了しました。")

	// JobFactory からデータベース接続を取得し、アプリケーションコンポーネントの登録に渡す
	sqlJobRepo, ok := batchInitializer.JobRepository.(*batch_repository.SQLJobRepository)
	if !ok {
		logger.Errorf("JobRepository の実装が予期された型ではありません。*sql.DB 接続を取得できません。")
		return 1
	}
	registerApplicationComponents(jobFactory, batchInitializer.Config, sqlJobRepo.GetDB())

	// 初期化完了後、リソースのクローズ処理を defer で登録
	defer func() {
		if closeErr := batchInitializer.Close(); closeErr != nil {
			logger.Errorf("バッチアプリケーションのリソースクローズ中にエラーが発生しました: %v", closeErr)
		} else {
			logger.Infof("バッチアプリケーションのリソースを正常にクローズしました。")
		}
	}()

	// 実行するジョブ名を設定ファイルから取得
	jobName := batchInitializer.Config.Batch.JobName
	if jobName == "" {
		logger.Errorf("設定ファイルにジョブ名が指定されていません。")
		return 1
	}
	logger.Infof("実行する Job: '%s'", jobName)

	// JobParameters を作成 (必要に応じてパラメータを設定)
	jobParams := config.NewJobParameters()
	jobParams.Put("input.file", "/path/to/input.csv")
	jobParams.Put("output.dir", "/path/to/output")
	jobParams.Put("process.date", time.Now().Format("2006-01-02"))

	// JobOperator を使用してジョブを起動
	jobExecution, startErr := jobOperator.Start(ctx, jobName, jobParams)

	// Start メソッドがエラーを返した場合のハンドリング
	if startErr != nil {
		if jobExecution != nil {
			logger.Errorf("Job '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
				jobName, jobExecution.ID, startErr)

			logger.Errorf("Job '%s' (Execution ID: %s) の最終状態: %s, ExitStatus: %s",
				jobExecution.JobName, jobExecution.ID, jobExecution.Status, jobExecution.ExitStatus)

			if len(jobExecution.Failures) > 0 {
				for i, f := range jobExecution.Failures {
					logger.Errorf("  - 失敗 %d: %v", i+1, f)
				}
			}
		} else {
			logger.Errorf("Job '%s' の起動処理中にエラーが発生しました: %v", jobName, startErr)
		}

		if be, ok := startErr.(*exception.BatchError); ok {
			logger.Errorf("BatchError 詳細: Module=%s, Message=%s, OriginalErr=%v", be.Module, be.Message, be.OriginalErr)
			if be.StackTrace != "" {
				logger.Debugf("BatchError StackTrace:\n%s", be.StackTrace)
			}
		}
		return 1
	}

	if jobExecution == nil {
		logger.Errorf("JobOperator.Start がエラーなしで nil の JobExecution を返しました。")
		return 1
	}

	logger.Infof("Job '%s' (Execution ID: %s) の最終状態: %s",
		jobExecution.JobName, jobExecution.ID, jobExecution.Status)

	// JobExecution の状態に基づいてアプリケーションの終了コードを制御
	if jobExecution.Status == core.BatchStatusFailed || jobExecution.Status == core.BatchStatusAbandoned {
		logger.Errorf(
			"Job '%s' は失敗しました。詳細は JobExecution (ID: %s) およびログを確認してください。",
			jobExecution.JobName,
			jobExecution.ID,
		)
		if len(jobExecution.Failures) > 0 {
			for i, f := range jobExecution.Failures {
				logger.Errorf("  - 失敗 %d: %v", i+1, f)
			}
		}
		return 1
	}

	logger.Infof("アプリケーションを正常に完了しました。Job '%s' (Execution ID: %s) は %s で終了しました。",
		jobExecution.JobName, jobExecution.ID, jobExecution.Status)

	return 0
}
