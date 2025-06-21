package main

import (
	"context"
	"os"

	config "sample/src/main/go/batch/config"
	job "sample/src/main/go/batch/job" // job パッケージをインポート
	core "sample/src/main/go/batch/job/core"
	factory "sample/src/main/go/batch/job/factory" // factory パッケージをインポート
	repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
	logger "sample/src/main/go/batch/util/logger"

	// JSLでコンポーネントを動的に解決するため、Reader/Processor/Writerのパッケージをインポート
	_ "sample/src/main/go/batch/step/processor" // NewWeatherProcessor が参照されるためインポート
	_ "sample/src/main/go/batch/step/reader"    // NewWeatherReader が参照されるためインポート
	_ "sample/src/main/go/batch/step/writer"    // NewWeatherWriter が参照されるためインポート
	_ "sample/src/main/go/batch/step/processor" // dummy_processor.go がこのパッケージに属する
	_ "sample/src/main/go/batch/step/reader"    // dummy_reader.go がこのパッケージに属する
	_ "sample/src/main/go/batch/step/writer"    // dummy_writer.go がこのパッケージに属する
)

func main() {
	// 設定のロード
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("設定のロードに失敗しました: %v", err)
	}

	logger.SetLogLevel(cfg.System.Logging.Level)
	logger.Infof("ログレベルを '%s' に設定しました。", cfg.System.Logging.Level)

	// 必要に応じて他の設定値もログ出力
	logger.Debugf("Database Type: %s", cfg.Database.Type)
	logger.Debugf("Batch API Endpoint: %s", cfg.Batch.APIEndpoint)
	logger.Debugf("Batch Job Name: %s", cfg.Batch.JobName)
	logger.Debugf("Batch Chunk Size: %d", cfg.Batch.ChunkSize)
	logger.Debugf("Retry Max Attempts: %d", cfg.Batch.Retry.MaxAttempts)

	ctx := context.Background()

	// Step 1: Job Repository の生成
	jobRepository, err := repository.NewJobRepository(ctx, *cfg)
	if err != nil {
		logger.Fatalf("Job Repository の生成に失敗しました: %v", err)
	}
	// Step 2: アプリケーション終了時に Job Repository をクローズするように defer を設定
	defer func() {
		closeErr := jobRepository.Close()
		if closeErr != nil {
			logger.Errorf("Job Repository のクローズに失敗しました: %v", closeErr)
		} else {
			logger.Infof("Job Repository を正常にクローズしました。")
		}
	}()
	logger.Infof("Job Repository を生成しました。")


	// Step 3: JobFactory の生成
	// JobFactory が JobRepository を必要とするように変更したので、ここで JobRepository を渡します。
	// componentRegistry は JobFactory 内部で構築されるため、ここでは不要
	jobFactory := factory.NewJobFactory(cfg, jobRepository)
	logger.Debugf("JobFactory を Job Repository と共に作成しました。")


	// 実行するジョブ名を指定 (JSLファイルで定義されたID)
	// 例: src/main/resources/job/weather.yaml に定義されたジョブID "weatherJob" を使用
	jobName := cfg.Batch.JobName // configから取得したジョブ名がJSLのIDと一致すると仮定
	logger.Infof("実行する Job: '%s'", jobName)

	// JobParameters を作成 (必要に応じてパラメータを設定)
	jobParams := core.NewJobParameters()
	// TODO: ここで JobParameters をロードするロジックを追加


	// Step 4: JobOperator を作成し、Job Repository と JobFactory を引き渡す
	jobOperator := job.NewDefaultJobOperator(jobRepository, *jobFactory)
	logger.Debugf("DefaultJobOperator を Job Repository および JobFactory と共に作成しました。")


	// Step 5: JobOperator を使用してジョブを起動
	// JobOperator.Start メソッドは jobName と jobParams を直接受け取ります。
	jobExecution, startErr := jobOperator.Start(ctx, jobName, jobParams)

	// Start メソッドがエラーを返した場合のハンドリングを修正
	if startErr != nil {
		if jobExecution != nil {
			logger.Errorf("Job '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
				jobName, jobExecution.ID, startErr)

			logger.Errorf("Job '%s' (Execution ID: %s) の最終状態: %s",
				jobExecution.JobName, jobExecution.ID, jobExecution.Status)

			if len(jobExecution.Failureliye) > 0 {
				logger.Errorf("Job '%s' (Execution ID: %s) の失敗例外: %v",
					jobExecution.JobName, jobExecution.ID, jobExecution.Failureliye)
			}
		} else {
			logger.Errorf("Job '%s' の起動処理中にエラーが発生しました: %v", jobName, startErr)
		}

		os.Exit(1)
	}

	if jobExecution == nil {
		logger.Fatalf("JobOperator.Start がエラーなしで nil の JobExecution を返しました。")
	}

	logger.Infof("Job '%s' (Execution ID: %s) の最終状態: %s",
		jobExecution.JobName, jobExecution.ID, jobExecution.Status)

	// JobExecution の状態に基づいてアプリケーションの終了コードを制御
	if jobExecution.Status == core.JobStatusFailed || jobExecution.Status == core.JobStatusAbandoned {
		logger.Errorf(
			"Job '%s' は失敗しました。詳細は JobExecution (ID: %s) およびログを確認してください。",
			jobExecution.JobName,
			jobExecution.ID,
		)
		if len(jobExecution.Failureliye) > 0 {
			logger.Errorf("Job '%s' (Execution ID: %s) の失敗例外: %v",
				jobExecution.JobName, jobExecution.ID, jobExecution.Failureliye)
		}

		os.Exit(1)
	}

	logger.Infof("アプリケーションを正常に完了しました。Job '%s' (Execution ID: %s) は %s で終了しました。",
		jobExecution.JobName, jobExecution.ID, jobExecution.Status)

	os.Exit(0)
}
