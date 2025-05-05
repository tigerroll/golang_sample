package main

import (
  "context"
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/repository"
  "sample/src/main/go/batch/job"
  joblistener "sample/src/main/go/batch/job/listener" // job/listener パッケージをインポート
  "sample/src/main/go/batch/step/listener" // step/listener パッケージをインポート (ステップリスナーも引き続き使用する場合)
  "sample/src/main/go/batch/util/logger"
)

func main() {
  cfg, err := config.LoadConfig()
  if err != nil {
    logger.Fatalf("設定のロードに失敗しました: %v", err)
  }

  logger.SetLogLevel(cfg.System.Logging.Level)
  logger.Infof("ログレベルを '%s' に設定しました。", cfg.System.Logging.Level)

  logger.Debugf("Database Type: %s", cfg.Database.Type)
  logger.Debugf("Batch Polling Interval: %d", cfg.Batch.PollingIntervalSeconds)
  logger.Debugf("Batch API Endpoint: %s", cfg.Batch.APIEndpoint)

  ctx := context.Background()

  repo, err := repository.NewWeatherRepository(ctx, *cfg)
  if err != nil {
    logger.Fatalf("リポジトリの初期化に失敗しました: %v", err)
  }

  jobFactory := job.NewJobFactory(cfg, repo)

  jobName := cfg.Batch.JobName
  logger.Infof("実行する Job: '%s'", jobName)

  batchJob, err := jobFactory.CreateJob(jobName)
  if err != nil {
    logger.Fatalf("Job '%s' の作成に失敗しました: %v", jobName, err)
  }

  // JobExecutionListener を作成し、ジョブに登録 (新規追加)
  loggingJobListener := joblistener.NewLoggingJobListener()
  // Job インターフェースに RegisterJobListener がないため、WeatherJob にキャストして登録
  if weatherJob, ok := batchJob.(*job.WeatherJob); ok {
    weatherJob.RegisterJobListener(loggingJobListener)
    // 必要に応じて他の JobExecutionListener もここで登録
    // metricsJobListener := joblistener.NewMetricsJobListener(...)
    // weatherJob.RegisterJobListener(metricsJobListener)

    // ステップリスナーも引き続き登録
    retryListener := listener.NewRetryListener(cfg)
    weatherJob.RegisterStepListener("Reader", retryListener)
    weatherJob.RegisterStepListener("Processor", retryListener)
    weatherJob.RegisterStepListener("Writer", retryListener)

    loggingStepListener := listener.NewLoggingListener()
    weatherJob.RegisterStepListener("Reader", loggingStepListener)
    weatherJob.RegisterStepListener("Processor", loggingStepListener)
    weatherJob.RegisterStepListener("Writer", loggingStepListener)

  } else {
    logger.Warnf("Job が WeatherJob 型ではないため、JobExecutionListener は登録されません。")
  }

  // Job 実行
  if err := batchJob.Run(ctx); err != nil {
    // Run メソッド内でエラーログは出力されるので、ここではFatalfで終了のみ
    logger.Fatalf("Job '%s' の実行に失敗しました: %v", jobName, err)
  }

  logger.Infof("アプリケーションを終了します。")
}
