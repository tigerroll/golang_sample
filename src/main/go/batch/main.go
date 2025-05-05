package main

import (
  "context"
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/job"
  "sample/src/main/go/batch/repository"
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
  defer repo.Close()

  jobFactory := job.NewJobFactory(cfg, repo)

  jobName := cfg.Batch.JobName
  logger.Infof("実行する Job: '%s'", jobName)

  batchJob, err := jobFactory.CreateJob(jobName)
  if err != nil {
    logger.Fatalf("Job '%s' の作成に失敗しました: %v", jobName, err)
  }

  if err := batchJob.Run(ctx); err != nil {
    logger.Fatalf("Job '%s' の実行に失敗しました: %v", jobName, err)
  }

  logger.Infof("アプリケーションを終了します。")
}
