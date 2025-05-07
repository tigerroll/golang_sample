package main

import (
  "context"

  config   "sample/src/main/go/batch/config"
  job      "sample/src/main/go/batch/job"
  core     "sample/src/main/go/batch/job/core"
  factory  "sample/src/main/go/batch/job/factory"
  logger   "sample/src/main/go/batch/util/logger"
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
  jobFactory := factory.NewJobFactory(cfg)

  jobName := cfg.Batch.JobName
  logger.Infof("実行する Job: '%s'", jobName)

  // JobFactory/weather_factory で JobExecutionListener も登録され、
  // リポジトリも生成された Job オブジェクトが返される
  // CreateJob の戻り値の型は core.Job に変更
  batchJob, err := jobFactory.CreateJob(jobName)
  if err != nil {
    logger.Fatalf("Job '%s' の作成に失敗しました: %v", jobName, err)
  }

  // main 関数から JobExecutionListener の生成・登録ロジックを削除 (JobFactory/weather_factory に移動)

  // JobLauncher を作成 (必要であれば依存関係を渡す)
  jobLauncher := job.NewSimpleJobLauncher() // job.NewSimpleJobLauncher を呼び出し

  // JobParameters を作成 (必要に応じてパラメータを設定)
  jobParams := core.NewJobParameters() // core.NewJobParameters を呼び出し
  // jobParams.StartDate = "2023-01-01" // 例

  // JobLauncher を使用してジョブを起動
  logger.Infof("JobLauncher を使用して Job '%s' を起動します。", jobName)
  // Launch メソッドの引数と戻り値の型は core パッケージから参照
  jobExecution, launchErr := jobLauncher.Launch(ctx, batchJob, jobParams)

  // JobLauncher の起動処理自体にエラーがないかチェック
  if launchErr != nil {
    logger.Fatalf("JobLauncher の起動に失敗しました: %v", launchErr)
  }

  // JobExecution の最終状態を確認し、結果をログ出力
  logger.Infof("Job '%s' (Execution ID: %s) の最終状態: %s",
    jobExecution.JobName, jobExecution.ID, jobExecution.Status) // JobExecution, Status は core パッケージから

  // JobExecution の状態に基づいてアプリケーションの終了コードなどを制御することも可能
  if jobExecution.Status == core.JobStatusFailed { // core.JobStatusFailed を参照
    logger.Fatalf("Job '%s' は失敗しました。詳細は JobExecution を確認してください。", jobExecution.JobName)
  }

  logger.Infof("アプリケーションを終了します。")
}
