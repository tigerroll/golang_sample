package main

import (
  "context"
  "os"

  config   "sample/src/main/go/batch/config"
  job      "sample/src/main/go/batch/job"
  core     "sample/src/main/go/batch/job/core"
  factory  "sample/src/main/go/batch/job/factory"
  logger   "sample/src/main/go/batch/util/logger"
)

func main() {
  // 設定のロード
  // エラーが発生した場合はログを出力して終了
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

  // JobFactory の生成
  jobFactory := factory.NewJobFactory(cfg)

  jobName := cfg.Batch.JobName
  logger.Infof("実行する Job: '%s'", jobName)

  // JobFactory で Job オブジェクトを取得
  batchJob, err := jobFactory.CreateJob(jobName)
  if err != nil {
    // Job の作成に失敗した場合もログを出力して終了
    logger.Fatalf("Job '%s' の作成に失敗しました: %v", jobName, err)
  }

  // JobLauncher を作成
  jobLauncher := job.NewSimpleJobLauncher()

  // JobParameters を作成 (必要に応じてパラメータを設定)
  jobParams := core.NewJobParameters()

  // JobLauncher を使用してジョブを起動
  logger.Infof("JobLauncher を使用して Job '%s' を起動します。", jobName)

  // Launch メソッドが JobExecution と起動処理自体のエラーを返す
  jobExecution, launchErr := jobLauncher.Launch(ctx, batchJob, jobParams)

  // JobLauncher の起動処理自体にエラーがないかチェック
  if launchErr != nil {
    // JobLauncher の起動に失敗した場合もログを出力して終了
    logger.Fatalf("JobLauncher の起動に失敗しました: %v", launchErr)
  }

  // JobExecution の最終状態を確認し、結果をログ出力
  logger.Infof("Job '%s' (Execution ID: %s) の最終状態: %s",
    jobExecution.JobName, jobExecution.ID, jobExecution.Status)

  // JobExecution の状態に基づいてアプリケーションの終了コードを制御
  if jobExecution.Status == core.JobStatusFailed {
    logger.Errorf(
      "Job '%s' は失敗しました。詳細は JobExecution を確認してください。",
      jobExecution.JobName,
    )
    // 失敗した場合は非ゼロ終了コードで終了
    os.Exit(1)
  }

  logger.Infof("アプリケーションを正常に完了しました。")
  // 成功した場合はゼロ終了コードで終了
  os.Exit(0)
}
