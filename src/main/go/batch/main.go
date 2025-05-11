// src/main/go/batch/main.go
package main

import (
  "context"
  "os"

  config "sample/src/main/go/batch/config"
  job "sample/src/main/go/batch/job"
  core "sample/src/main/go/batch/job/core"
  factory "sample/src/main/go/batch/job/factory"
  "sample/src/main/go/batch/repository" // repository パッケージをインポート
  logger "sample/src/main/go/batch/util/logger"
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
  // logger.Debugf("Batch Polling Interval: %d", cfg.Batch.PollingIntervalSeconds) // 使用しないなら省略可
  logger.Debugf("Batch API Endpoint: %s", cfg.Batch.APIEndpoint)
  logger.Debugf("Batch Job Name: %s", cfg.Batch.JobName)
  logger.Debugf("Batch Chunk Size: %d", cfg.Batch.ChunkSize)
  logger.Debugf("Retry Max Attempts: %d", cfg.Batch.Retry.MaxAttempts)

  ctx := context.Background()

  // Step 1: Job Repository の生成
  // repository.NewJobRepository 関数を呼び出します。
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


  // JobFactory の生成
  // JobFactory が直接 JobRepository を必要とする場合は、ここで JobRepository を渡すように変更します。
  // 現時点では JobFactory は Config のみ必要なので変更なし。
  jobFactory := factory.NewJobFactory(cfg)

  jobName := cfg.Batch.JobName
  logger.Infof("実行する Job: '%s'", jobName)

  // JobFactory で Job オブジェクトを取得
  batchJob, err := jobFactory.CreateJob(jobName)
  if err != nil {
    logger.Fatalf("Job '%s' の作成に失敗しました: %v", jobName, err)
  }
    logger.Debugf("Job '%s' オブジェクトを作成しました。", jobName)


  // Step 3: JobLauncher を作成し、Job Repository を引き渡す
  // job.NewSimpleJobLauncher 関数が JobRepository を引数に取るように変更したので、ここで渡します。
  jobLauncher := job.NewSimpleJobLauncher(jobRepository)
    logger.Debugf("SimpleJobLauncher を Job Repository と共に作成しました。")


  // JobParameters を作成 (必要に応じてパラメータを設定)
  // 例えば、コマンドライン引数や環境変数からパラメータを読み込み、JobParameters に設定します。
  // 例: core.NewJobParameters(); jobParams.Put("input.file", "/path/to/input.txt")
  jobParams := core.NewJobParameters()
    // TODO: ここで JobParameters をロードするロジックを追加


  // JobLauncher を使用してジョブを起動
  logger.Infof("JobLauncher を使用して Job '%s' を起動します。", jobName)

  // Launch メソッドは JobExecution オブジェクトと、起動処理自体のエラーを返します。
    // ジョブ自体の実行エラーは JobExecution に記録される場合と、Launch の戻り値のエラーに含まれる場合があります。
  jobExecution, launchErr := jobLauncher.Launch(ctx, batchJob, jobParams)

    // Launch メソッドは常に JobExecution を返すように設計されているため、
    // Launch 処理自体にエラーがあっても jobExecution は nil でない場合があります。
    // launchErr は JobLauncher レベルのエラー（例: JobRepository との通信エラー）
    // または Job の Run メソッドが返したエラーを含みます。

  // JobLauncher の起動処理やジョブ実行中にエラーが発生したかチェック
  if launchErr != nil {
    // Launch メソッドがエラーを返した場合、それは Job 実行中に発生したエラーか、
        // JobLauncher 内部（JobRepository 関連など）のエラーです。
    logger.Errorf("Job '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
            jobName, jobExecution.ID, launchErr)

        // JobExecution の最終状態を再度確認 (Run メソッドや Update で状態が設定されているはず)
        logger.Errorf("Job '%s' (Execution ID: %s) の最終状態: %s",
             jobExecution.JobName, jobExecution.ID, jobExecution.Status)

        // JobExecution に記録された失敗例外もログ出力することを検討
        if len(jobExecution.Failureliye) > 0 {
            logger.Errorf("Job '%s' (Execution ID: %s) の失敗例外: %v",
                 jobExecution.JobName, jobExecution.ID, jobExecution.Failureliye)
        }

        // 失敗した場合は非ゼロ終了コードで終了
    os.Exit(1)
  }

  // Launch メソッドがエラーを返さなかった場合でも、JobExecution のステータスを確認して、
    // ジョブが論理的に成功したか判断します。
  logger.Infof("Job '%s' (Execution ID: %s) の最終状態: %s",
    jobExecution.JobName, jobExecution.ID, jobExecution.Status)

  // JobExecution の状態に基づいてアプリケーションの終了コードを制御
  if jobExecution.Status == core.JobStatusFailed || jobExecution.Status == core.JobStatusAbandoned {
    logger.Errorf(
      "Job '%s' は失敗しました。詳細は JobExecution (ID: %s) およびログを確認してください。",
      jobExecution.JobName,
            jobExecution.ID,
    )
        // JobExecution に記録された失敗例外もログ出力することを検討
        if len(jobExecution.Failureliye) > 0 {
            logger.Errorf("Job '%s' (Execution ID: %s) の失敗例外: %v",
                 jobExecution.JobName, jobExecution.ID, jobExecution.Failureliye)
        }

    // 失敗した場合は非ゼロ終了コードで終了
    os.Exit(1)
  }

  // ここに到達するのは JobExecution.Status が COMPLETED または STOPPED の場合などです。
  logger.Infof("アプリケーションを正常に完了しました。Job '%s' (Execution ID: %s) は %s で終了しました。",
        jobExecution.JobName, jobExecution.ID, jobExecution.Status)

  // 成功した場合はゼロ終了コードで終了
  os.Exit(0)
}
