package main

import (
  "context"
  "os"

  config "sample/src/main/go/batch/config"
  job "sample/src/main/go/batch/job" // job パッケージをインポート
  core "sample/src/main/go/batch/job/core"
  factory "sample/src/main/go/batch/job/factory" // factory パッケージをインポート
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
  jobFactory := factory.NewJobFactory(cfg, jobRepository) // JobRepository を渡す
  logger.Debugf("JobFactory を Job Repository と共に作成しました。")


  jobName := cfg.Batch.JobName
  logger.Infof("実行する Job: '%s'", jobName)

  // JobParameters を作成 (必要に応じてパラメータを設定)
  // 例えば、コマンドライン引数や環境変数からパラメータを読み込み、JobParameters に設定します。
  // 例: core.NewJobParameters(); jobParams.Put("input.file", "/path/to/input.txt")
  jobParams := core.NewJobParameters()
  // TODO: ここで JobParameters をロードするロジックを追加


  // Step 4: JobOperator を作成し、Job Repository と JobFactory を引き渡す
  // job.NewDefaultJobOperator 関数が JobRepository と JobFactory を引数に取るように変更したので、ここで渡します。
  // jobFactory はポインタ (*factory.JobFactory) なので、デリファレンスして渡します。
  jobOperator := job.NewDefaultJobOperator(jobRepository, *jobFactory) // ★ ここを修正: *jobFactory
  logger.Debugf("DefaultJobOperator を Job Repository および JobFactory と共に作成しました。")


  // Step 5: JobOperator を使用してジョブを起動
  // JobOperator.Start メソッドは jobName と jobParams を直接受け取ります。
  // Launch メソッドは JobExecution オブジェクトと、起動処理自体のエラーを返します。
  // ジョブ自体の実行エラーは JobExecution に記録される場合と、Start の戻り値のエラーに含まれる場合があります。
  jobExecution, startErr := jobOperator.Start(ctx, jobName, jobParams)

  // Start メソッドがエラーを返した場合のハンドリングを修正
  if startErr != nil {
    // Start メソッドがエラーを返した場合、それは Job 実行中に発生したエラーか、
    // JobOperator 内部（Job Repository 関連、JobFactory 関連など）のエラーです。
    // panic を防ぐため、jobExecution が nil でないかチェックします。
    if jobExecution != nil {
      logger.Errorf("Job '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
        jobName, jobExecution.ID, startErr)

      // JobExecution の最終状態を再度確認 (Run メソッドや Operator の Update で状態が設定されているはず)
      logger.Errorf("Job '%s' (Execution ID: %s) の最終状態: %s",
        jobExecution.JobName, jobExecution.ID, jobExecution.Status)

      // JobExecution に記録された失敗例外もログ出力することを検討
      if len(jobExecution.Failureliye) > 0 {
        logger.Errorf("Job '%s' (Execution ID: %s) の失敗例外: %v",
          jobExecution.JobName, jobExecution.ID, jobExecution.Failureliye)
      }
    } else {
      // jobExecution が nil の場合は、JobExecution の作成以前にエラーが発生した可能性が高い
      logger.Errorf("Job '%s' の起動処理中にエラーが発生しました: %v", jobName, startErr)
      // この場合、JobExecution は存在しないため、その後の JobExecution を参照するログ出力はスキップ
    }


    // 失敗した場合は非ゼロ終了コードで終了
    os.Exit(1)
  }

  // Start メソッドがエラーを返さなかった場合でも、JobExecution のステータスを確認して、
  // ジョブが論理的に成功したか判断します。
  // ここに到達した場合、jobExecution は nil ではないはずですが、念のためチェックすることも可能です。
  if jobExecution == nil {
    // 通常はここに到達しないはずですが、もし jobExecution が nil なら致命的な問題
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
