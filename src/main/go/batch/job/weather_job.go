package job

import (
  "context"
  "fmt"
  "time"

  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  jobListener "sample/src/main/go/batch/job/listener"
  repository "sample/src/main/go/batch/repository"
  stepListener "sample/src/main/go/batch/step/listener" // stepListener パッケージをインポート
  stepProcessor "sample/src/main/go/batch/step/processor"
  stepReader "sample/src/main/go/batch/step/reader"
  stepWriter "sample/src/main/go/batch/step/writer"
  logger "sample/src/main/go/batch/util/logger"
)

type WeatherJob struct {
  repo repository.WeatherRepository
  // 具体的な構造体ではなく、インターフェース型を使用
  reader stepReader.Reader
  processor stepProcessor.Processor
  writer stepWriter.Writer
  config *config.Config
  // stepExecutionListener を stepListener に修正
  stepListeners map[string][]stepListener.StepExecutionListener
  jobListeners []jobListener.JobExecutionListener
}

// WeatherJob が core.Job インターフェースを満たすことを宣言 (明示的な実装宣言は不要だが意図を示す)
var _ core.Job = (*WeatherJob)(nil)

// NewWeatherJob 関数がインターフェース型を引数として受け取るように修正
func NewWeatherJob(
  repo repository.WeatherRepository,
  reader stepReader.Reader, // インターフェース型
  processor stepProcessor.Processor, // インターフェース型
  writer stepWriter.Writer, // インターフェース型
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo: repo,
    reader: reader,
    processor: processor,
    writer: writer,
    config: cfg,
    // stepExecutionListener を stepListener に修正
    stepListeners: make(map[string][]stepListener.StepExecutionListener),
    jobListeners: make([]jobListener.JobExecutionListener, 0),
  }
}

// RegisterStepListener は特定のステップに StepExecutionListener を登録します。
// stepExecutionListener を stepListener に修正
func (j *WeatherJob) RegisterStepListener(stepName string, l stepListener.StepExecutionListener) {
  if _, ok := j.stepListeners[stepName]; !ok {
    // stepExecutionListener を stepListener に修正
    j.stepListeners[stepName] = make([]stepListener.StepExecutionListener, 0)
  }
  j.stepListeners[stepName] = append(j.stepListeners[stepName], l)
}

// RegisterJobListener は JobExecutionListener を登録します。
func (j *WeatherJob) RegisterJobListener(l jobListener.JobExecutionListener) {
  j.jobListeners = append(j.jobListeners, l)
}

// ジョブリスナーへの通知メソッド
// notifyBeforeJob メソッドシグネチャを変更し、JobExecution を受け取るように修正
func (j *WeatherJob) notifyBeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.BeforeJob(ctx, jobExecution) // JobExecution を渡す
  }
}

// ジョブリスナーへの通知メソッド
// notifyAfterJob メソッドシグネチャを変更し、JobExecution を受け取るように修正
func (j *WeatherJob) notifyAfterJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.AfterJob(ctx, jobExecution) // JobExecution を渡す
  }
}

// ステップリスナーへの通知メソッド
// notifyBeforeStep メソッドシグネチャを変更し、StepExecution を受け取るように修正
func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
  if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
    for _, l := range stepListeners {
      l.BeforeStep(ctx, stepExecution) // StepExecution を渡す
    }
  }
}

// ステップリスナーへの通知メソッド
// notifyAfterStep メソッドシグネチャを変更し、StepExecution を受け取るように修正
func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
  if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
    for _, l := range stepListeners {
      // LoggingListener の AfterStepWithDuration を特別に呼び出す場合
      // LoggingListener は StepExecution を受け取るように既に修正済み
      if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepExecution) // StepExecution を渡す
      } else {
        // StepExecutionListener インターフェースの AfterStep メソッドに Context と StepExecution を渡して呼び出し
        l.AfterStep(ctx, stepExecution) // StepExecution を渡す
      }
    }
  }
}


// Run メソッド (core.Job インターフェースの実装)
// Run メソッドシグネチャを変更し、JobExecution を受け取るように修正
func (j *WeatherJob) Run(ctx context.Context, jobExecution *core.JobExecution) error {
  retryConfig := j.config.Batch.Retry
  // 変数宣言を関数の先頭に移動
  var readItem interface{}
  var processedItem interface{}
  var readerErr error
  var processorErr error
  var writerErr error
  var jobErr error // ジョブ全体の最終的なエラーを保持する変数

  // StepExecution 変数も関数の先頭に宣言
  var readerStepExecution *core.StepExecution
  var processorStepExecution *core.StepExecution
  var writerStepExecution *core.StepExecution


  // ジョブ開始前リスナーを呼び出し
  j.notifyBeforeJob(ctx, jobExecution) // JobExecution を渡す

  // ジョブ完了後に必ず JobExecutionListener の AfterJob を呼び出し、リポジトリをクローズするための defer
  defer func() {
    // JobExecution の最終状態をここで設定 (または各ステップの終了時に設定)
    // ジョブ全体の最終的なエラーを JobExecution に追加
    if jobErr != nil {
       // MarkAsFailed はすでに jobErr を JobExecution.Failureliye に追加する
       // jobExecution.AddFailureException(jobErr) // 重複する場合があるので、MarkAsFailedに任せる
       jobExecution.MarkAsFailed(jobErr)
    } else {
      jobExecution.MarkAsCompleted()
    }

    // JobExecutionListener の AfterJob を呼び出し
    j.notifyAfterJob(ctx, jobExecution) // JobExecution を渡す

    // リポジトリのクローズ処理
    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
        // リポジトリクローズエラーもジョブ全体のエラーとして記録
        jobExecution.AddFailureException(fmt.Errorf("リポジトリのクローズエラー: %w", err))
        if jobErr == nil {
            jobErr = fmt.Errorf("リポジトリのクローズエラー: %w", err) // ジョブエラーがなければ設定
        }
      }
    }
  }()

  logger.Infof("Weather Job を開始します。")

  // ステップごとのリトライループ (各ステップ内でリトライを管理)
  // StepExecution を各ステップの実行ごとに生成・更新します。

  // Reader ステップ
  readerStepExecution = core.NewStepExecution("Reader", jobExecution) // StepExecution を生成

  for attempt := 0; attempt < retryConfig.MaxAttempts; attempt++ {
    // ループ全体でも Context の完了をチェック
    select {
    case <-ctx.Done():
      readerErr = ctx.Err()
      readerStepExecution.MarkAsFailed(readerErr) // ステップ実行を失敗としてマーク
      logger.Warnf("Context がキャンセルされたため、リーダー ステップの実行を中断します: %v", readerErr)
      goto endJobSteps // 全体のステップ処理を中断
    default:
    }

    // ステップ実行開始時刻を設定し、状態を更新
    readerStepExecution.StartTime = time.Now()
    readerStepExecution.MarkAsStarted() // ステップ実行を開始としてマーク
    j.notifyBeforeStep(ctx, readerStepExecution) // StepExecution を渡す

    // Reader の Read メソッドを呼び出し (インターフェース経由)
    readItem, readerErr = j.reader.Read(ctx)
    readerStepExecution.EndTime = time.Now() // ステップ終了時間を設定

    if readerErr != nil {
      readerStepExecution.AddFailureException(readerErr) // エラーを StepExecution に追加
      logger.Errorf("データの読み込みに失敗しました (リトライ %d): %v", attempt+1, readerErr)
      j.notifyAfterStep(ctx, readerStepExecution) // エラー情報を含む StepExecution を渡す

      if attempt < retryConfig.MaxAttempts-1 {
        // リトライ待ち時間
        select {
        case <-time.After(time.Duration(retryConfig.InitialInterval) * time.Second):
          // スリープ完了
        case <-ctx.Done():
          readerErr = ctx.Err() // Context キャンセル
          readerStepExecution.MarkAsFailed(readerErr) // ステップ実行を失敗としてマーク
          goto endJobSteps // 全体のステップ処理を中断
        }
        // attempt++ は for ループで行われる
        continue // リトライ
      } else {
        // リトライ上限に達した場合
        readerStepExecution.MarkAsFailed(readerErr) // ステップ実行を失敗としてマーク
        jobErr = fmt.Errorf("データの読み込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, readerErr)
        logger.Errorf("Reader ステップが最大リトライ回数に達し、失敗しました。")
        j.notifyAfterStep(ctx, readerStepExecution) // 最終的な StepExecution を渡す
        goto endJobSteps // 全体のステップ処理を中断
      }
    } else {
      // ステップ実行成功、状態を更新
      readerStepExecution.MarkAsCompleted() // ステップ実行を完了としてマーク
      logger.Debugf("リーダーからデータを読み込みました: %+v", readItem)

      // Reader ステップの実行コンテキストに読み込み件数を記録する例
      // 実際の ItemReader は読み込んだ件数を返すように修正が必要
      // ここではダミー値を使用
      readerStepExecution.ExecutionContext.Put("readCount", 100)
      // JobExecution のコンテキストに読み込み件数を昇格させる例
      jobExecution.ExecutionContext.Put("totalReadCount", 100) // Jobレベルで共有

      j.notifyAfterStep(ctx, readerStepExecution) // 成功した StepExecution を渡す
      break // Reader ステップ成功、ループを抜ける
    }
  }

  if readerErr != nil {
    // Reader ステップでエラーが発生して中断した場合、後続ステップは実行しない
    jobErr = readerErr // ジョブ全体のエラーとする
    goto endJobSteps
  }

  // Processor ステップ
  processorStepExecution = core.NewStepExecution("Processor", jobExecution) // StepExecution を生成

  for attempt := 0; attempt < retryConfig.MaxAttempts; attempt++ {
    select {
    case <-ctx.Done():
      processorErr = ctx.Err()
      processorStepExecution.MarkAsFailed(processorErr) // ステップ実行を失敗としてマーク
      logger.Warnf("Context がキャンセルされたため、プロセッサー ステップの実行を中断します: %v", processorErr)
      goto endJobSteps // 全体のステップ処理を中断
    default:
    }

    // ステップ実行開始時刻を設定し、状態を更新
    processorStepExecution.StartTime = time.Now()
    processorStepExecution.MarkAsStarted() // ステップ実行を開始としてマーク
    j.notifyBeforeStep(ctx, processorStepExecution) // StepExecution を渡す

    // Processor の Process メソッドを呼び出し (インターフェース経由)
    processedItem, processorErr = j.processor.Process(ctx, readItem) // 読み込んだアイテムを渡す
    processorStepExecution.EndTime = time.Now() // ステップ終了時間を設定

    if processorErr != nil {
      processorStepExecution.AddFailureException(processorErr) // エラーを StepExecution に追加
      logger.Errorf("データの加工に失敗しました (リトライ %d): %v", attempt+1, processorErr)
      j.notifyAfterStep(ctx, processorStepExecution) // エラー情報を含む StepExecution を渡す

      if attempt < retryConfig.MaxAttempts-1 {
        // リトライ待ち時間
        select {
        case <-time.After(time.Duration(retryConfig.InitialInterval) * time.Second):
          // スリープ完了
        case <-ctx.Done():
          processorErr = ctx.Err() // Context キャンセル
          processorStepExecution.MarkAsFailed(processorErr) // ステップ実行を失敗としてマーク
          goto endJobSteps // 全体のステップ処理を中断
        }
        // attempt++ は for ループで行われる
        continue // リトライ
      } else {
        // リトライ上限に達した場合
        processorStepExecution.MarkAsFailed(processorErr) // ステップ実行を失敗としてマーク
        jobErr = fmt.Errorf("データの加工に最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, processorErr)
        logger.Errorf("Processor ステップが最大リトライ回数に達し、失敗しました。")
        j.notifyAfterStep(ctx, processorStepExecution) // 最終的な StepExecution を渡す
        goto endJobSteps // 全体のステップ処理を中断
      }
    } else {
      // ステップ実行成功、状態を更新
      processorStepExecution.MarkAsCompleted() // ステップ実行を完了としてマーク
      logger.Debugf("プロセッサーでデータを加工しました: %+v", processedItem)

      // Processor ステップの実行コンテキストに加工件数を記録する例
      // 実際の ItemProcessor は加工したアイテム数を返すように修正が必要
      // ここではダミー値を使用
      processorStepExecution.ExecutionContext.Put("processedCount", 100)
      // JobExecution のコンテキストに加工件数を昇格させる例
      jobExecution.ExecutionContext.Put("totalProcessedCount", 100) // Jobレベルで共有

      // JobExecution のコンテキストから情報を読み取る例
      if totalReadCount, ok := jobExecution.ExecutionContext.GetInt("totalReadCount"); ok {
        logger.Debugf("JobExecution から読み込み件数 (totalReadCount): %d を取得しました。", totalReadCount)
      }

      j.notifyAfterStep(ctx, processorStepExecution) // 成功した StepExecution を渡す
      break // Processor ステップ成功、ループを抜ける
    }
  }

  if processorErr != nil {
    // Processor ステップでエラーが発生して中断した場合、後続ステップは実行しない
    jobErr = processorErr // ジョブ全体のエラーとする
    goto endJobSteps
  }

  // Writer ステップ
  writerStepExecution = core.NewStepExecution("Writer", jobExecution) // StepExecution を生成

  for attempt := 0; attempt < retryConfig.MaxAttempts; attempt++ {
    select {
    case <-ctx.Done():
      writerErr = ctx.Err()
      writerStepExecution.MarkAsFailed(writerErr) // ステップ実行を失敗としてマーク
      logger.Warnf("Context がキャンセルされたため、ライター ステップの実行を中断します: %v", writerErr)
      goto endJobSteps // 全体のステップ処理を中断
    default:
    }

    // ステップ実行開始時刻を設定し、状態を更新
    writerStepExecution.StartTime = time.Now()
    writerStepExecution.MarkAsStarted() // ステップ実行を開始としてマーク
    j.notifyBeforeStep(ctx, writerStepExecution) // StepExecution を渡す

    // Writer の Write メソッドを呼び出し (インターフェース経由)
    writerErr = j.writer.Write(ctx, processedItem) // 処理済みアイテムを渡す
    writerStepExecution.EndTime = time.Now() // ステップ終了時間を設定

    if writerErr != nil {
      writerStepExecution.AddFailureException(writerErr) // エラーを StepExecution に追加
      logger.Errorf("データの書き込みに失敗しました (リトライ %d): %v", attempt+1, writerErr)
      j.notifyAfterStep(ctx, writerStepExecution) // エラー情報を含む StepExecution を渡す

      if attempt < retryConfig.MaxAttempts-1 {
        // リトライ待ち時間
        select {
        case <-time.After(time.Duration(retryConfig.InitialInterval) * time.Second):
          // スリープ完了
        case <-ctx.Done():
          writerErr = ctx.Err() // Context キャンセル
          writerStepExecution.MarkAsFailed(writerErr) // ステップ実行を失敗としてマーク
          goto endJobSteps // 全体のステップ処理を中断
        }
        // attempt++ は for ループで行われる
        continue // リトライ
      } else {
        // リトライ上限に達した場合
        writerStepExecution.MarkAsFailed(writerErr) // ステップ実行を失敗としてマーク
        jobErr = fmt.Errorf("データの書き込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, writerErr)
        logger.Errorf("Writer ステップが最大リトライ回数に達し、失敗しました。")
        j.notifyAfterStep(ctx, writerStepExecution) // 最終的な StepExecution を渡す
        goto endJobSteps // 全体のステップ処理を中断
      }
    } else {
      // ステップ実行成功、状態を更新
      writerStepExecution.MarkAsCompleted() // ステップ実行を完了としてマーク
      logger.Infof("ライターでデータを書き込みました。")

      // Writer ステップの実行コンテキストに書き込み件数を記録する例
      // 実際の ItemWriter は書き込んだアイテム数を返すように修正が必要
      // ここではダミー値を使用
      writerStepExecution.ExecutionContext.Put("writeCount", 100)
      // JobExecution のコンテキストに書き込み件数を昇格させる例
      jobExecution.ExecutionContext.Put("totalWriteCount", 100) // Jobレベルで共有

      // JobExecution のコンテキストから情報を読み取る例
      if totalProcessedCount, ok := jobExecution.ExecutionContext.GetInt("totalProcessedCount"); ok {
        logger.Debugf("JobExecution から加工件数 (totalProcessedCount): %d を取得しました。", totalProcessedCount)
      }


      j.notifyAfterStep(ctx, writerStepExecution) // 成功した StepExecution を渡す
      break // Writer ステップ成功、ループを抜ける
    }
  }

  if writerErr != nil {
    // Writer ステップでエラーが発生して中断した場合、ジョブ全体のエラーとする
    jobErr = writerErr
  }

endJobSteps: // Context キャンセル時などにジャンプするラベル

  // defer で AfterJob が呼ばれる際に jobErr が設定される
  // ここでのログ出力は defer の後に実行されるため、JobExecution の最終状態が反映されている
  if jobExecution.Status == core.JobStatusFailed {
    logger.Errorf("Weather Job がエラーで終了しました (最終状態: %s): %v", jobExecution.Status, jobExecution.Failureliye)
  } else {
     logger.Infof("Weather Job を完了しました (最終状態: %s)。", jobExecution.Status)
     // ジョブが成功した場合、ExecutionContext に格納された最終情報をログ出力する例
     if totalRead, ok := jobExecution.ExecutionContext.GetInt("totalReadCount"); ok {
         logger.Infof("JobExecutionContext: Total Read Items: %d", totalRead)
     }
     if totalProcessed, ok := jobExecution.ExecutionContext.GetInt("totalProcessedCount"); ok {
         logger.Infof("JobExecutionContext: Total Processed Items: %d", totalProcessed)
     }
     if totalWritten, ok := jobExecution.ExecutionContext.GetInt("totalWriteCount"); ok {
         logger.Infof("JobExecutionContext: Total Written Items: %d", totalWritten)
     }
  }


  return jobErr // defer で設定された jobErr が返される
}

// JobName メソッドを追加 (SimpleJobLauncher でジョブ名を取得するために使用)
// core.Job インターフェースに JobName() string を追加した場合に実装
func (j *WeatherJob) JobName() string {
  return j.config.Batch.JobName
}
