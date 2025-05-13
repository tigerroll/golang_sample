package job

import (
  "context"
  "errors"
  "fmt"
  "io"
  "time"

  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  jobListener "sample/src/main/go/batch/job/listener" // jobListener パッケージをインポート
  repository "sample/src/main/go/batch/repository"
  stepListener "sample/src/main/go/batch/step/listener"
  stepProcessor "sample/src/main/go/batch/step/processor"
  stepReader "sample/src/main/go/batch/step/reader"
  stepWriter "sample/src/main/go/batch/step/writer"
  logger "sample/src/main/go/batch/util/logger"
  entity "sample/src/main/go/batch/domain/entity" // entity パッケージをインポート
)

// WeatherJob は天気予報データを取得・処理・保存するバッチジョブです。
// core.Job インターフェースを実装します。
type WeatherJob struct {
  repo          repository.WeatherRepository
  reader        stepReader.Reader
  processor     stepProcessor.Processor
  writer        stepWriter.Writer
  config        *config.Config
  stepListeners map[string][]stepListener.StepExecutionListener
  jobListeners  []jobListener.JobExecutionListener // jobListener パッケージの JobExecutionListener を使用
}

// WeatherJob が core.Job インターフェースを満たすことを確認します。
var _ core.Job = (*WeatherJob)(nil)

// NewWeatherJob は新しい WeatherJob のインスタンスを作成します。
func NewWeatherJob(
  repo repository.WeatherRepository,
  reader stepReader.Reader,
  processor stepProcessor.Processor,
  writer stepWriter.Writer,
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo:          repo,
    reader:        reader,
    processor:     processor,
    writer:        writer,
    config:        cfg,
    stepListeners: make(map[string][]stepListener.StepExecutionListener),
    jobListeners:  make([]jobListener.JobExecutionListener, 0), // jobListener パッケージの JobExecutionListener を使用
  }
}

// RegisterStepListener は指定されたステップ名に StepExecutionListener を登録します。
func (j *WeatherJob) RegisterStepListener(stepName string, l stepListener.StepExecutionListener) {
  if _, ok := j.stepListeners[stepName]; !ok {
    j.stepListeners[stepName] = make([]stepListener.StepExecutionListener, 0)
  }
  j.stepListeners[stepName] = append(j.stepListeners[stepName], l)
}

// RegisterJobListener は JobExecutionListener を登録します。
func (j *WeatherJob) RegisterJobListener(l jobListener.JobExecutionListener) { // jobListener パッケージの JobExecutionListener を使用
  j.jobListeners = append(j.jobListeners, l)
}

// notifyBeforeJob は登録されている JobExecutionListener の BeforeJob メソッドを呼び出します。
func (j *WeatherJob) notifyBeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.BeforeJob(ctx, jobExecution)
  }
}

// notifyAfterJob は登録されている JobExecutionListener の AfterJob メソッドを呼び出します。
func (j *WeatherJob) notifyAfterJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.AfterJob(ctx, jobExecution)
  }
}

// notifyBeforeStep は指定されたステップ名に登録されている StepExecutionListener の BeforeStep メソッドを呼び出します。
func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
  if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
    for _, l := range stepListeners {
      l.BeforeStep(ctx, stepExecution)
    }
  }
}

// notifyAfterStep は指定されたステップ名に登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
  if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
    for _, l := range stepListeners {
      // LoggingListener の AfterStepWithDuration を特別に呼び出す例 (必要に応じて調整)
      if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepExecution)
      } else {
        l.AfterStep(ctx, stepExecution)
      }
    }
  }
}

// Run メソッドは core.Job インターフェースの実装です。
// ジョブ全体の実行フローを制御します。
func (j *WeatherJob) Run(ctx context.Context, jobExecution *core.JobExecution) error {
  retryConfig := j.config.Batch.Retry
  chunkSize := j.config.Batch.ChunkSize

  logger.Infof("Weather Job を開始します。チャンクサイズ: %d", chunkSize)

  // Job 実行前処理の通知
  j.notifyBeforeJob(ctx, jobExecution)

  // Job 実行後処理 (defer で必ず実行)
  defer func() {
    // ジョブがまだ失敗としてマークされていなければ完了としてマーク
    if jobExecution.Status != core.JobStatusFailed {
      jobExecution.MarkAsCompleted()
    }

    // Job 実行後処理の通知
    j.notifyAfterJob(ctx, jobExecution)

    // リポジトリのリソース解放 (Close メソッドを持つ場合)
    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
        jobExecution.AddFailureException(fmt.Errorf("リポジトリのクローズエラー: %w", err))
        // クローズエラーが発生した場合、ジョブを失敗としてマーク
        if jobExecution.Status != core.JobStatusFailed {
          jobExecution.MarkAsFailed(fmt.Errorf("リポジトリのクローズエラー: %w", err))
        }
      }
    }
  }()

  // ★ 第1段階: ステップ実行のセットアップ処理を新しいメソッドに切り出し
  stepExecution, err := j.setupStepExecution(ctx, jobExecution, "WeatherProcessingStep")
  if err != nil {
    // ステップセットアップに失敗した場合、ジョブを失敗としてマークし、エラーを返す
    jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' のセットアップに失敗しました: %w", "WeatherProcessingStep", err))
    return err
  }

  // ステップ実行後処理 (defer で必ず実行)
  defer func() {
    stepExecution.EndTime = time.Now() // ステップの終了時刻を設定
    j.notifyAfterStep(ctx, stepExecution) // ステップ実行後処理の通知

    // ステップが失敗としてマークされていれば、ジョブも失敗としてマーク
    if stepExecution.Status == core.JobStatusFailed {
      jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' が失敗しました", stepExecution.StepName))
    } else if jobExecution.Status != core.JobStatusFailed {
      // ジョブがまだ失敗でなく、ステップが失敗でなければ、ステップを完了としてマーク
      stepExecution.MarkAsCompleted()
    }
  }()


  // ★ チャンク処理ループ (この部分はまだ長いままですが、次の段階で分割します)
  processedItemsChunk := make([]*entity.WeatherDataToStore, 0, chunkSize)
  itemCountInChunk := 0
  chunkCount := 0
  retryAttempt := 0

  for retryAttempt = 0; retryAttempt < retryConfig.MaxAttempts; retryAttempt++ {
    logger.Debugf("チャンク処理試行: %d", retryAttempt+1)

    chunkProcessErr := func() error {
      processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
      itemCountInChunk = 0

      for {
        select {
        case <-ctx.Done():
          logger.Warnf("Context がキャンセルされたため、チャンク処理を中断します: %v", ctx.Err())
          stepExecution.MarkAsFailed(ctx.Err()) // ステップ実行にエラーを追加
          jobExecution.AddFailureException(ctx.Err()) // ジョブ実行にエラーを追加
          return ctx.Err()
        default:
        }

        // ★ 第2段階: 単一アイテムの読み込みと処理を新しいメソッドに切り出し
        processedItemSlice, eofReached, itemErr := j.processSingleItem(ctx, stepExecution)
        if itemErr != nil {
          // Reader または Processor でエラーが発生した場合
          logger.Errorf("アイテム処理でエラーが発生しました (試行 %d/%d): %v", retryAttempt+1, retryConfig.MaxAttempts, itemErr)
          stepExecution.AddFailureException(itemErr) // ステップ実行にエラーを追加
          return fmt.Errorf("item processing failed: %w", itemErr) // エラーを返してこのチャンク試行を終了
        }

        if eofReached {
          logger.Debugf("Reader からデータの終端に達しました。")
          break // インナーループを抜ける
        }

        // 処理済みアイテムをチャンクに追加
        processedItemsChunk = append(processedItemsChunk, processedItemSlice...)
        itemCountInChunk = len(processedItemsChunk)

        // チャンクが満たされたら書き込み
        if itemCountInChunk >= chunkSize {
          // ★ 第3段階: チャンク書き込み処理を新しいメソッドに切り出し (まだ実装はここ)
          chunkCount++
          logger.Infof("チャンクサイズ (%d) に達しました。Writer で書き込みを開始します (チャンク #%d, 試行 %d)。", chunkSize, chunkCount, retryAttempt+1)

          writerErr := j.writer.Write(ctx, processedItemsChunk)
          if writerErr != nil {
            logger.Errorf("Writer でエラーが発生しました (チャンク #%d, 試行 %d): %v", chunkCount, retryAttempt+1, writerErr)
            stepExecution.AddFailureException(writerErr)
            return fmt.Errorf("writer error: %w", writerErr)
          }

          logger.Infof("チャンク #%d の書き込みが完了しました。", chunkCount)

          processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
          itemCountInChunk = 0

          // リトライ回数をリセットしない（チャンク処理全体でリトライをカウントするため）
          // retryAttempt = 0
        }
      } // インナーループ終了

      // インナーループ終了後の処理

      // 残っているアイテムがあれば最終チャンクとして書き込む
      if itemCountInChunk > 0 {
        chunkCount++
        logger.Infof("Reader 終端到達。残りのアイテム (%d 件) を書き込みます (チャンク #%d, 試行 %d)。", itemCountInChunk, chunkCount, retryAttempt+1)
        // ★ 第3段階: チャンク書き込み処理を新しいメソッドに切り出し (まだ実装はここ)
        writerErr := j.writer.Write(ctx, processedItemsChunk)
        if writerErr != nil {
          logger.Errorf("Writer でエラーが発生しました (最終チャンク #%d, 試行 %d): %v", chunkCount, retryAttempt+1, writerErr)
          stepExecution.AddFailureException(writerErr)
          return fmt.Errorf("writer error on final chunk: %w", writerErr)
        }
        logger.Infof("最終チャンク #%d の書き込みが完了しました。", chunkCount)
        processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
        itemCountInChunk = 0
        // リトライ回数をリセットしない
        // retryAttempt = 0
      }

      return nil // このチャンク試行はエラーなく完了
    }() // chunkProcessErr := func() error {...}() 終了

    if chunkProcessErr == nil {
      logger.Infof("チャンク処理ステップが正常に完了しました。")
      // ステップ実行の最終状態は defer で設定されるためここでは不要
      // stepExecution.MarkAsCompleted()
      stepExecution.ExecutionContext.Put("chunkCount", chunkCount)

      // ItemCount の設定 (簡易的に計算)
      stepExecution.ReadCount = chunkCount * chunkSize // あくまで概算
      stepExecution.WriteCount = chunkCount * chunkSize // あくまで概算
      stepExecution.CommitCount = chunkCount

      break // チャンク処理全体が成功したらリトライループを抜ける
    } else {
      if errors.Is(chunkProcessErr, context.Canceled) || errors.Is(chunkProcessErr, context.DeadlineExceeded) {
         return chunkProcessErr // Context エラーは即座に返す
      }

      // チャンク処理試行でエラーが発生した場合のリトライ判定
      if retryAttempt < retryConfig.MaxAttempts-1 {
        logger.Warnf("チャンク処理でエラーが発生しました。リトライします (試行 %d/%d)。エラー: %v", retryAttempt+1, retryConfig.MaxAttempts, chunkProcessErr)
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second) // シンプルな待機
        // TODO: Exponential Backoff や Circuit Breaker ロジックをここに実装
      } else {
        logger.Errorf("チャンク処理が最大リトライ回数 (%d) 失敗しました。ステップを終了します。エラー: %v", retryConfig.MaxAttempts, chunkProcessErr)
        stepExecution.MarkAsFailed(chunkProcessErr) // ステップ実行を失敗としてマーク
        jobExecution.AddFailureException(fmt.Errorf("ステップ '%s' が最大リトライ回数に達し失敗しました", stepExecution.StepName)) // ジョブ実行にエラーを追加
        // JobExecution の状態は defer の Job 側で最終決定される
        return chunkProcessErr // エラーを返してジョブを失敗させる
      }
    }
  } // リトライループ終了

  // ここに到達するのは、リトライ回数が0の場合か、論理的に到達しない場合
  // 安全のためエラーを返しておく (通常は processChunkLoop 内で return される)
  if len(stepExecution.Failureliye) > 0 {
    // ステップにエラーが記録されている場合は、そのエラーを返す
    return stepExecution.Failureliye[0] // 最初の失敗例外を返す (または全てをラップ)
  }

  return nil // Run メソッドはジョブ全体の実行結果を返す。エラーは JobExecution に記録される。
}

// JobName はジョブ名を返します。core.Job インターフェースの実装です。
func (j *WeatherJob) JobName() string {
  return j.config.Batch.JobName
}

// ★ 第1段階で追加されたメソッド
// setupStepExecution は新しい StepExecution を作成し、初期化します。
func (j *WeatherJob) setupStepExecution(ctx context.Context, jobExecution *core.JobExecution, stepName string) (*core.StepExecution, error) {
  // StepExecution の作成
  stepExecution := core.NewStepExecution(stepName, jobExecution)
  // NewStepExecution 内で jobExecution.StepExecutions に追加済み

  // ステップ開始時刻の設定と状態のマーク
  stepExecution.StartTime = time.Now()
  stepExecution.MarkAsStarted()

  // ステップ実行前処理の通知
  j.notifyBeforeStep(ctx, stepExecution)

  logger.Debugf("ステップ '%s' (Execution ID: %s) のセットアップが完了しました。", stepName, stepExecution.ID)

  // セットアップ処理自体でエラーが発生する可能性は低いですが、将来的な拡張に備えてエラーを返すようにしておきます。
  return stepExecution, nil
}

// ★ 第2段階で追加されたメソッド
// processSingleItem は Reader から1アイテム読み込み、Processor で処理します。
// 処理結果のスライス、EOFに達したかを示すフラグ、エラーを返します。
func (j *WeatherJob) processSingleItem(ctx context.Context, stepExecution *core.StepExecution) ([]*entity.WeatherDataToStore, bool, error) {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return nil, false, ctx.Err()
  default:
  }

  // Reader から読み込み
  readItem, readerErr := j.reader.Read(ctx)

  if readerErr != nil {
    if errors.Is(readerErr, io.EOF) {
      // EOF の場合はエラーではないが、終端に達したことを示すフラグを返す
      logger.Debugf("Reader returned EOF.")
      return nil, true, nil
    }
    // その他の Reader エラー
    logger.Errorf("Reader error: %v", readerErr)
    return nil, false, fmt.Errorf("reader error: %w", readerErr)
  }

  if readItem == nil {
    // nil アイテムはスキップ
    logger.Debugf("Reader returned nil item, skipping.")
    return nil, false, nil
  }

  // Processor で処理
  processedItem, processorErr := j.processor.Process(ctx, readItem)
  if processorErr != nil {
    // Processor エラー
    logger.Errorf("Processor error: %v", processorErr)
    return nil, false, fmt.Errorf("processor error: %w", processorErr)
  }

  // 処理済みアイテムの型アサート
  processedItemsSlice, ok := processedItem.([]*entity.WeatherDataToStore)
  if !ok {
    // 予期しない型の場合
    err := fmt.Errorf("processor returned unexpected type: %T, expected []*entity.WeatherDataToStore", processedItem)
    logger.Errorf("%v", err)
    return nil, false, err
  }

  // 成功
  //logger.Debugf("Successfully processed an item.")
  return processedItemsSlice, false, nil
}


// ★ 今後の段階で追加されるメソッドのプレースホルダ (コメントアウト)
/*
// writeChunk は加工済みアイテムのチャンクを Writer で書き込みます。
func (j *WeatherJob) writeChunk(ctx context.Context, stepExecution *core.StepExecution, chunkNum int, items []*entity.WeatherDataToStore) error {
  // TODO: Implement chunk writing logic here
  return fmt.Errorf("writeChunk not implemented")
}

// processChunkLoop はチャンク処理のメインループとリトライロジックを管理します。
func (j *WeatherJob) processChunkLoop(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
  // TODO: Implement chunk processing loop and retry logic here
  return fmt.Errorf("processChunkLoop not implemented")
}
*/
