package job

import (
  "context"
  "errors"
  "fmt"
  "io"
  "time"

  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core"
  jobListener "sample/src/main/go/batch/job/listener"
  repository "sample/src/main/go/batch/repository"
  stepListener "sample/src/main/go/batch/step/listener"
  stepProcessor "sample/src/main/go/batch/step/processor"
  stepReader "sample/src/main/go/batch/step/reader"
  stepWriter "sample/src/main/go/batch/step/writer"
  logger "sample/src/main/go/batch/util/logger"
  entity "sample/src/main/go/batch/domain/entity" // entity パッケージをインポート
)

type WeatherJob struct {
  repo repository.WeatherRepository
  reader stepReader.Reader
  processor stepProcessor.Processor
  writer stepWriter.Writer
  config *config.Config
  stepListeners map[string][]stepListener.StepExecutionListener
  jobListeners []jobListener.JobExecutionListener
}

var _ core.Job = (*WeatherJob)(nil)

func NewWeatherJob(
  repo repository.WeatherRepository,
  reader stepReader.Reader,
  processor stepProcessor.Processor,
  writer stepWriter.Writer,
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo: repo,
    reader: reader,
    processor: processor,
    writer: writer,
    config: cfg,
    stepListeners: make(map[string][]stepListener.StepExecutionListener),
    jobListeners: make([]jobListener.JobExecutionListener, 0),
  }
}

func (j *WeatherJob) RegisterStepListener(stepName string, l stepListener.StepExecutionListener) {
  if _, ok := j.stepListeners[stepName]; !ok {
    j.stepListeners[stepName] = make([]stepListener.StepExecutionListener, 0)
  }
  j.stepListeners[stepName] = append(j.stepListeners[stepName], l)
}

func (j *WeatherJob) RegisterJobListener(l jobListener.JobExecutionListener) {
  j.jobListeners = append(j.jobListeners, l)
}

func (j *WeatherJob) notifyBeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.BeforeJob(ctx, jobExecution)
  }
}

func (j *WeatherJob) notifyAfterJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.AfterJob(ctx, jobExecution)
  }
}

func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
  if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
    for _, l := range stepListeners {
      l.BeforeStep(ctx, stepExecution)
    }
  }
}

func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
  if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
    for _, l := range stepListeners {
      if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepExecution)
      } else {
        l.AfterStep(ctx, stepExecution)
      }
    }
  }
}


// Run メソッド (core.Job インターフェースの実装)
func (j *WeatherJob) Run(ctx context.Context, jobExecution *core.JobExecution) error {
  retryConfig := j.config.Batch.Retry
  chunkSize := j.config.Batch.ChunkSize

  logger.Infof("Weather Job を開始します。チャンクサイズ: %d", chunkSize)

  j.notifyBeforeJob(ctx, jobExecution)

  defer func() {
    if jobExecution.Status != core.JobStatusFailed {
        jobExecution.MarkAsCompleted()
    }

    j.notifyAfterJob(ctx, jobExecution)

    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
        jobExecution.AddFailureException(fmt.Errorf("リポジトリのクローズエラー: %w", err))
        if jobExecution.Status != core.JobStatusFailed {
            jobExecution.MarkAsFailed(fmt.Errorf("リポジトリのクローズエラー: %w", err))
        }
      }
    }
  }()

  stepExecution := core.NewStepExecution("WeatherProcessingStep", jobExecution)
  jobExecution.StepExecutions = append(jobExecution.StepExecutions, stepExecution)

  stepExecution.StartTime = time.Now()
  stepExecution.MarkAsStarted()
  j.notifyBeforeStep(ctx, stepExecution)

  defer func() {
      stepExecution.EndTime = time.Now()

      j.notifyAfterStep(ctx, stepExecution)

      if stepExecution.Status == core.JobStatusFailed {
          jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' が失敗しました", stepExecution.StepName))
      } else if jobExecution.Status != core.JobStatusFailed {
          stepExecution.MarkAsCompleted()
      }
  }()


  // チャンク処理ループ
  // processedItemsChunk := make([]interface{}, 0, chunkSize) // 修正前
  processedItemsChunk := make([]*entity.WeatherDataToStore, 0, chunkSize) // ★ 修正：型を []*entity.WeatherDataToStore に変更
  itemCountInChunk := 0
  chunkCount := 0
  retryAttempt := 0

  for retryAttempt = 0; retryAttempt < retryConfig.MaxAttempts; retryAttempt++ {
      logger.Debugf("チャンク処理試行: %d", retryAttempt+1)

      chunkProcessErr := func() error {
          processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize) // ★ 修正：型を []*entity.WeatherDataToStore に変更
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

              readItem, readerErr := j.reader.Read(ctx)

              if readerErr != nil {
                  if errors.Is(readerErr, io.EOF) {
                      logger.Debugf("Reader からデータの終端に達しました。")
                      break
                  }
                  logger.Errorf("Reader でエラーが発生しました (試行 %d): %v", retryAttempt+1, readerErr)
                  stepExecution.AddFailureException(readerErr)
                  return fmt.Errorf("reader error: %w", readerErr)
              }

              if readItem == nil {
                  continue
              }

              processedItem, processorErr := j.processor.Process(ctx, readItem)
              if processorErr != nil {
                  logger.Errorf("Processor でエラーが発生しました (試行 %d): %v", retryAttempt+1, processorErr)
                  stepExecution.AddFailureException(processorErr)
                  return fmt.Errorf("processor error: %w", processorErr)
              }

              // 処理済みアイテムをチャンクに追加
              // Processorからの戻り値を []*entity.WeatherDataToStore にアサート ★ 追加
              processedItemsSlice, ok := processedItem.([]*entity.WeatherDataToStore)
              if !ok {
                   // 予期しない型の場合はエラーとする
                   err := fmt.Errorf("processor returned unexpected type: %T, expected []*entity.WeatherDataToStore", processedItem)
                   logger.Errorf("%v", err)
                   stepExecution.AddFailureException(err) // ステップ実行にエラーを追加
                   return err // チャンク処理エラーとして返す
              }

              // アサートしたスライスの要素をチャンクに追加 ★ 修正
              processedItemsChunk = append(processedItemsChunk, processedItemsSlice...)
              itemCountInChunk = len(processedItemsChunk) // チャンクの現在のアイテム数を更新

              if itemCountInChunk >= chunkSize {
                  chunkCount++
                  logger.Infof("チャンクサイズ (%d) に達しました。Writer で書き込みを開始します (チャンク #%d, 試行 %d)。", chunkSize, chunkCount, retryAttempt+1)

                  writerErr := j.writer.Write(ctx, processedItemsChunk) // []*entity.WeatherDataToStore を渡す
                  if writerErr != nil {
                      logger.Errorf("Writer でエラーが発生しました (チャンク #%d, 試行 %d): %v", chunkCount, retryAttempt+1, writerErr)
                      stepExecution.AddFailureException(writerErr)
                      return fmt.Errorf("writer error: %w", writerErr)
                  }

                  logger.Infof("チャンク #%d の書き込みが完了しました。", chunkCount)

                  processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize) // ★ 修正：型を []*entity.WeatherDataToStore に変更
                  itemCountInChunk = 0

                  retryAttempt = 0 // 成功したチャンクの後に失敗しても、次のチャンクは最初からリトライする
              }
          }

          if itemCountInChunk > 0 {
              chunkCount++
              logger.Infof("Reader 終端到達。残りのアイテム (%d 件) を書き込みます (チャンク #%d, 試行 %d)。", itemCountInChunk, chunkCount, retryAttempt+1)
              writerErr := j.writer.Write(ctx, processedItemsChunk) // []*entity.WeatherDataToStore を渡す
              if writerErr != nil {
                  logger.Errorf("Writer でエラーが発生しました (最終チャンク #%d, 試行 %d): %v", chunkCount, retryAttempt+1, writerErr)
                  stepExecution.AddFailureException(writerErr)
                  return fmt.Errorf("writer error on final chunk: %w", writerErr)
              }
              logger.Infof("最終チャンク #%d の書き込みが完了しました。", chunkCount)
              processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize) // ★ 修正：型を []*entity.WeatherDataToStore に変更
              itemCountInChunk = 0
              retryAttempt = 0
          }

          return nil
      }()

      if chunkProcessErr == nil {
          logger.Infof("チャンク処理ステップが正常に完了しました。")
          // ステップ実行の最終状態は defer で設定されるためここでは不要
          // stepExecution.MarkAsCompleted()
          stepExecution.ExecutionContext.Put("chunkCount", chunkCount)

          // ItemCount の設定 (例)
          // ReadCount は Reader で取得したアイテムの合計数
          // WriteCount は Writer で書き込んだアイテムの合計数
          // CommitCount は Writer が成功した回数 (== chunkCount)
          // これは Reader/Processor/Writer または Job のロジックで正確に集計し、StepExecutionに設定する必要があります。
          // 簡単のため、ここでは ReadCount と WriteCount はダミー値を設定します。
          // 実際の ReadCount は WeatherReader に totalReadCount などのフィールドを追加して集計する必要があります。
          // 実際の WriteCount は WeatherWriter に totalWriteCount などのフィールドを追加して集計する必要があります。
          // StepExecution の ReadCount/WriteCount/CommitCount フィールド自体は core.go に既に存在します。
          stepExecution.ReadCount = chunkCount * chunkSize // あくまで概算
          stepExecution.WriteCount = chunkCount * chunkSize // あくまで概算
          stepExecution.CommitCount = chunkCount

          break
      } else {
          if errors.Is(chunkProcessErr, context.Canceled) || errors.Is(chunkProcessErr, context.DeadlineExceeded) {
               return chunkProcessErr
          }

          if retryAttempt < retryConfig.MaxAttempts-1 {
              logger.Warnf("チャンク処理でエラーが発生しました。リトライします (試行 %d/%d)。エラー: %v", retryAttempt+1, retryConfig.MaxAttempts, chunkProcessErr)
              time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
          } else {
              logger.Errorf("チャンク処理が最大リトライ回数 (%d) 失敗しました。ジョブを終了します。エラー: %v", retryConfig.MaxAttempts, chunkProcessErr)
              stepExecution.MarkAsFailed(chunkProcessErr) // ステップ実行を失敗としてマーク
              jobExecution.AddFailureException(fmt.Errorf("ステップ '%s' が最大リトライ回数に達し失敗しました", stepExecution.StepName)) // ジョブ実行にエラーを追加
              // JobExecution の状態は defer の Job 側で最終決定される
              return chunkProcessErr
          }
      }
  }

  // チャンク処理全体がリトライ上限に達して失敗した場合、ここでエラーが返される。
  // 成功した場合は nil が返される。
  // stepExecution の最終状態は defer で設定済み。
  // JobExecution の最終状態も defer で設定済み。
  return nil // Run メソッドはジョブ全体の実行結果を返す。エラーは JobExecution に記録される。
}

func (j *WeatherJob) JobName() string {
  return j.config.Batch.JobName
}
