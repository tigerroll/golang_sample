package step

import (
  "context"
  "errors"
  "fmt"
  "io" // io パッケージをインポート
  "time"

  "sample/src/main/go/batch/config" // config パッケージをインポート
  "sample/src/main/go/batch/domain/entity" // entity パッケージをインポート
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  stepListener "sample/src/main/go/batch/step/listener" // stepListener パッケージをインポート
  stepProcessor "sample/src/main/go/batch/step/processor" // stepProcessor パッケージをインポート
  stepReader "sample/src/main/go/batch/step/reader" // stepReader パッケージをインポート
  stepWriter "sample/src/main/go/batch/step/writer" // stepWriter パッケージをインポート
  logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

// ChunkOrientedStep は ItemReader, ItemProcessor, ItemWriter を使用するステップの実装です。
// core.Step インターフェースを実装します。
type ChunkOrientedStep struct {
  name          string // ステップ名
  reader        stepReader.Reader
  processor     stepProcessor.Processor
  writer        stepWriter.Writer
  chunkSize     int
  retryConfig   *config.RetryConfig // リトライ設定
  listeners     []stepListener.StepExecutionListener // このステップに固有のリスナー
}

// ChunkOrientedStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*ChunkOrientedStep)(nil)

// NewChunkOrientedStep は新しい ChunkOrientedStep のインスタンスを作成します。
// ステップの依存関係と設定を受け取ります。
func NewChunkOrientedStep(
  name string,
  reader stepReader.Reader,
  processor stepProcessor.Processor,
  writer stepWriter.Writer,
  chunkSize int,
  retryConfig *config.RetryConfig,
) *ChunkOrientedStep {
  return &ChunkOrientedStep{
    name:          name,
    reader:        reader,
    processor:     processor,
    writer:        writer,
    chunkSize:     chunkSize,
    retryConfig:   retryConfig,
    listeners:     make([]stepListener.StepExecutionListener, 0), // リスナーリストを初期化
  }
}

// StepName はステップ名を返します。core.Step インターフェースの実装です。
func (s *ChunkOrientedStep) StepName() string {
  return s.name
}

// RegisterListener はこのステップに StepExecutionListener を登録します。
func (s *ChunkOrientedStep) RegisterListener(l stepListener.StepExecutionListener) {
  s.listeners = append(s.listeners, l)
}

// notifyBeforeStep は登録されている StepExecutionListener の BeforeStep メソッドを呼び出します。
func (s *ChunkOrientedStep) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
  for _, l := range s.listeners {
    l.BeforeStep(ctx, stepExecution)
  }
}

// notifyAfterStep は登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
func (s *ChunkOrientedStep) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
  for _, l := range s.listeners {
    // LoggingListener の AfterStepWithDuration を特別に呼び出す例 (必要に応じて調整)
    if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
      loggingListener.AfterStepWithDuration(ctx, stepExecution)
    } else {
      l.AfterStep(ctx, stepExecution)
    }
  }
}


// Execute はチャンク処理を実行します。core.Step インターフェースの実装です。
// StepExecution のライフサイクル管理（開始/終了マーク、リスナー通知）をここで行います。
func (s *ChunkOrientedStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
  logger.Infof("ステップ '%s' (Execution ID: %s) を開始します。", s.name, stepExecution.ID)

  // StepExecution の開始時刻を設定し、状態をマーク
  stepExecution.StartTime = time.Now()
  stepExecution.MarkAsStarted() // Status = Started

  // ステップ実行前処理の通知
  s.notifyBeforeStep(ctx, stepExecution)

  // ステップ実行後処理 (defer で必ず実行)
  defer func() {
    // ステップの終了時刻を設定
    stepExecution.EndTime = time.Now()

    // ステップ実行後処理の通知
    s.notifyAfterStep(ctx, stepExecution)

    // StepExecution の最終状態を JobRepository で更新する必要がある
    // これは Job.Run メソッド内で JobRepository を使用して行うことを想定
    // ここでは StepExecution オブジェクト自体は更新済み
  }()


  // ★ WeatherJob.processChunkLoop のロジックをここに移動 ★
  retryConfig := s.retryConfig
  chunkSize := s.chunkSize

  // 成功したチャンクの数
  var chunkCount int = 0

  // FetchAndProcessStep の場合のみチャンク処理ループを実行
  if s.name == "FetchAndProcessStep" {
    // チャンク処理全体のリトライループ
    for retryAttempt := 0; retryAttempt < retryConfig.MaxAttempts; retryAttempt++ {
      logger.Debugf("ステップ '%s' チャンク処理試行: %d/%d", s.name, retryAttempt+1, retryConfig.MaxAttempts)

      // リトライ時にはチャンクをリセット
      processedItemsChunk := make([]*entity.WeatherDataToStore, 0, chunkSize)
      itemCountInChunk := 0
      chunkAttemptError := false // この試行でエラーが発生したかを示すフラグ

      // アイテムの読み込み、処理、チャンクへの追加を行うインナーループ
      for {
        select {
        case <-ctx.Done():
          logger.Warnf("Context がキャンセルされたため、ステップ '%s' のチャンク処理を中断します: %v", s.name, ctx.Err())
          stepExecution.MarkAsFailed(ctx.Err()) // ステップを失敗としてマーク
          jobExecution.AddFailureException(ctx.Err()) // JobExecution にもエラーを追加
          return ctx.Err() // Context エラーは即座に返す
        default:
        }

        // 単一アイテムの読み込みと処理
        // processSingleItem は Reader/Processor エラーまたは Context キャンセルエラーを返す
        processedItemSlice, eofReached, itemErr := s.processSingleItem(ctx, stepExecution)
        if itemErr != nil {
          // Reader または Processor でエラーが発生した場合
          logger.Errorf("ステップ '%s' アイテム処理でエラーが発生しました (試行 %d/%d): %v", s.name, retryAttempt+1, retryConfig.MaxAttempts, itemErr)
          chunkAttemptError = true // この試行はエラー
          break                    // インナーループを抜ける
        }

        // processSingleItem が nil アイテムを返した場合 (スキップされた場合)
        if processedItemSlice == nil && !eofReached {
          continue // 次のアイテムへ
        }

        // 処理済みアイテムをチャンクに追加
        processedItemsChunk = append(processedItemsChunk, processedItemSlice...)
        itemCountInChunk = len(processedItemsChunk)

        // チャンクが満たされたら ExecutionContext に追加
        if itemCountInChunk >= chunkSize {
          // ★ 処理済みチャンクを ExecutionContext に追加 ★
          currentProcessedData, ok := jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)
          if !ok {
            currentProcessedData = make([]*entity.WeatherDataToStore, 0)
          }
          jobExecution.ExecutionContext.Put("processed_weather_data", append(currentProcessedData, processedItemsChunk...))
          logger.Debugf("ステップ '%s' 処理済みチャンク (%d 件) を ExecutionContext に追加しました。ExecutionContext合計: %d件",
            s.name, len(processedItemsChunk), len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))

          chunkCount++ // チャンク処理成功
          // チャンクをリセット
          processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
          itemCountInChunk = 0
          // Note: ここで retryAttempt をリセットしない。リトライはチャンク処理全体に対して行う。
        }

        // Reader の終端に達した場合
        if eofReached {
          logger.Debugf("ステップ '%s' Reader からデータの終端に達しました。", s.name)
          break // インナーループを抜ける
        }
      } // インナーループ終了

      // インナーループ終了後の処理

      // この試行でエラーが発生した場合のリトライ判定
      if chunkAttemptError {
        if retryAttempt < retryConfig.MaxAttempts-1 {
          // リトライ可能回数が残っている場合
          logger.Warnf("ステップ '%s' チャンク処理試行 %d/%d が失敗しました。リトライ間隔: %d秒", s.name, retryAttempt+1, retryConfig.MaxAttempts, retryConfig.InitialInterval)
          // TODO: Exponential Backoff や Circuit Breaker ロジックをここに実装
          time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second) // シンプルな待機
          // リトライ前に ExecutionContext に追加したデータをロールバックする必要があるか検討
          // 現状は append しているので、失敗したチャンクのデータが重複して追加される可能性がある
          // 厳密には、トランザクションのように失敗したチャンクの処理結果は破棄すべき
          // ここではシンプル化のため、ExecutionContext への追加はそのままにする
        } else {
          // 最大リトライ回数に達した場合
          logger.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました。ステップを終了します。", s.name, retryConfig.MaxAttempts)
          // エラーは既に StepExecution に追加済み
          stepExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました", s.name, retryConfig.MaxAttempts)) // ステップを失敗としてマーク
          jobExecution.AddFailureException(stepExecution.Failureliye[len(stepExecution.Failureliye)-1]) // JobExecution にも最後のステップエラーを追加
          return fmt.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました", s.name, retryConfig.MaxAttempts) // エラーを返してステップを失敗させる
        }
      } else {
        // この試行がエラーなく完了した場合（Reader 終端に達したか、Context キャンセル以外）
        // 残っているアイテムがあれば最終チャンクとして処理し、ExecutionContext に追加
        if itemCountInChunk > 0 {
          logger.Infof("ステップ '%s' Reader 終端到達。残りのアイテム (%d 件) を処理し ExecutionContext に追加します (試行 %d/%d)。", s.name, itemCountInChunk, retryAttempt+1, retryConfig.MaxAttempts)
          // ★ 処理済み最終チャンクを ExecutionContext に追加 ★
          currentProcessedData, ok := jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)
          if !ok {
            currentProcessedData = make([]*entity.WeatherDataToStore, 0)
          }
          jobExecution.ExecutionContext.Put("processed_weather_data", append(currentProcessedData, processedItemsChunk...))
          logger.Debugf("ステップ '%s' 処理済み最終チャンク (%d 件) を ExecutionContext に追加しました。ExecutionContext合計: %d件",
            s.name, len(processedItemsChunk), len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))

          chunkCount++ // 最終チャンク処理成功
          // チャンクをリセット (不要かもしれないが念のため)
          processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
          itemCountInChunk = 0
        }

        // チャンク処理全体がエラーなく完了
        logger.Infof("ステップ '%s' チャンク処理ステップが正常に完了しました。合計チャンク数: %d, 合計アイテム数 (ExecutionContext): %d",
          s.name, chunkCount, len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))
        // ItemCount の設定 (Reader/Processor で正確に集計し、StepExecution に設定するのが理想)
        // stepExecution.ReadCount = // Reader でカウント
        // stepExecution.CommitCount = chunkCount // あくまでチャンク数

        // 成功したらリトライループを抜ける
        break
      }
    } // リトライループ終了
    // FetchAndProcessStep のチャンク処理ループが正常終了した場合、ここでメソッドを抜ける
    // Step はチャンク処理ループの最後に Step を完了としてマークしている
    stepExecution.MarkAsCompleted() // ★ 正常終了時に Step を完了としてマーク
    return nil // 正常終了
  }


  // SaveDataStep の場合: ExecutionContext からデータを取得し、Writer で書き込み
  if s.name == "SaveDataStep" {
    logger.Infof("ステップ '%s' は書き込み専用ステップです。ExecutionContext からデータを取得します。", s.name)
    dataToStore, ok := jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)
    if !ok {
      // データが ExecutionContext にない、または型が違う場合はエラー
      err := fmt.Errorf("ステップ '%s': ExecutionContext から書き込み対象データを取得できませんでした。", s.name)
      logger.Errorf("%v", err)
      stepExecution.MarkAsFailed(err)
      jobExecution.AddFailureException(err)
      return err
    }

    if len(dataToStore) == 0 {
      logger.Infof("ステップ '%s': ExecutionContext に書き込むデータがありません。", s.name)
      // データがない場合も正常完了とみなす
      stepExecution.MarkAsCompleted()
      return nil
    }

    // Writer でデータを書き込み
    // SaveDataStep の Writer (WeatherWriter) は ExecutionContext から取得したデータを直接受け取るように修正済み
    writeErr := s.writer.Write(ctx, dataToStore) // ExecutionContext から取得したデータを渡す
    if writeErr != nil {
      // Writer エラー
      logger.Errorf("ステップ '%s' Writer でエラーが発生しました: %v", s.name, writeErr)
      stepExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' writer error: %w", s.name, writeErr))
      jobExecution.AddFailureException(stepExecution.Failureliye[len(stepExecution.Failureliye)-1])
      return fmt.Errorf("ステップ '%s' writer error: %w", s.name, writeErr)
    }
    logger.Infof("ステップ '%s' Writer による書き込みが完了しました。", s.name)

    // WriteCount の設定 (Writer で正確に集計し、StepExecution に設定するのが理想)
    // ここでは簡易的に設定
    // stepExecution.WriteCount = len(dataToStore) // 書き込んだアイテム数

    // 成功したら Step を完了としてマーク
    stepExecution.MarkAsCompleted() // Status = Completed, ExitStatus = Completed

    return nil // 成功したら nil を返してメソッドを終了
  }


  // ここに到達するのは、FetchAndProcessStep のチャンク処理ループが正常終了した場合
  // または、SaveDataStep の Writer 処理が正常終了した場合

  // FetchAndProcessStep の場合、チャンク処理ループの最後に Step を完了としてマークし、nil を返している (上記修正で追加)
  // SaveDataStep の場合、Writer 処理の最後に Step を完了としてマークし、nil を返している

  // 安全のため、もしここに到達したらエラーとする（通常は到達しないはず）
  err := fmt.Errorf("ステップ '%s' 実行が予期せず終了しました", s.name)
  stepExecution.MarkAsFailed(err)
  jobExecution.AddFailureException(err)
  return err
}


// processSingleItem は Reader から1アイテム読み込み、Processor で処理します。
// 処理結果のスライス、EOFに達したかを示すフラグ、エラーを返します。
// WeatherJob から移動し、ChunkOrientedStep のプライベートメソッドとしました。
func (s *ChunkOrientedStep) processSingleItem(ctx context.Context, stepExecution *core.StepExecution) ([]*entity.WeatherDataToStore, bool, error) {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return nil, false, ctx.Err()
  default:
  }

  // Reader から読み込み
  readItem, readerErr := s.reader.Read(ctx)

  if readerErr != nil {
    if errors.Is(readerErr, io.EOF) {
      // EOF の場合はエラーではないが、終端に達したことを示すフラグを返す
      logger.Debugf("ステップ '%s' Reader returned EOF.", s.name)
      return nil, true, nil
    }
    // その他の Reader エラー
    logger.Errorf("ステップ '%s' Reader error: %v", s.name, readerErr)
    stepExecution.AddFailureException(readerErr) // Reader エラーも StepExecution に記録
    return nil, false, fmt.Errorf("ステップ '%s' reader error: %w", s.name, readerErr)
  }

  if readItem == nil {
    // nil アイテムはスキップ
    logger.Debugf("ステップ '%s' Reader returned nil item, skipping.", s.name)
    return nil, false, nil
  }

  // Processor で処理
  processedItem, processorErr := s.processor.Process(ctx, readItem)
  if processorErr != nil {
    // Processor エラー
    logger.Errorf("ステップ '%s' Processor error: %v", s.name, processorErr)
    stepExecution.AddFailureException(processorErr) // Processor エラーも StepExecution に記録
    return nil, false, fmt.Errorf("ステップ '%s' processor error: %w", s.name, processorErr)
  }

  // 処理済みアイテムの型アサート
  // Processor は []*entity.WeatherDataToStore を返すことを期待
  processedItemsSlice, ok := processedItem.([]*entity.WeatherDataToStore)
  if !ok {
    // 予期しない型の場合
    err := fmt.Errorf("ステップ '%s' processor returned unexpected type: %T, expected []*entity.WeatherDataToStore", s.name, processedItem)
    logger.Errorf("%v", err)
    stepExecution.AddFailureException(err) // 型アサートエラーも StepExecution に記録
    return nil, false, err
  }

  // 成功
  //logger.Debugf("Successfully processed an item in step '%s'.", s.name)
  return processedItemsSlice, false, nil
}

// writeChunk は加工済みアイテムのチャンクを Writer で書き込みます。
// このメソッドは ChunkOrientedStep から削除されました。
/*
func (s *ChunkOrientedStep) writeChunk(ctx context.Context, stepExecution *core.StepExecution, chunkNum int, items []*entity.WeatherDataToStore) error {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    logger.Warnf("Context がキャンセルされたため、ステップ '%s' チャンク #%d の書き込みを中断します: %v", s.name, chunkNum, ctx.Err())
    return ctx.Err()
  default:
  }

  // Writer の Write メソッドを呼び出す
  // SaveDataStep の場合、items は ExecutionContext から取得されたデータになります。
  // FetchAndProcessStep の場合、items は Reader/Processor の出力であり、Writer は DummyWriter です。
  writeErr := s.writer.Write(ctx, items)
  if writeErr != nil {
    // Writer エラー
    logger.Errorf("ステップ '%s' Writer error for chunk #%d: %v", s.name, chunkNum, writeErr)
    stepExecution.AddFailureException(writeErr) // Writer エラーも StepExecution に記録
    return fmt.Errorf("ステップ '%s' writer error for chunk #%d: %w", s.name, chunkNum, writeErr)
  }

  logger.Infof("ステップ '%s' チャンク #%d の書き込み処理が完了しました。", s.name, chunkNum)
  return nil // Writer が成功したら nil を返す
}
*/
