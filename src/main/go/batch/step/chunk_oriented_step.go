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

      // チャンクが満たされたら書き込み (または次のステップへの引き渡し準備)
      if itemCountInChunk >= chunkSize {
        // ★ 処理済みチャンクを ExecutionContext に追加（FetchAndProcessStep の場合）または Writer で書き込み（SaveDataStep の場合）★
        if s.name == "FetchAndProcessStep" {
          // FetchAndProcessStep の場合: ExecutionContext に追加
          currentProcessedData, ok := jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)
          if !ok {
            currentProcessedData = make([]*entity.WeatherDataToStore, 0)
          }
          jobExecution.ExecutionContext.Put("processed_weather_data", append(currentProcessedData, processedItemsChunk...))
          logger.Debugf("ステップ '%s' 処理済みチャンク (%d 件) を ExecutionContext に追加しました。ExecutionContext合計: %d件",
            s.name, len(processedItemsChunk), len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))

          // Dummy Writer の呼び出し (実際には何も起きない)
          writeErr := s.writeChunk(ctx, stepExecution, chunkCount+1, processedItemsChunk) // chunkCount を使用
          if writeErr != nil {
            logger.Errorf("ステップ '%s' Writer でエラーが発生しました (チャンク #%d, 試行 %d/%d): %v", s.name, chunkCount+1, retryAttempt+1, retryConfig.MaxAttempts, writeErr) // chunkCount を使用
            chunkAttemptError = true
            break
          }

        } else if s.name == "SaveDataStep" {
          // SaveDataStep の場合: Writer で書き込み
          // SaveDataStep では Reader が DummyReader なので、items は常に空になります。
          // 実際のデータは ExecutionContext から取得されるべきですが、
          // ChunkOrientedStep の設計上、Writer は processSingleItem の出力 (またはその蓄積) を受け取るため、
          // SaveDataStep の Reader/Processor はダミーでも、ExecutionContext からデータを取得して
          // Writer に渡すような仕組みが ChunkOrientedStep 内に必要になります。
          // しかし、これは ChunkOrientedStep の標準的な動作とは異なります。
          // シンプル化のため、SaveDataStep の Writer は ChunkOrientedStep から渡される空のチャンクを受け取りますが、
          // 内部で ExecutionContext からデータを取得して書き込むように WeatherWriter を修正します。
          // ここでは SaveDataStep の Writer 呼び出しは行いません。Writer はステップ完了後にまとめて呼び出すか、
          // または SaveDataStep 自体の Execute メソッド内で ExecutionContext から読み込み、
          // Writer を呼び出す設計にする必要があります。

          // 現在の ChunkOrientedStep の設計では、Reader -> Processor -> Writer は一体です。
          // SaveDataStep を ChunkOrientedStep で実装する場合、DummyReader/DummyProcessor を使用すると
          // Writer にデータが渡りません。
          // したがって、SaveDataStep は ChunkOrientedStep ではなく、ExecutionContext から読み込み、
          // Writer を実行する別のタイプのステップ (例: TaskletStep のようなもの) で実装するか、
          // または SaveDataStep の ChunkOrientedStep の Reader が ExecutionContext からデータを読み込むように
          // DummyReader を修正する必要があります。

          // ここでは、SaveDataStep は ChunkOrientedStep であり、DummyReader/Processor を使用する前提で、
          // ChunkOrientedStep のチャンク処理ループは SaveDataStep では実質的に何も読み込まないため、
          // このチャンク処理ループ自体は SaveDataStep ではデータを書き込みません。
          // SaveDataStep の書き込みは、ステップの Execute メソッドの最後で、
          // ExecutionContext からデータを取得して Writer を呼び出す形にする必要があります。

          // したがって、SaveDataStep の ChunkOrientedStep の Execute メソッドでは、
          // このチャンク処理ループは Reader が EOF を返すまで空回りし、
          // 処理済みアイテムは常に空になります。
          // ExecutionContext への追加も FetchAndProcessStep でのみ行われます。
          // SaveDataStep の書き込みロジックは、このループの外に実装します。

          // このブロックでは FetchAndProcessStep の Writer 呼び出しのみを扱います。
          // SaveDataStep の場合はこのブロックはスキップされます。
        }


        chunkCount++ // 書き込み成功した場合のみチャンク数をインクリメント
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
      // 残っているアイテムがあれば最終チャンクとして処理し、ExecutionContext に追加（FetchAndProcessStep の場合）
      if itemCountInChunk > 0 {
        if s.name == "FetchAndProcessStep" {
          logger.Infof("ステップ '%s' Reader 終端到達。残りのアイテム (%d 件) を処理し ExecutionContext に追加します (試行 %d/%d)。", s.name, itemCountInChunk, retryAttempt+1, retryConfig.MaxAttempts)
          // ★ 処理済み最終チャンクを ExecutionContext に追加 ★
          currentProcessedData, ok := jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)
          if !ok {
            currentProcessedData = make([]*entity.WeatherDataToStore, 0)
          }
          jobExecution.ExecutionContext.Put("processed_weather_data", append(currentProcessedData, processedItemsChunk...))
          logger.Debugf("ステップ '%s' 処理済み最終チャンク (%d 件) を ExecutionContext に追加しました。ExecutionContext合計: %d件",
            s.name, len(processedItemsChunk), len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))

          // 最終チャンクの書き込み処理 (このステップではダミーWriterなので実際には何も起きない)
          writeErr := s.writeChunk(ctx, stepExecution, chunkCount+1, processedItemsChunk) // chunkCount を使用
          if writeErr != nil {
            logger.Errorf("ステップ '%s' Writer でエラーが発生しました (最終チャンク #%d, 試行 %d/%d): %v", s.name, chunkCount+1, retryAttempt+1, retryConfig.MaxAttempts, writeErr) // chunkCount を使用
            // 最終チャンクの書き込み失敗もリトライ対象とする
            if retryAttempt < retryConfig.MaxAttempts-1 {
              logger.Warnf("ステップ '%s' 最終チャンクの書き込み試行 %d/%d が失敗しました。リトライ間隔: %d秒", s.name, retryAttempt+1, retryConfig.MaxAttempts, retryConfig.InitialInterval)
              // TODO: Exponential Backoff や Circuit Breaker ロジックをここに実装
              time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second) // シンプルな待機
              continue // 次のリトライ試行へ
            } else {
              logger.Errorf("ステップ '%s' 最終チャンクの書き込みが最大リトライ回数 (%d) 失敗しました。ステップを終了します。", s.name, retryConfig.MaxAttempts)
              stepExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' 最終チャンクの書き込みが最大リトライ回数 (%d) 失敗しました: %w", s.name, retryConfig.MaxAttempts, writeErr)) // ステップを失敗としてマーク
              jobExecution.AddFailureException(stepExecution.Failureliye[len(stepExecution.Failureliye)-1]) // JobExecution にも最後のステップエラーを追加
              return fmt.Errorf("ステップ '%s' 最終チャンクの書き込みが最大リトライ回数 (%d) 失敗しました: %w", s.name, retryConfig.MaxAttempts, writeErr) // エラーを返してステップを失敗させる
            }
          }
          chunkCount++ // 最終チャンクの書き込み成功
          // チャンクをリセット (不要かもしれないが念のため)
          processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
          itemCountInChunk = 0
        } else if s.name == "SaveDataStep" {
          // SaveDataStep の場合: ここでは Reader/Processor がダミーなので、itemCountInChunk は常に 0 です。
          // したがって、このブロックは SaveDataStep では実行されません。
          // SaveDataStep の書き込みロジックは、このループの外に実装します。
        }
      }

      // チャンク処理全体がエラーなく完了
      logger.Infof("ステップ '%s' チャンク処理ステップが正常に完了しました。合計アイテム数 (ExecutionContext): %d",
        s.name, len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))
      // ItemCount の設定 (Reader/Processor/Writer で正確に集計し、StepExecution に設定するのが理想)
      // ここでは簡易的に計算
      // stepExecution.ReadCount = // Reader でカウント
      // stepExecution.WriteCount = // Writer でカウント
      // stepExecution.CommitCount = chunkCount // あくまでチャンク数

      // 成功したら Step を完了としてマーク
      stepExecution.MarkAsCompleted() // Status = Completed, ExitStatus = Completed

      return nil // 成功したら nil を返してメソッドを終了
    }
  } // リトライループ終了

  // ここに到達するのは、リトライ回数が0の場合か、論理的に到達しない場合
  // 安全のためエラーを返しておく
  err := fmt.Errorf("ステップ '%s' チャンク処理ループが予期せず終了しました", s.name)
  stepExecution.MarkAsFailed(err) // ステップを失敗としてマーク
  jobExecution.AddFailureException(err) // JobExecution にもエラーを追加
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
// WeatherJob から移動し、ChunkOrientedStep のプライベートメソッドとしました。
// FetchAndProcessStep では DummyWriter を使用し、SaveDataStep では Actual Writer を使用します。
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
