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
	repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
	stepListener "sample/src/main/go/batch/step/listener" // stepListener パッケージをインポート
	stepProcessor "sample/src/main/go/batch/step/processor" // stepProcessor パッケージをインポート
	stepReader "sample/src/main/go/batch/step/reader" // stepReader パッケージをインポート
	stepWriter "sample/src/main/go/batch/step/writer" // stepWriter パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

// JSLAdaptedStep は ItemReader, ItemProcessor, ItemWriter を使用するステップの実装です。
// core.Step インターフェースを実装します。
type JSLAdaptedStep struct {
	name          string // ステップ名
	reader        stepReader.Reader
	processor     stepProcessor.Processor
	writer        stepWriter.Writer
	chunkSize     int
	retryConfig   *config.RetryConfig // リトライ設定
	listeners     []stepListener.StepExecutionListener // このステップに固有のリスナー
	jobRepository repository.JobRepository // JobRepository を追加 (トランザクション管理のため)
}

// JSLAdaptedStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*JSLAdaptedStep)(nil)

// NewJSLAdaptedStep は新しい JSLAdaptedStep のインスタンスを作成します。
// ステップの依存関係と設定を受け取ります。
func NewJSLAdaptedStep(
	name string,
	reader stepReader.Reader,
	processor stepProcessor.Processor,
	writer stepWriter.Writer,
	chunkSize int,
	retryConfig *config.RetryConfig,
	jobRepository repository.JobRepository, // JobRepository を追加
	listeners []stepListener.StepExecutionListener, // ★ 追加: リスナーリスト
) *JSLAdaptedStep {
	return &JSLAdaptedStep{
		name:          name,
		reader:        reader,
		processor:     processor,
		writer:        writer,
		chunkSize:     chunkSize,
		retryConfig:   retryConfig,
		listeners:     listeners, // ★ 修正: 受け取ったリスナーを設定
		jobRepository: jobRepository, // JobRepository を設定
	}
}

// StepName はステップ名を返します。core.Step インターフェースの実装です。
func (s *JSLAdaptedStep) StepName() string {
	return s.name
}

// RegisterListener はこのステップに StepExecutionListener を登録します。
func (s *JSLAdaptedStep) RegisterListener(l stepListener.StepExecutionListener) {
	s.listeners = append(s.listeners, l)
}

// notifyBeforeStep は登録されている StepExecutionListener の BeforeStep メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.listeners {
		l.BeforeStep(ctx, stepExecution)
	}
}

// notifyAfterStep は登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
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
func (s *JSLAdaptedStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	logger.Infof("ステップ '%s' (Execution ID: %s) を開始します。", s.name, stepExecution.ID)

	// StepExecution の開始時刻を設定し、状態をマーク
	stepExecution.StartTime = time.Now()
	stepExecution.MarkAsStarted() // Status = Started

	// ステップ実行前処理の通知
	s.notifyBeforeStep(ctx, stepExecution)

	// StepExecution の ExecutionContext から Reader/Writer の状態を復元 (リスタート時)
	if len(stepExecution.ExecutionContext) > 0 {
		logger.Debugf("ステップ '%s': ExecutionContext から Reader/Writer の状態を復元します。", s.name)
		if readerEC, ok := stepExecution.ExecutionContext.Get("reader_context").(core.ExecutionContext); ok {
			if err := s.reader.SetExecutionContext(ctx, readerEC); err != nil {
				logger.Errorf("ステップ '%s': Reader の ExecutionContext 復元に失敗しました: %v", s.name, err)
				stepExecution.AddFailureException(err)
				return fmt.Errorf("Reader の ExecutionContext 復元エラー: %w", err)
			}
		}
		if writerEC, ok := stepExecution.ExecutionContext.Get("writer_context").(core.ExecutionContext); ok {
			if err := s.writer.SetExecutionContext(ctx, writerEC); err != nil {
				logger.Errorf("ステップ '%s': Writer の ExecutionContext 復元に失敗しました: %v", s.name, err)
				stepExecution.AddFailureException(err)
				return fmt.Errorf("Writer の ExecutionContext 復元エラー: %w", err)
			}
		}
	}


	// ステップ実行後処理 (defer で必ず実行)
	defer func() {
		// ステップの終了時刻を設定
		stepExecution.EndTime = time.Now()

		// Reader/Writer の Close を呼び出す
		if err := s.reader.Close(ctx); err != nil {
			logger.Errorf("ステップ '%s': Reader のクローズに失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(err)
		}
		if err := s.writer.Close(ctx); err != nil {
			logger.Errorf("ステップ '%s': Writer のクローズに失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(err)
		}

		// ステップ実行後処理の通知
		s.notifyAfterStep(ctx, stepExecution)

		// StepExecution の最終状態を JobRepository で更新する必要がある
		// これは Job.Run メソッド内で JobRepository を使用して行うことを想定
		// ここでは StepExecution オブジェクト自体は更新済み
	}()


	retryConfig := s.retryConfig
	chunkSize := s.chunkSize

	// 成功したチャンクの数
	var chunkCount int = 0
	var totalReadCount int = 0
	var totalWriteCount int = 0

	// ステップ名に応じて処理を分岐
	if s.name == "fetchWeatherDataStep" { // ★ 修正: "FetchAndProcessStep" -> "fetchWeatherDataStep"
		// チャンク処理全体のリトライループ
		for retryAttempt := 0; retryAttempt < retryConfig.MaxAttempts; retryAttempt++ {
			logger.Debugf("ステップ '%s' チャンク処理試行: %d/%d", s.name, retryAttempt+1, retryConfig.MaxAttempts)

			// リトライ時にはチャンクをリセット
			processedItemsChunk := make([]*entity.WeatherDataToStore, 0, chunkSize)
			itemCountInChunk := 0
			chunkAttemptError := false // この試行でエラーが発生したかを示すフラグ
			eofReached := false

			// トランザクションを開始
			tx, err := s.jobRepository.GetDB().BeginTx(ctx, nil)
			if err != nil {
				logger.Errorf("ステップ '%s': トランザクションの開始に失敗しました: %v", s.name, err)
				stepExecution.MarkAsFailed(fmt.Errorf("トランザクション開始エラー: %w", err))
				jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
				return err
			}

			// アイテムの読み込み、処理、チャンクへの追加を行うインナーループ
			for {
				select {
				case <-ctx.Done():
					logger.Warnf("Context がキャンセルされたため、ステップ '%s' のチャンク処理を中断します: %v", s.name, ctx.Err())
					stepExecution.MarkAsFailed(ctx.Err()) // ステップを失敗としてマーク
					jobExecution.AddFailureException(ctx.Err()) // JobExecution にもエラーを追加
					tx.Rollback() // トランザクションをロールバック
					return ctx.Err() // Context エラーは即座に返す
				default:
				}

				// 単一アイテムの読み込みと処理
				// processSingleItem は Reader/Processor エラーまたは Context キャンセルエラーを返す
				processedItemSlice, currentEOFReached, itemErr := s.processSingleItem(ctx, stepExecution)
				totalReadCount++ // 読み込みカウントをインクリメント
				stepExecution.ReadCount = totalReadCount // StepExecution に反映

				if itemErr != nil {
					// Reader または Processor でエラーが発生した場合
					logger.Errorf("ステップ '%s' アイテム処理でエラーが発生しました (試行 %d/%d): %v", s.name, retryAttempt+1, retryConfig.MaxAttempts, itemErr)
					chunkAttemptError = true // この試行はエラー
					break                    // インナーループを抜ける
				}

				// processSingleItem が nil アイテムを返した場合 (スキップされた場合)
				if processedItemSlice == nil && !currentEOFReached {
					continue // 次のアイテムへ
				}

				// 処理済みアイテムをチャンクに追加
				processedItemsChunk = append(processedItemsChunk, processedItemSlice...)
				itemCountInChunk = len(processedItemsChunk)
				eofReached = currentEOFReached // EOF 状態を更新

				// チャンクが満たされたら、または EOF に達したら処理
				if itemCountInChunk >= chunkSize || eofReached {
					// ★ 処理済みチャンクを ExecutionContext に追加 ★
					currentProcessedData, ok := jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)
					if !ok {
						currentProcessedData = make([]*entity.WeatherDataToStore, 0)
					}
					jobExecution.ExecutionContext.Put("processed_weather_data", append(currentProcessedData, processedItemsChunk...))
					logger.Debugf("ステップ '%s' 処理済みチャンク (%d 件) を ExecutionContext に追加しました。ExecutionContext合計: %d件",
						s.name, len(processedItemsChunk), len(jobExecution.ExecutionContext.Get("processed_weather_data").([]*entity.WeatherDataToStore)))

					totalWriteCount += len(processedItemsChunk) // 書き込みカウントをインクリメント
					stepExecution.WriteCount = totalWriteCount // StepExecution に反映

					// Reader/Writer の ExecutionContext を取得し、StepExecution に保存
					readerEC, err := s.reader.GetExecutionContext(ctx) // err はここでシャドウイングされる
					if err != nil {
						logger.Errorf("ステップ '%s': Reader の ExecutionContext 取得に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					writerEC, err := s.writer.GetExecutionContext(ctx) // err はここでシャドウイングされる
					if err != nil {
						logger.Errorf("ステップ '%s': Writer の ExecutionContext 取得に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					stepExecution.ExecutionContext.Put("reader_context", readerEC)
					stepExecution.ExecutionContext.Put("writer_context", writerEC)

					// StepExecution を更新してチェックポイントを永続化
					if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil { // err を再利用
						logger.Errorf("ステップ '%s': StepExecution の更新 (チェックポイント) に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					stepExecution.CommitCount++ // コミットカウントをインクリメント

					chunkCount++ // チャンク処理成功
					// チャンクをリセット
					processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
					itemCountInChunk = 0

					// EOF に達した場合はループを抜ける
					if eofReached {
						logger.Debugf("ステップ '%s' Reader 終端到達。最終チャンク処理完了。", s.name)
						break
					}
				}

				// Reader の終端に達した場合
				if eofReached {
					logger.Debugf("ステップ '%s' Reader からデータの終端に達しました。", s.name)
					break // インナーループを抜ける
				}
			} // インナーループ終了

			// インナーループ終了後の処理
			if chunkAttemptError {
				tx.Rollback() // エラーが発生した場合はロールバック
				stepExecution.RollbackCount++ // ロールバックカウントをインクリメント
				if retryAttempt < retryConfig.MaxAttempts-1 {
					// リトライ可能回数が残っている場合
					logger.Warnf("ステップ '%s' チャンク処理試行 %d/%d が失敗しました。リトライ間隔: %d秒", s.name, retryAttempt+1, retryConfig.MaxAttempts, retryConfig.InitialInterval)
					// TODO: Exponential Backoff や Circuit Breaker ロジックをここに実装
					time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second) // シンプルな待機
					// リトライ前に Reader/Writer の ExecutionContext をリセットする必要があるか検討
					// 現状は Reader/Writer が内部状態を保持し、SetExecutionContext で復元されるため、
					// ロールバックされたチャンクのデータは ExecutionContext に追加されない。
					// Reader の currentIndex はロールバックされたチャンクの開始時点に戻るべきだが、
					// 現在の Reader 実装では Read が進んでしまうため、SetExecutionContext で明示的に戻す必要がある。
					// これは Reader の SetExecutionContext が呼ばれることで、currentIndex が戻ることを期待する。
					// ただし、API呼び出しは毎回行われるため、API呼び出し自体が冪等である必要がある。
					// ここでは、Reader の SetExecutionContext が呼ばれることで、currentIndex が戻ることを期待する。
					if readerEC, ok := stepExecution.ExecutionContext.Get("reader_context").(core.ExecutionContext); ok {
						s.reader.SetExecutionContext(ctx, readerEC) // 前回のチェックポイントにReaderを戻す
					} else {
						s.reader.SetExecutionContext(ctx, core.NewExecutionContext()) // 初回またはECがない場合は初期状態に
					}
					s.writer.SetExecutionContext(ctx, core.NewExecutionContext()) // Writerは状態を持たないためクリア
				} else {
					// 最大リトライ回数に達した場合
					logger.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました。ステップを終了します。", s.name, retryConfig.MaxAttempts)
					stepExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました", s.name, retryConfig.MaxAttempts)) // ステップを失敗としてマーク
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1]) // JobExecution にも最後のステップエラーを追加
					return fmt.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました", s.name, retryConfig.MaxAttempts) // エラーを返してステップを失敗させる
				}
			} else {
				// この試行がエラーなく完了した場合（Reader 終端に達したか、Context キャンセル以外）
				// 残っているアイテムがあれば最終チャンクとして処理し、ExecutionContext に追加
				// このブロックは、インナーループ内で itemCountInChunk >= chunkSize || eofReached の条件で既に処理されているため、
				// ここで再度処理する必要はない。トランザクションのコミットはここで行う。
				if err = tx.Commit(); err != nil { // err を再利用
					logger.Errorf("ステップ '%s': トランザクションのコミットに失敗しました: %v", s.name, err)
					stepExecution.MarkAsFailed(fmt.Errorf("トランザクションコミットエラー: %w", err))
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
					return err
				}
				stepExecution.CommitCount++ // コミットカウントをインクリメント
				logger.Infof("ステップ '%s' チャンク処理ステップが正常に完了しました。合計チャンク数: %d, 合計読み込みアイテム数: %d, 合計書き込みアイテム数: %d",
					s.name, chunkCount, totalReadCount, totalWriteCount)
				stepExecution.ReadCount = totalReadCount
				stepExecution.WriteCount = totalWriteCount
				stepExecution.MarkAsCompleted() // ★ 正常終了時に Step を完了としてマーク
				return nil // 正常終了したらループを抜けて nil を返す
			}
		} // リトライループ終了
		// ここに到達するのは、リトライ回数を使い果たして失敗した場合のみ
		return fmt.Errorf("ステップ '%s' が最大リトライ回数を超えて失敗しました", s.name)
	}


	else if s.name == "saveWeatherDataStep" { // ★ 修正: "SaveDataStep" -> "saveWeatherDataStep"
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
		}

		// Writer でデータを書き込み (トランザクション内で実行)
		tx, err := s.jobRepository.GetDB().BeginTx(ctx, nil) // err はここでシャドウイングされる
		if err != nil {
			logger.Errorf("ステップ '%s': トランザクションの開始に失敗しました: %v", s.name, err)
			stepExecution.MarkAsFailed(fmt.Errorf("トランザクション開始エラー: %w", err))
			jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
			return err
		}

		// トランザクションの defer 処理
		defer func() {
			if r := recover(); r != nil { // パニックが発生した場合
				tx.Rollback()
				panic(r) // 再パニック
			}
			// ここに到達するのは、defer が実行される時点で err が nil の場合
			// しかし、tx.Commit() は defer の外で明示的に呼び出すべき
			// defer の中で err を参照してロールバックするかどうかを判断するロジックは、
			// defer の中で err を引数として受け取る関数リテラルにする必要がある。
			// 今回は defer の外で明示的にコミット/ロールバックするため、
			// ここでの err チェックは不要。
		}()

		writeErr := s.writer.Write(ctx, dataToStore) // ExecutionContext から取得したデータを渡す
		if writeErr != nil {
			// Writer エラー
			logger.Errorf("ステップ '%s' Writer でエラーが発生しました: %v", s.name, writeErr)
			stepExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' writer error: %w", s.name, writeErr))
			jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
			tx.Rollback() // ロールバック
			stepExecution.RollbackCount++
			return fmt.Errorf("ステップ '%s' writer error: %w", s.name, writeErr)
		}
		logger.Infof("ステップ '%s' Writer による書き込みが完了しました。", s.name)

		// Writer の ExecutionContext を取得し、StepExecution に保存 (Writer が状態を持つ場合)
		writerEC, err := s.writer.GetExecutionContext(ctx) // err はここでシャドウイングされる
		if err != nil {
			logger.Errorf("ステップ '%s': Writer の ExecutionContext 取得に失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(err)
			tx.Rollback()
			return fmt.Errorf("Writer の ExecutionContext 取得エラー: %w", err)
		}
		stepExecution.ExecutionContext.Put("writer_context", writerEC)

		// StepExecution を更新してチェックポイントを永続化
		if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil { // err を再利用
			logger.Errorf("ステップ '%s': StepExecution の更新 (チェックポイント) に失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(err)
			tx.Rollback()
			return fmt.Errorf("StepExecution の更新 (チェックポイント) エラー: %w", err)
		}
		stepExecution.CommitCount++ // コミットカウントをインクリメント

		// トランザクションをコミット
		if err = tx.Commit(); err != nil { // err を再利用
			logger.Errorf("ステップ '%s': トランザクションのコミットに失敗しました: %v", s.name, err)
			stepExecution.MarkAsFailed(fmt.Errorf("トランザクションコミットエラー: %w", err))
			jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
			return err
		}

		// WriteCount の設定
		stepExecution.WriteCount = len(dataToStore) // 書き込んだアイテム数

		// 成功したら Step を完了としてマーク
		stepExecution.MarkAsCompleted() // Status = Completed, ExitStatus = Completed

		return nil // 成功したら nil を返してメソッドを終了
	} else if s.name == "processDummyDataStep" { // ★ 追加: processDummyDataStep の処理
		logger.Infof("ステップ '%s' はダミー処理ステップです。ダミーデータを読み込み、処理します。", s.name)
		// ダミー処理のロジックをここに実装
		// 例: DummyReader から読み込み、DummyProcessor で処理し、DummyWriter で書き込む
		// fetchWeatherDataStep と同様のチャンク処理ループを適用
		for retryAttempt := 0; retryAttempt < retryConfig.MaxAttempts; retryAttempt++ { // 修正: for retryAttempt := 0; retryAttempt := 0; -> for retryAttempt := 0; retryAttempt < retryConfig.MaxAttempts;
			logger.Debugf("ステップ '%s' チャンク処理試行: %d/%d", s.name, retryAttempt+1, retryConfig.MaxAttempts)

			processedItemsChunk := make([]interface{}, 0, chunkSize) // DummyProcessor は interface{} を返す
			itemCountInChunk := 0
			chunkAttemptError := false
			eofReached := false

			tx, err := s.jobRepository.GetDB().BeginTx(ctx, nil)
			if err != nil {
				logger.Errorf("ステップ '%s': トランザクションの開始に失敗しました: %v", s.name, err)
				stepExecution.MarkAsFailed(fmt.Errorf("トランザクション開始エラー: %w", err))
				jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
				return err
			}

			for {
				select {
				case <-ctx.Done():
					logger.Warnf("Context がキャンセルされたため、ステップ '%s' のチャンク処理を中断します: %v", s.name, ctx.Err())
					stepExecution.MarkAsFailed(ctx.Err())
					jobExecution.AddFailureException(ctx.Err())
					tx.Rollback()
					return ctx.Err()
				default:
				}

				readItem, readerErr := s.reader.Read(ctx)
				totalReadCount++
				stepExecution.ReadCount = totalReadCount

				if readerErr != nil {
					if errors.Is(readerErr, io.EOF) {
						eofReached = true
						break
					}
					logger.Errorf("ステップ '%s' Reader error: %v", s.name, readerErr)
					stepExecution.AddFailureException(readerErr)
					chunkAttemptError = true
					break
				}

				if readItem == nil {
					continue
				}

				processedItem, processorErr := s.processor.Process(ctx, readItem)
				if processorErr != nil {
					logger.Errorf("ステップ '%s' Processor error: %v", s.name, processorErr)
					stepExecution.AddFailureException(processorErr)
					chunkAttemptError = true
					break
				}

				if processedItem != nil {
					processedItemsChunk = append(processedItemsChunk, processedItem)
					itemCountInChunk = len(processedItemsChunk)
				} else {
					stepExecution.FilterCount++
				}

				if itemCountInChunk >= chunkSize || eofReached {
					if len(processedItemsChunk) > 0 {
						writeErr := s.writer.Write(ctx, processedItemsChunk)
						if writeErr != nil {
							logger.Errorf("ステップ '%s' Writer error: %v", s.name, writeErr)
							stepExecution.AddFailureException(writeErr)
							chunkAttemptError = true
							break
						}
						totalWriteCount += len(processedItemsChunk)
						stepExecution.WriteCount = totalWriteCount
					}

					readerEC, err := s.reader.GetExecutionContext(ctx)
					if err != nil {
						logger.Errorf("ステップ '%s': Reader の ExecutionContext 取得に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					writerEC, err := s.writer.GetExecutionContext(ctx)
					if err != nil {
						logger.Errorf("ステップ '%s': Writer の ExecutionContext 取得に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					stepExecution.ExecutionContext.Put("reader_context", readerEC)
					stepExecution.ExecutionContext.Put("writer_context", writerEC)

					if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
						logger.Errorf("ステップ '%s': StepExecution の更新 (チェックポイント) に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					stepExecution.CommitCount++
					chunkCount++ // チャンク処理成功

					processedItemsChunk = make([]interface{}, 0, chunkSize)
					itemCountInChunk = 0

					if eofReached {
						break
					}
				}

				if eofReached {
					break
				}
			} // inner loop end

			if chunkAttemptError {
				tx.Rollback()
				stepExecution.RollbackCount++
				if retryAttempt < retryConfig.MaxAttempts-1 {
					logger.Warnf("ステップ '%s' チャンク処理試行 %d/%d が失敗しました。リトライ間隔: %d秒", s.name, retryAttempt+1, retryConfig.MaxAttempts, retryConfig.InitialInterval)
					time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
					if readerEC, ok := stepExecution.ExecutionContext.Get("reader_context").(core.ExecutionContext); ok {
						s.reader.SetExecutionContext(ctx, readerEC)
					} else {
						s.reader.SetExecutionContext(ctx, core.NewExecutionContext())
					}
					s.writer.SetExecutionContext(ctx, core.NewExecutionContext())
				} else {
					logger.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました。ステップを終了します。", s.name, retryConfig.MaxAttempts)
					stepExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました", s.name, retryConfig.MaxAttempts))
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
					return fmt.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました", s.name, retryConfig.MaxAttempts)
				}
			} else {
				if err = tx.Commit(); err != nil {
					logger.Errorf("ステップ '%s': トランザクションのコミットに失敗しました: %v", s.name, err)
					stepExecution.MarkAsFailed(fmt.Errorf("トランザクションコミットエラー: %w", err))
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
					return err
				}
				stepExecution.CommitCount++
				logger.Infof("ステップ '%s' チャンク処理ステップが正常に完了しました。合計チャンク数: %d, 合計読み込みアイテム数: %d, 合計書き込みアイテム数: %d",
					s.name, chunkCount, totalReadCount, totalWriteCount)
				stepExecution.ReadCount = totalReadCount
				stepExecution.WriteCount = totalWriteCount
				stepExecution.MarkAsCompleted() // Mark as completed here for dummy step
				return nil
			}
		} // retry loop end
		return fmt.Errorf("ステップ '%s' が最大リトライ回数を超えて失敗しました", s.name)
	}

	// 未知のステップ名の場合
	err := fmt.Errorf("ステップ '%s' の実行ロジックが定義されていません。", s.name)
	stepExecution.MarkAsFailed(err)
	jobExecution.AddFailureException(err)
	return err
}


// processSingleItem は Reader から1アイテム読み込み、Processor で処理します。
// 処理結果のスライス、EOFに達したかを示すフラグ、エラーを返します。
// WeatherJob から移動し、JSLAdaptedStep のプライベートメソッドとしました。
func (s *JSLAdaptedStep) processSingleItem(ctx context.Context, stepExecution *core.StepExecution) ([]*entity.WeatherDataToStore, bool, error) {
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
func (s *JSLAdaptedStep) writeChunk(ctx context.Context, stepExecution *core.StepExecution, chunkNum int, items []*entity.WeatherDataToStore) error {
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
