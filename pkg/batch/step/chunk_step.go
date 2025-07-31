package step

import (
	"context"
	"fmt"
	"io" // io.EOF のために追加
	"reflect" // reflect パッケージを追加
	"time"

	// "github.com/google/uuid" // UUID生成のためにインポート (NewStepExecutionで生成されるため不要)

	"sample/pkg/batch/config"
	core "sample/pkg/batch/job/core"
	repository "sample/pkg/batch/repository"
	stepListener "sample/pkg/batch/step/listener"
	itemprocessor "sample/pkg/batch/step/processor" // Renamed import
	itemreader "sample/pkg/batch/step/reader"       // Renamed import
	itemwriter "sample/pkg/batch/step/writer"       // Renamed import
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// ChunkStep はチャンク指向のステップを実装します。
// Reader, Processor, Writer を使用してアイテムを処理します。
type ChunkStep struct { // ジェネリクス型を削除
	name string
	reader itemreader.ItemReader[any] // any に戻す
	processor itemprocessor.ItemProcessor[any, any] // any に戻す
	writer itemwriter.ItemWriter[any] // any に戻す
	chunkSize int
	jobRepository repository.JobRepository

	// リスナー
	stepListeners []stepListener.StepExecutionListener
	itemReadListeners []core.ItemReadListener
	itemProcessListeners []core.ItemProcessListener
	itemWriteListeners []core.ItemWriteListener
	skipListeners []stepListener.SkipListener
	retryItemListeners []stepListener.RetryItemListener

	// アイテムレベルのリトライ・スキップ設定
	itemRetryConfig config.ItemRetryConfig
	itemSkipConfig config.ItemSkipConfig
}

// NewChunkStep は新しい ChunkStep のインスタンスを作成します。
func NewChunkStep[I, O any]( // ジェネリクス型はコンストラクタの引数にのみ残す
	name string,
	r itemreader.ItemReader[I],
	p itemprocessor.ItemProcessor[I, O],
	w itemwriter.ItemWriter[O],
	chunkSize int,
	repo repository.JobRepository,
	stepLs []stepListener.StepExecutionListener,
	itemReadLs []core.ItemReadListener,
	itemProcessLs []core.ItemProcessListener,
	itemWriteLs []core.ItemWriteListener,
	skipLs []stepListener.SkipListener,
	retryItemLs []stepListener.RetryItemListener,
	itemRetryCfg config.ItemRetryConfig,
	itemSkipCfg config.ItemSkipConfig,
) *ChunkStep { // ジェネリクス型を削除
	return &ChunkStep{ // ジェネリクス型を削除
		name:                 name,
		reader:               r.(itemreader.ItemReader[any]), // any にキャスト
		processor:            p.(itemprocessor.ItemProcessor[any, any]), // any にキャスト
		writer:               w.(itemwriter.ItemWriter[any]), // any にキャスト
		chunkSize:            chunkSize,
		jobRepository:        repo,
		stepListeners:        stepLs,
		itemReadListeners:    itemReadLs,
		itemProcessListeners: itemProcessLs,
		itemWriteListeners:   itemWriteLs,
		skipListeners:        skipLs,
		retryItemListeners:   retryItemLs,
		itemRetryConfig:      itemRetryCfg,
		itemSkipConfig:       itemSkipCfg,
	}
}

// ID はステップのIDを返します。
func (cs *ChunkStep) ID() string { // ジェネリクス型を削除
	return cs.name
}

// StepName はステップの名前を返します。
func (cs *ChunkStep) StepName() string { // ジェネリクス型を削除
	return cs.name
}

// Execute はチャンクステップのビジネスロジックを実行します。
func (cs *ChunkStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error { // ジェネリクス型を削除
	logger.Infof("ステップ '%s' の実行を開始します。", cs.name)

	// StepExecution は Job.Run メソッドで既に初期化され、JobExecution に追加されていることを想定
	// ここでは、その StepExecution の状態を更新し、永続化する
	stepExecution.StartTime = time.Now()
	stepExecution.Status = core.BatchStatusStarting
	stepExecution.LastUpdated = time.Now()
	// ExecutionContext は NewStepExecution で初期化済み、またはリスタート時にロード済み

	// Reader, Writer の ExecutionContext をステップの ExecutionContext から復元
	// Reader, Writer, Processor は ExecutionContext を持つインターフェースを実装していると仮定
	if err := cs.reader.SetExecutionContext(ctx, stepExecution.ExecutionContext); err != nil {
		return exception.NewBatchError("chunk_step", "Reader の ExecutionContext 設定に失敗しました", err, false, false)
	}
	// Processor は通常 ExecutionContext を直接持たないが、もし持つなら同様に設定
	// if err := cs.processor.SetExecutionContext(ctx, stepExecution.ExecutionContext); err != nil { ... }
	if err := cs.writer.SetExecutionContext(ctx, stepExecution.ExecutionContext); err != nil {
		return exception.NewBatchError("chunk_step", "Writer の ExecutionContext 設定に失敗しました", err, false, false)
	}

	// BeforeStep リスナーの呼び出し
	for _, listener := range cs.stepListeners {
		listener.BeforeStep(ctx, stepExecution)
	}

	stepExecution.MarkAsStarted()
	if err := cs.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
		return exception.NewBatchError("chunk_step", fmt.Sprintf("StepExecution (ID: %s) の状態更新に失敗しました", stepExecution.ID), err, false, false)
	}

	var readError error
	var processError error
	var writeError error
	var totalReadCount int
	var totalWriteCount int
	var totalFilterCount int
	var totalSkipReadCount int
	var totalSkipProcessCount int
	var totalSkipWriteCount int

	defer func() {
		// Reader, Writer の ExecutionContext をステップの ExecutionContext に保存
		if ec, err := cs.reader.GetExecutionContext(ctx); err == nil {
			for k, v := range ec {
				stepExecution.ExecutionContext[k] = v
			}
		} else {
			logger.Errorf("Reader の ExecutionContext 取得に失敗しました: %v", err)
		}
		if ec, err := cs.writer.GetExecutionContext(ctx); err == nil {
			for k, v := range ec {
				stepExecution.ExecutionContext[k] = v
			}
		} else {
			logger.Errorf("Writer の ExecutionContext 取得に失敗しました: %v", err)
		}

		// 最終的な StepExecution の状態を更新
		stepExecution.ReadCount = totalReadCount
		stepExecution.WriteCount = totalWriteCount
		stepExecution.FilterCount = totalFilterCount
		stepExecution.SkipReadCount = totalSkipReadCount
		stepExecution.SkipProcessCount = totalSkipProcessCount
		stepExecution.SkipWriteCount = totalSkipWriteCount

		// AfterStep リスナーの呼び出し
		for _, listener := range cs.stepListeners {
			listener.AfterStep(ctx, stepExecution)
		}

		// 最終的な StepExecution の永続化
		if err := cs.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
			logger.Errorf("ステップ '%s' の最終 StepExecution (ID: %s) の更新に失敗しました: %v", cs.name, stepExecution.ID, err)
		}
		logger.Infof("ステップ '%s' の実行が完了しました。ステータス: %s, 終了ステータス: %s", cs.name, stepExecution.Status, stepExecution.ExitStatus)
	}()

	// チャンク処理ループ
ChunkLoop: // ラベルを追加
	for {
		select {
		case <-ctx.Done():
			stepExecution.MarkAsFailed(ctx.Err())
			stepExecution.ExitStatus = core.ExitStatusStopped
			logger.Warnf("ステップ '%s' がコンテキストキャンセルにより停止されました: %v", cs.name, ctx.Err())
			return ctx.Err()
		default:
			// チャンクの開始
			currentChunkReadCount := 0
			currentChunkProcessedItems := make([]any, 0, cs.chunkSize) // O を any に変更

			var lastReadError error // チャンク内の最後の読み込みエラーを保持
			// トランザクションの開始
			tx, err := cs.jobRepository.GetDB().BeginTx(ctx, nil)
			if err != nil {
				stepExecution.MarkAsFailed(exception.NewBatchError("chunk_step", "トランザクションの開始に失敗しました", err, false, false)) // 引数の順序と型を修正
				return err
			}

			// Read フェーズ
			for i := 0; i < cs.chunkSize; i++ {
				var itemI any // I を any に変更
				readAttempts := 0
				for { // リトライループ
					readAttempts++
					itemI, readError = cs.reader.Read(ctx)
					if readError == nil {
						break // 読み込み成功
					}

					// io.EOF はデータの終端を示すため、エラーではない。ただし、このチャンクの読み込みは終了。
					if readError == io.EOF {
						lastReadError = io.EOF // EOFを記録
						break
					}

					// 読み込みエラーハンドリング
					batchErr, isBatchErr := readError.(*exception.BatchError)
					if isBatchErr && batchErr.IsRetryable() && readAttempts <= cs.itemRetryConfig.MaxAttempts {
						logger.Warnf("アイテム読み込みエラー (リトライ可能): %v (試行回数: %d/%d)", readError, readAttempts, cs.itemRetryConfig.MaxAttempts)
						for _, listener := range cs.retryItemListeners {
							listener.OnRetryRead(ctx, readError)
						}
						time.Sleep(time.Duration(cs.itemRetryConfig.InitialInterval) * time.Millisecond) // シンプルな固定遅延
						continue
					}

					// リトライ不可または最大試行回数を超えた場合
					for _, listener := range cs.itemReadListeners {
						listener.OnReadError(ctx, readError)
					}

					if isBatchErr && batchErr.IsSkippable() && totalSkipReadCount < cs.itemSkipConfig.SkipLimit {
						logger.Warnf("アイテム読み込みエラー (スキップ可能): %v (スキップカウント: %d/%d)", readError, totalSkipReadCount+1, cs.itemSkipConfig.SkipLimit)
						for _, listener := range cs.skipListeners {
							listener.OnSkipRead(ctx, readError)
						}
						totalSkipReadCount++
						readError = nil // スキップしたのでエラーをクリアし、次のアイテムへ
						continue
					}

					// 致命的な読み込みエラー
					stepExecution.MarkAsFailed(readError) // ここで readError は io.EOF ではない
					stepExecution.ExitStatus = core.ExitStatusFailed
					_ = tx.Rollback() // トランザクションをロールバック
					return readError
				}

				// Readerがnilを返したら終了 (io.EOFでbreakした場合、itemIはnil)
				// reflectIsZero はポインタ型でない場合にゼロ値を正しく判定するために使用
				if itemI == nil { // nil チェックで十分
					break
				}
				if reflect.ValueOf(itemI).Kind() != reflect.Ptr && reflectIsZero(itemI) {
					break
				}

				currentChunkReadCount++
				totalReadCount++

				// Process フェーズ
				var itemO any // O を any に変更
				processAttempts := 0
				for { // リトライループ
					processAttempts++
					itemO, processError = cs.processor.Process(ctx, itemI)
					if processError == nil {
						break // 処理成功
					}

					// 処理エラーハンドリング
					batchErr, isBatchErr := processError.(*exception.BatchError)
					if isBatchErr && batchErr.IsRetryable() && processAttempts <= cs.itemRetryConfig.MaxAttempts {
						logger.Warnf("アイテム処理エラー (リトライ可能): %v (試行回数: %d/%d)", processError, processAttempts, cs.itemRetryConfig.MaxAttempts)
						for _, listener := range cs.retryItemListeners {
							listener.OnRetryProcess(ctx, itemI, processError)
						}
						time.Sleep(time.Duration(cs.itemRetryConfig.InitialInterval) * time.Millisecond)
						continue
					}

					// リトライ不可または最大試行回数を超えた場合
					for _, listener := range cs.itemProcessListeners {
						listener.OnProcessError(ctx, itemI, processError)
					}

					if isBatchErr && batchErr.IsSkippable() && totalSkipProcessCount < cs.itemSkipConfig.SkipLimit {
						logger.Warnf("アイテム処理エラー (スキップ可能): %v (スキップカウント: %d/%d)", processError, totalSkipProcessCount+1, cs.itemSkipConfig.SkipLimit)
						for _, listener := range cs.skipListeners {
							listener.OnSkipProcess(ctx, itemI, processError)
						}
						totalSkipProcessCount++
						processError = nil // スキップしたのでエラーをクリアし、次のアイテムへ
						itemO = nil // 処理結果を nil にリセット
						break // 処理をスキップしたので、このアイテムの処理は終了
					}

					// 致命的な処理エラー
					stepExecution.MarkAsFailed(processError)
					stepExecution.ExitStatus = core.ExitStatusFailed
					_ = tx.Rollback() // トランザクションをロールバック
					return processError
				}

				// 処理結果がnilの場合（フィルタリングされた場合）
				// reflectIsZero はポインタ型でない場合にゼロ値を正しく判定するために使用
				if itemO == nil { // nil チェックで十分
					totalFilterCount++
					continue
				}
				if reflect.ValueOf(itemO).Kind() != reflect.Ptr && reflectIsZero(itemO) {
					totalFilterCount++
					continue
				}
				currentChunkProcessedItems = append(currentChunkProcessedItems, itemO)
			}

			// 読み込みアイテムがチャンクサイズ未満であれば、データの終端に達したと判断
			if lastReadError == io.EOF { // 最後の読み込みでEOFだった場合、チャンク処理を終了
				_ = tx.Rollback() // 空のトランザクションはロールバック
				break ChunkLoop // ラベル付き break で外側のループを終了
			}

			// Write フェーズ
			if len(currentChunkProcessedItems) > 0 {
				writeAttempts := 0
				for { // リトライループ
					writeAttempts++
					writeError = cs.writer.Write(ctx, tx, currentChunkProcessedItems) // currentChunkProcessedItems は []any
					if writeError == nil {
						break // 書き込み成功
					}

					// 書き込みエラーハンドリング
					batchErr, isBatchErr := writeError.(*exception.BatchError)
					if isBatchErr && batchErr.IsRetryable() && writeAttempts <= cs.itemRetryConfig.MaxAttempts {
						logger.Warnf("アイテム書き込みエラー (リトライ可能): %v (試行回数: %d/%d)", writeError, writeAttempts, cs.itemRetryConfig.MaxAttempts)
						for _, listener := range cs.retryItemListeners {
							listener.OnRetryWrite(ctx, convertToInterfaceSlice(currentChunkProcessedItems), writeError)
						}
						time.Sleep(time.Duration(cs.itemRetryConfig.InitialInterval) * time.Millisecond)
						continue
					}

					// リトライ不可または最大試行回数を超えた場合
					for _, listener := range cs.itemWriteListeners {
						listener.OnWriteError(ctx, convertToInterfaceSlice(currentChunkProcessedItems), writeError)
					}

					// Writeフェーズでのスキップは、通常、チャンク全体をロールバックし、
					// エラーアイテムを除外して再試行するか、ジョブを失敗させる。
					// ここではシンプルに、スキップ可能ならチャンク全体をスキップ（ロールバック）し、
					// ジョブを失敗させないが、スキップカウントを増やす。
					// Spring Batch の SkipPolicy はより複雑。
					if isBatchErr && batchErr.IsSkippable() && totalSkipWriteCount < cs.itemSkipConfig.SkipLimit {
						logger.Warnf("アイテム書き込みエラー (スキップ可能): %v (スキップカウント: %d/%d)", writeError, totalSkipWriteCount+1, cs.itemSkipConfig.SkipLimit)
						for _, listener := range cs.skipListeners {
							// 個々のアイテムではなく、チャンク全体がスキップされることを想定
							for _, item := range currentChunkProcessedItems {
								listener.OnSkipWrite(ctx, item, writeError)
							}
						}
						totalSkipWriteCount += len(currentChunkProcessedItems) // チャンク内の全アイテムをスキップとしてカウント
						_ = tx.Rollback() // スキップなのでロールバック
						stepExecution.RollbackCount++
						writeError = nil // スキップしたのでエラーをクリアし、次のチャンクへ
						break // 書き込みをスキップしたので、このチャンクの処理は終了
					}

					// 致命的な書き込みエラー
					stepExecution.MarkAsFailed(writeError)
					stepExecution.ExitStatus = core.ExitStatusFailed
					_ = tx.Rollback() // トランザクションをロールバック
					stepExecution.RollbackCount++
					return writeError
				}
			}

			// 書き込みエラーがなければコミット
			if writeError == nil {
				if err := tx.Commit(); err != nil {
					stepExecution.MarkAsFailed(exception.NewBatchError("chunk_step", "トランザクションのコミットに失敗しました", err, false, false))
					stepExecution.RollbackCount++ // コミット失敗はロールバックとみなす
					_ = tx.Rollback() // コミット失敗時のロールバック
					return err
				}
				stepExecution.CommitCount++
				totalWriteCount += len(currentChunkProcessedItems)
				logger.Debugf("ステップ '%s': %d アイテムを読み込み、%d アイテムを処理し、%d アイテムを書き込みました。コミットカウント: %d",
					cs.name, currentChunkReadCount, len(currentChunkProcessedItems), len(currentChunkProcessedItems), stepExecution.CommitCount)
			} else {
				// writeError が発生し、かつスキップされた場合は、既にロールバック済み
				logger.Debugf("ステップ '%s': チャンク処理中に書き込みエラーが発生し、スキップされました。", cs.name)
			}

		}
	}

	stepExecution.MarkAsCompleted()
	stepExecution.ExitStatus = core.ExitStatusCompleted
	return nil
}

// Close はリソースを解放するためのメソッドです。
func (cs *ChunkStep) Close(ctx context.Context) error { // ジェネリクス型を削除
	var errs []error
	if err := cs.reader.Close(ctx); err != nil {
		errs = append(errs, exception.NewBatchError("chunk_step", "Reader のクローズに失敗しました", err, false, false))
	}
	// Processor は通常 Close メソッドを持たないが、もし持つなら呼び出す
	// if err := cs.processor.Close(ctx); err != nil { errs = append(errs, err) }
	if err := cs.writer.Close(ctx); err != nil {
		errs = append(errs, exception.NewBatchError("chunk_step", "Writer のクローズに失敗しました", err, false, false))
	}

	if len(errs) > 0 {
		return fmt.Errorf("複数のクローズエラーが発生しました: %v", errs)
	}
	logger.Debugf("ステップ '%s' のリソースを閉じました。", cs.name)
	return nil
}

// SetExecutionContext は ExecutionContext を設定します。
// ChunkStep 自体は ExecutionContext を直接保持せず、Reader/Writer に委譲します。
func (cs *ChunkStep) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error { // ジェネリクス型を削除
	// Reader と Writer の ExecutionContext を設定
	if err := cs.reader.SetExecutionContext(ctx, ec); err != nil {
		return exception.NewBatchError("chunk_step", "Reader の ExecutionContext 設定に失敗しました", err, false, false)
	}
	if err := cs.writer.SetExecutionContext(ctx, ec); err != nil {
		return exception.NewBatchError("chunk_step", "Writer の ExecutionContext 設定に失敗しました", err, false, false)
	}
	return nil
}

// GetExecutionContext は ExecutionContext を取得します。
// Reader と Writer の ExecutionContext をマージして返します。
func (cs *ChunkStep) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) { // ジェネリクス型を削除
	mergedEC := core.NewExecutionContext()

	if ec, err := cs.reader.GetExecutionContext(ctx); err == nil {
		for k, v := range ec {
			mergedEC[k] = v
		}
	} else {
		return nil, exception.NewBatchError("chunk_step", "Reader の ExecutionContext 取得に失敗しました", err, false, false)
	}

	if ec, err := cs.writer.GetExecutionContext(ctx); err == nil {
		for k, v := range ec {
			mergedEC[k] = v
		}
	} else {
		return nil, exception.NewBatchError("chunk_step", "Writer の ExecutionContext 取得に失敗しました", err, false, false)
	}

	return mergedEC, nil
}

// reflectIsZero は reflect パッケージを使用して、任意の型のゼロ値をチェックします。
// ジェネリクス型 O が nil を許容しないプリミティブ型の場合に役立ちます。
func reflectIsZero(v any) bool { // T を any に変更
	return reflect.ValueOf(&v).Elem().IsZero()
}

// convertToInterfaceSlice は任意の型のスライスを []interface{} に変換します。
// リスナーに渡すために必要です。
func convertToInterfaceSlice(slice any) []interface{} { // T を any に変更
	if slice == nil {
		return nil
	}
	// slice は []any 型として渡されることを想定
	concreteSlice, ok := slice.([]any)
	if !ok {
		// This should ideally not happen if currentChunkProcessedItems is always []any
		// but for safety, we can log or return an error.
		logger.Errorf("convertToInterfaceSlice: 予期しないスライス型です: %T", slice)
		return nil
	}
	result := make([]interface{}, len(concreteSlice))
	for i, v := range concreteSlice {
		result[i] = v
	}
	return result
}

// ChunkStep が core.Step インターフェースを満たすことを確認 (ジェネリクス型を削除)
var _ core.Step = (*ChunkStep)(nil)
