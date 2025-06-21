package step

import (
	"context"
	"errors"
	"encoding/json" // json.UnmarshalTypeError のためにインポート
	"net" // net.OpError のためにインポート
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
	exception "sample/src/main/go/batch/util/exception" // exception パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

// JSLAdaptedStep は ItemReader, ItemProcessor, ItemWriter を使用するステップの実装です。
// core.Step インターフェースを実装します。
// Reader, Processor, Writer はジェネリックインターフェースですが、
// JSLAdaptedStep は core.Step (非ジェネリック) を実装するため、
// これらのフィールドは `any` 型引数を持つジェネリックインターフェースとして保持し、
// 内部で適切な型アサーションを行います。
type JSLAdaptedStep struct {
	name          string // ステップ名
	reader        stepReader.Reader[any] // Reader[O any]
	processor     stepProcessor.Processor[any, any] // Processor[I, O any]
	writer        stepWriter.Writer[any] // Writer[I any]
	chunkSize     int
	stepRetryConfig   *config.RetryConfig // ステップレベルのリトライ設定 (チャンク処理全体のリトライ)
	itemRetryConfig   config.ItemRetryConfig // アイテムレベルのリトライ設定
	itemSkipConfig    config.ItemSkipConfig  // アイテムレベルのスキップ設定
	stepListeners     []stepListener.StepExecutionListener // ステップレベルのリスナー
	itemReadListeners []core.ItemReadListener // アイテム読み込みリスナー
	itemProcessListeners []core.ItemProcessListener // アイテム処理リスナー
	itemWriteListeners []core.ItemWriteListener // アイテム書き込みリスナー
	skipListeners     []stepListener.SkipListener // スキップリスナー
	retryItemListeners []stepListener.RetryItemListener // アイテムリトライリスナー
	jobRepository repository.JobRepository // JobRepository を追加 (トランザクション管理のため)
}

// JSLAdaptedStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*JSLAdaptedStep)(nil)

// NewJSLAdaptedStep は新しい JSLAdaptedStep のインスタンスを作成します。
// ステップの依存関係と設定、および各種リスナーを受け取ります。
// reader, processor, writer は any 型引数を持つジェネリックインターフェースとして受け取ります。
func NewJSLAdaptedStep(
	name string,
	reader stepReader.Reader[any], // Reader[any]
	processor stepProcessor.Processor[any, any], // Processor[any, any]
	writer stepWriter.Writer[any], // Writer[any]
	chunkSize int,
	stepRetryConfig *config.RetryConfig,
	itemRetryConfig config.ItemRetryConfig,
	itemSkipConfig  config.ItemSkipConfig,
	jobRepository repository.JobRepository, // JobRepository を追加
	stepListeners []stepListener.StepExecutionListener, // ステップレベルリスナー
	itemReadListeners []core.ItemReadListener, // アイテム読み込みリスナー
	itemProcessListeners []core.ItemProcessListener, // アイテム処理リスナー
	itemWriteListeners []core.ItemWriteListener, // アイテム書き込みリスナー
	skipListeners []stepListener.SkipListener, // スキップリスナー
	retryItemListeners []stepListener.RetryItemListener, // アイテムリトライリスナー
) *JSLAdaptedStep {
	return &JSLAdaptedStep{
		name:          name,
		reader:        reader,
		processor:     processor,
		writer:        writer,
		chunkSize:     chunkSize,
		stepRetryConfig:   stepRetryConfig,
		itemRetryConfig:   itemRetryConfig,
		itemSkipConfig:    itemSkipConfig,
		stepListeners:     stepListeners,
		itemReadListeners: itemReadListeners,
		itemProcessListeners: itemProcessListeners,
		itemWriteListeners: itemWriteListeners,
		skipListeners:     skipListeners,
		retryItemListeners: retryItemListeners,
		jobRepository: jobRepository, // JobRepository を設定
	}
}

// StepName はステップ名を返します。core.Step インターフェースの実装です。
func (s *JSLAdaptedStep) StepName() string {
	return s.name
}

// RegisterListener はこのステップに StepExecutionListener を登録します。
// NewJSLAdaptedStep でリスナーを受け取るように変更したため、このメソッドは不要になる可能性がありますが、
// 実行時に動的にリスナーを追加するユースケースのために残しておくこともできます。
func (s *JSLAdaptedStep) RegisterListener(l stepListener.StepExecutionListener) {
	s.stepListeners = append(s.stepListeners, l)
}

// notifyBeforeStep は登録されている StepExecutionListener の BeforeStep メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.stepListeners {
		l.BeforeStep(ctx, stepExecution)
	}
}

// notifyAfterStep は登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.stepListeners {
		l.AfterStep(ctx, stepExecution)
	}
}

// notifyItemReadError は登録されている ItemReadListener の OnReadError メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyItemReadError(ctx context.Context, err error) {
	for _, l := range s.itemReadListeners {
		l.OnReadError(ctx, err)
	}
}

// notifyItemProcessError は登録されている ItemProcessListener の OnProcessError メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyItemProcessError(ctx context.Context, item interface{}, err error) {
	for _, l := range s.itemProcessListeners {
		l.OnProcessError(ctx, item, err)
	}
}

// notifySkipInProcess は登録されている ItemProcessListener の OnSkipInProcess メソッドを呼び出します。
func (s *JSLAdaptedStep) notifySkipInProcess(ctx context.Context, item interface{}, err error) {
	for _, l := range s.itemProcessListeners {
		l.OnSkipInProcess(ctx, item, err)
	}
}

// notifyItemWriteError は登録されている ItemWriteListener の OnWriteError メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyItemWriteError(ctx context.Context, items []interface{}, err error) {
	for _, l := range s.itemWriteListeners {
		l.OnWriteError(ctx, items, err)
	}
}

// notifySkipInWrite は登録されている ItemWriteListener の OnSkipInWrite メソッドを呼び出します。
func (s *JSLAdaptedStep) notifySkipInWrite(ctx context.Context, item interface{}, err error) {
	for _, l := range s.itemWriteListeners {
		l.OnSkipInWrite(ctx, item, err)
	}
}

// notifySkipRead は登録されている SkipListener の OnSkipRead メソッドを呼び出します。
func (s *JSLAdaptedStep) notifySkipRead(ctx context.Context, err error) {
	for _, l := range s.skipListeners {
		l.OnSkipRead(ctx, err)
	}
}

// notifySkipProcess は登録されている SkipListener の OnSkipProcess メソッドを呼び出します。
func (s *JSLAdaptedStep) notifySkipProcess(ctx context.Context, item interface{}, err error) {
	for _, l := range s.skipListeners {
		l.OnSkipProcess(ctx, item, err)
	}
}

// notifySkipWrite は登録されている SkipListener の OnSkipWrite メソッドを呼び出します。
func (s *JSLAdaptedStep) notifySkipWrite(ctx context.Context, item interface{}, err error) {
	for _, l := range s.skipListeners {
		l.OnSkipWrite(ctx, item, err)
	}
}

// notifyRetryRead は登録されている RetryItemListener の OnRetryRead メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyRetryRead(ctx context.Context, err error) {
	for _, l := range s.retryItemListeners {
		l.OnRetryRead(ctx, err)
	}
}

// notifyRetryProcess は登録されている RetryItemListener の OnRetryProcess メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyRetryProcess(ctx context.Context, item interface{}, err error) {
	for _, l := range s.retryItemListeners {
		l.OnRetryProcess(ctx, item, err)
	}
}

// notifyRetryWrite は登録されている RetryItemListener の OnRetryWrite メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyRetryWrite(ctx context.Context, items []interface{}, err error) {
	for _, l := range s.retryItemListeners {
		l.OnRetryWrite(ctx, items, err)
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
				return exception.NewBatchError(s.name, "Reader の ExecutionContext 復元エラー", err, false, false)
			}
		}
		if writerEC, ok := stepExecution.ExecutionContext.Get("writer_context").(core.ExecutionContext); ok {
			if err := s.writer.SetExecutionContext(ctx, writerEC); err != nil {
				logger.Errorf("ステップ '%s': Writer の ExecutionContext 復元に失敗しました: %v", s.name, err)
				stepExecution.AddFailureException(err)
				return exception.NewBatchError(s.name, "Writer の ExecutionContext 復元エラー", err, false, false)
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


	stepRetryConfig := s.stepRetryConfig
	chunkSize := s.chunkSize

	// 成功したチャンクの数
	var chunkCount int = 0
	var totalReadCount int = 0
	var totalWriteCount int = 0

	// ステップ名に応じて処理を分岐
	if s.name == "fetchWeatherDataStep" {
		// チャンク処理全体のリトライループ
		for retryAttempt := 0; retryAttempt < stepRetryConfig.MaxAttempts; retryAttempt++ {
			logger.Debugf("ステップ '%s' チャンク処理試行: %d/%d", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts)

			// リトライ時にはチャンクをリセット
			processedItemsChunk := make([]*entity.WeatherDataToStore, 0, chunkSize)
			itemCountInChunk := 0
			chunkAttemptError := false // この試行でエラーが発生したかを示すフラグ
			eofReached := false

			// トランザクションを開始
			tx, err := s.jobRepository.GetDB().BeginTx(ctx, nil)
			if err != nil {
				logger.Errorf("ステップ '%s': トランザクションの開始に失敗しました: %v", s.name, err)
				stepExecution.MarkAsFailed(exception.NewBatchError(s.name, "トランザクション開始エラー", err, false, false))
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
				// Reader の出力は *entity.OpenMeteoForecast
				// Processor の出力は []*entity.WeatherDataToStore
				processedItemSlice, currentEOFReached, itemErr := s.processSingleItem(ctx, stepExecution, retryAttempt) // retryAttempt を渡す
				totalReadCount++ // 読み込みカウントをインクリメント
				stepExecution.ReadCount = totalReadCount // StepExecution に反映

				if itemErr != nil {
					// Reader または Processor でエラーが発生した場合
					logger.Errorf("ステップ '%s' アイテム処理でエラーが発生しました (試行 %d/%d): %v", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts, itemErr)
					chunkAttemptError = true // この試行はエラー
					// アイテムレベルのリトライ/スキップは processSingleItem 内で処理されるため、ここではチャンクエラーとして扱う
					break                    // インナーループを抜ける
				}

				// processSingleItem が nil アイテムを返した場合 (スキップされた場合)
				if processedItemSlice == nil && !currentEOFReached {
					continue // 次のアイテムへ
				}

				// 処理済みアイテムをチャンクに追加
				// processedItemSlice は []*entity.WeatherDataToStore 型
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

					// Writer でデータを書き込み (チャンク全体を一度に書き込む)
					var writeErr error
					// []*entity.WeatherDataToStore を []any に変換
					itemsForWriter := make([]any, len(processedItemsChunk))
					for i, item := range processedItemsChunk {
						itemsForWriter[i] = item
					}

					writeErr = s.writer.Write(ctx, itemsForWriter) // ★ チャンク全体を渡す
					if writeErr != nil {
						logger.Errorf("ステップ '%s' チャンク書き込みエラー: %v", s.name, writeErr)
						stepExecution.AddFailureException(writeErr)
						chunkAttemptError = true // チャンク全体のエラーとする
						break // このチャンクの処理を中断
					}
					totalWriteCount += len(processedItemsChunk) // 書き込みカウントをインクリメント (成功したアイテム数)
					stepExecution.WriteCount = totalWriteCount // StepExecution に反映


					if chunkAttemptError {
						break // インナーループを抜ける
					}

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
						chunkAttemptError = true // StepExecution の永続化エラーもチャンクエラー
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
				if retryAttempt < stepRetryConfig.MaxAttempts-1 {
					// リトライ可能回数が残っている場合
					logger.Warnf("ステップ '%s' チャンク処理試行 %d/%d が失敗しました。リトライ間隔: %d秒", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts, stepRetryConfig.InitialInterval)
					// TODO: Exponential Backoff や Circuit Breaker ロジックをここに実装
					time.Sleep(time.Duration(s.stepRetryConfig.InitialInterval) * time.Second) // シンプルな待機
					// リトライ前に Reader/Writer の ExecutionContext をリセットする必要があるか検討
					// 現状は Reader/Writer が内部状態を保持し、SetExecutionContext で復元されるため、
					// ロールバックされたチャンクのデータは ExecutionContext に追加されない。
					// Reader の currentIndex はロールバックされたチャンクの開始時点に戻るべきだが、
					// 現在の Reader 実装では Read が進んでしまうため、SetExecutionContext で明示的に戻す必要がある。
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
					logger.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗しました。ステップを終了します。", s.name, stepRetryConfig.MaxAttempts)
					stepExecution.MarkAsFailed(exception.NewBatchErrorf(s.name, "チャンク処理が最大リトライ回数 (%d) 失敗しました", stepRetryConfig.MaxAttempts)) // ステップを失敗としてマーク
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1]) // JobExecution にも最後のステップエラーを追加
					return exception.NewBatchErrorf(s.name, "チャンク処理が最大リトライ回数 (%d) 失敗しました", stepRetryConfig.MaxAttempts) // エラーを返してステップを失敗させる
				}
			} else {
				// この試行がエラーなく完了した場合（Reader 終端に達したか、Context キャンセル以外）
				// 残っているアイテムがあれば最終チャンクとして処理し、ExecutionContext に追加
				// このブロックは、インナーループ内で itemCountInChunk >= chunkSize || eofReached の条件で既に処理されているため、
				// ここで再度処理する必要はない。トランザクションのコミットはここで行う。
				if err = tx.Commit(); err != nil { // err を再利用
					logger.Errorf("ステップ '%s': トランザクションのコミットに失敗しました: %v", s.name, err)
					stepExecution.MarkAsFailed(exception.NewBatchError(s.name, "トランザクションコミットエラー", err, false, false))
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
		return exception.NewBatchErrorf(s.name, "ステップ '%s' が最大リトライ回数を超えて失敗しました", s.name)
	} else if s.name == "saveWeatherDataStep" {
		logger.Infof("ステップ '%s' は書き込み専用ステップです。ExecutionContext からデータを取得し、書き込みます。", s.name)
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

		// Writer でデータを書き込み (トランザクション内で実行)
		tx, err := s.jobRepository.GetDB().BeginTx(ctx, nil) // err はここでシャドウイングされる
		if err != nil {
			logger.Errorf("ステップ '%s': トランザクションの開始に失敗しました: %v", s.name, err)
			stepExecution.MarkAsFailed(exception.NewBatchError(s.name, "トランザクション開始エラー", err, false, false))
			jobExecution.AddFailureException(err)
			return err
		}

		var chunkWriteError bool
		var currentWriteCount int
		// []*entity.WeatherDataToStore を []any に変換
		itemsForWriter := make([]any, len(dataToStore))
		for i, item := range dataToStore {
			itemsForWriter[i] = item
		}

		writeErr := s.writer.Write(ctx, itemsForWriter) // ★ チャンク全体を渡す
		if writeErr != nil {
			logger.Errorf("ステップ '%s' チャンク書き込みエラー: %v", s.name, writeErr)
			stepExecution.AddFailureException(writeErr)
			chunkWriteError = true // チャンク全体のエラーとする
		} else {
			currentWriteCount += len(dataToStore) // 書き込みカウントをインクリメント (成功したアイテム数)
		}
		stepExecution.WriteCount = currentWriteCount // StepExecution に反映

		if chunkWriteError {
			tx.Rollback() // ロールバック
			stepExecution.RollbackCount++
			return exception.NewBatchError(s.name, "Writer エラー", stepExecution.Failures[len(stepExecution.Failures)-1], false, false)
		}
		logger.Infof("ステップ '%s' Writer による書き込みが完了しました。", s.name)

		// Writer の ExecutionContext を取得し、StepExecution に保存 (Writer が状態を持つ場合)
		writerEC, err := s.writer.GetExecutionContext(ctx) // err はここでシャドウイングされる
		if err != nil {
			logger.Errorf("ステップ '%s': Writer の ExecutionContext 取得に失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(err)
			tx.Rollback() // エラーが発生した場合はロールバック
			return exception.NewBatchError(s.name, "Writer の ExecutionContext 取得エラー", err, false, false)
		}
		stepExecution.ExecutionContext.Put("writer_context", writerEC)

		// StepExecution を更新してチェックポイントを永続化
		if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil { // err を再利用
			logger.Errorf("ステップ '%s': StepExecution の更新 (チェックポイント) に失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(exception.NewBatchError(s.name, "StepExecution 更新エラー", err, false, false))
			tx.Rollback()
			return exception.NewBatchError(s.name, "StepExecution の更新 (チェックポイント) エラー", err, false, false)
		}
		stepExecution.CommitCount++ // コミットカウントをインクリメント

		// トランザクションをコミット
		if err = tx.Commit(); err != nil { // err を再利用
			logger.Errorf("ステップ '%s': トランザクションのコミットに失敗しました: %v", s.name, err)
			stepExecution.MarkAsFailed(exception.NewBatchError(s.name, "トランザクションコミットエラー", err, false, false))
			jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
			return err
		}

		// 成功したら Step を完了としてマーク
		stepExecution.MarkAsCompleted()

		return nil // 成功したら nil を返してメソッドを終了
	} else if s.name == "processDummyDataStep" {
		logger.Infof("ステップ '%s' はダミー処理ステップです。ダミーデータを読み込み、処理します。", s.name)

		// チャンク処理全体のリトライループ
		for retryAttempt := 0; retryAttempt < stepRetryConfig.MaxAttempts; retryAttempt++ {
			logger.Debugf("ステップ '%s' チャンク処理試行: %d/%d", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts)

			processedItemsChunk := make([]*entity.WeatherDataToStore, 0, chunkSize) // DummyProcessor は []*entity.WeatherDataToStore を返す
			itemCountInChunk := 0
			chunkAttemptError := false
			eofReached := false

			tx, err := s.jobRepository.GetDB().BeginTx(ctx, nil)
			if err != nil {
				logger.Errorf("ステップ '%s': ダミー処理トランザクションの開始に失敗しました: %v", s.name, err)
				stepExecution.MarkAsFailed(exception.NewBatchError(s.name, "ダミー処理トランザクション開始エラー", err, false, false))
				jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
				return err
			}

			for {
				select {
				case <-ctx.Done():
					logger.Warnf("Context がキャンセルされたため、ステップ '%s' のチャンク処理を中断します: %v", s.name, ctx.Err())
					stepExecution.MarkAsFailed(ctx.Err())
					jobExecution.AddFailureException(ctx.Err()) // JobExecution にもエラーを追加
					tx.Rollback()
					return ctx.Err()
				default:
				}

				// Reader の Read メソッドは any を返す
				readItem, readerErr := s.reader.Read(ctx)
				totalReadCount++
				stepExecution.ReadCount = totalReadCount // StepExecution に反映

				if readerErr != nil {
					if errors.Is(readerErr, io.EOF) {
						eofReached = true
						break
					}
					logger.Errorf("ステップ '%s' ダミー処理 Reader error: %v", s.name, readerErr)
					stepExecution.AddFailureException(readerErr) // StepExecution に記録
					chunkAttemptError = true
					break
				}

				// nil アイテムはスキップ
				if readItem == nil {
					continue
				}

				// Processor の Process メソッドは any を受け取り any を返す
				processedItem, processorErr := s.processor.Process(ctx, readItem)
				if processorErr != nil {
					logger.Errorf("ステップ '%s' ダミー処理 Processor error: %v", s.name, processorErr)
					stepExecution.AddFailureException(processorErr) // StepExecution に記録
					chunkAttemptError = true
					break
				}

				// Processor の出力が []*entity.WeatherDataToStore であることを期待
				processedItemSlice, ok := processedItem.([]*entity.WeatherDataToStore)
				if !ok {
					err := exception.NewBatchErrorf(s.name, "processor returned unexpected type: %T, expected []*entity.WeatherDataToStore", processedItem)
					logger.Errorf("%v", err)
					stepExecution.AddFailureException(err)
					chunkAttemptError = true
					break
				}

				if processedItemSlice != nil && len(processedItemSlice) > 0 {
					processedItemsChunk = append(processedItemsChunk, processedItemSlice...)
					itemCountInChunk = len(processedItemsChunk)
				} else { // フィルタリングされた場合
					stepExecution.FilterCount++
				}

				if itemCountInChunk >= chunkSize || eofReached {
					if len(processedItemsChunk) > 0 {
						// Writer でデータを書き込み (チャンク全体を一度に書き込む)
						var writeErr error
						var currentWriteCount int
						// []*entity.WeatherDataToStore を []any に変換
						itemsForWriter := make([]any, len(processedItemsChunk))
						for i, item := range processedItemsChunk {
							itemsForWriter[i] = item
						}

						writeErr = s.writer.Write(ctx, itemsForWriter) // ★ チャンク全体を渡す
						if writeErr != nil {
							logger.Errorf("ステップ '%s' チャンク書き込みエラー: %v", s.name, writeErr)
							stepExecution.AddFailureException(writeErr)
							chunkWriteError = true // チャンク全体のエラーとする
						} else {
							currentWriteCount += len(processedItemsChunk) // 書き込みカウントをインクリメント (成功したアイテム数)
						}
						totalWriteCount += currentWriteCount
						stepExecution.WriteCount = totalWriteCount // StepExecution に反映

						if chunkWriteError {
							break // チャンク処理を中断
						}
					}

					readerEC, err := s.reader.GetExecutionContext(ctx)
					if err != nil {
						logger.Errorf("ステップ '%s': ダミー処理 Reader の ExecutionContext 取得に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					writerEC, err := s.writer.GetExecutionContext(ctx)
					if err != nil {
						logger.Errorf("ステップ '%s': ダミー処理 Writer の ExecutionContext 取得に失敗しました: %v", s.name, err)
						chunkAttemptError = true
						break
					}
					stepExecution.ExecutionContext.Put("reader_context", readerEC)
					stepExecution.ExecutionContext.Put("writer_context", writerEC)

					if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
						logger.Errorf("ステップ '%s': StepExecution の更新 (チェックポイント) に失敗しました: %v", s.name, err)
						chunkAttemptError = true // StepExecution の永続化エラーもチャンクエラー
						break
					}
					stepExecution.CommitCount++
					chunkCount++ // チャンク処理成功

					processedItemsChunk = make([]*entity.WeatherDataToStore, 0, chunkSize)
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
				if retryAttempt < stepRetryConfig.MaxAttempts-1 {
					logger.Warnf("ステップ '%s' ダミー処理チャンク試行 %d/%d が失敗しました。リトライ間隔: %d秒", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts, stepRetryConfig.InitialInterval)
					time.Sleep(time.Duration(stepRetryConfig.InitialInterval) * time.Second)
					if readerEC, ok := stepExecution.ExecutionContext.Get("reader_context").(core.ExecutionContext); ok {
						s.reader.SetExecutionContext(ctx, readerEC)
					} else {
						s.reader.SetExecutionContext(ctx, core.NewExecutionContext())
					}
					s.writer.SetExecutionContext(ctx, core.NewExecutionContext()) // Writer は状態を持たないためクリア
				} else {
					logger.Errorf("ステップ '%s' ダミー処理チャンクが最大リトライ回数 (%d) 失敗しました。ステップを終了します。", s.name, stepRetryConfig.MaxAttempts)
					stepExecution.AddFailureException(exception.NewBatchErrorf(s.name, "ダミー処理チャンクが最大リトライ回数 (%d) 失敗しました", stepRetryConfig.MaxAttempts))
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
					return exception.NewBatchErrorf(s.name, "ダミー処理チャンクが最大リトライ回数 (%d) 失敗しました", stepRetryConfig.MaxAttempts)
				}
			} else {
				if err = tx.Commit(); err != nil {
					logger.Errorf("ステップ '%s': ダミー処理トランザクションのコミットに失敗しました: %v", s.name, err)
					stepExecution.MarkAsFailed(exception.NewBatchError(s.name, "ダミー処理トランザクションコミットエラー", err, false, false))
					jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
					return err
				}
				stepExecution.CommitCount++
				logger.Infof("ステップ '%s' ダミー処理チャンクが正常に完了しました。合計チャンク数: %d, 合計読み込みアイテム数: %d, 合計書き込みアイテム数: %d",
					s.name, chunkCount, totalReadCount, totalWriteCount)
				stepExecution.ReadCount = totalReadCount
				stepExecution.WriteCount = totalWriteCount
				stepExecution.MarkAsCompleted() // Mark as completed here for dummy step
				return nil
			}
		} // retry loop end
		return exception.NewBatchErrorf(s.name, "ダミー処理が最大リトライ回数を超えて失敗しました")
	}

	// 未知のステップ名の場合
	err := fmt.Errorf("ステップ '%s' の実行ロジックが定義されていません。", s.name)
	stepExecution.MarkAsFailed(err)
	jobExecution.AddFailureException(err)
	return err
}


// processSingleItem は Reader から1アイテム読み込み、Processor で処理します。
// 処理結果のスライス、EOFに達したかを示すフラグ、エラーを返します。
func (s *JSLAdaptedStep) processSingleItem(ctx context.Context, stepExecution *core.StepExecution, chunkAttempt int) ([]*entity.WeatherDataToStore, bool, error) {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	default:
	}

	// Reader から読み込み
	var readItem any // Reader[any] の出力は any
	var readErr error
	for itemRetryAttempt := 0; itemRetryAttempt < s.itemRetryConfig.MaxAttempts; itemRetryAttempt++ {
		readItem, readErr = s.reader.Read(ctx)
		if readErr == nil || errors.Is(readErr, io.EOF) {
			break // 成功またはEOF
		}

		// リトライ可能な例外かチェック
		if s.isRetryableException(readErr, s.itemRetryConfig.RetryableExceptions) && itemRetryAttempt < s.itemRetryConfig.MaxAttempts-1 {
			s.notifyRetryRead(ctx, readErr)
			logger.Warnf("ステップ '%s' アイテム読み込みエラーがリトライされます (試行 %d/%d): %v", s.name, itemRetryAttempt+1, s.itemRetryConfig.MaxAttempts, readErr)
			time.Sleep(time.Duration(s.stepRetryConfig.InitialInterval) * time.Second) // リトライ間隔
		} else {
			// リトライ不可または最大リトライ回数に達した場合
			s.notifyItemReadError(ctx, readErr)
			if s.isSkippableException(readErr, s.itemSkipConfig.SkippableExceptions) && stepExecution.SkipReadCount < s.itemSkipConfig.SkipLimit {
				s.notifySkipRead(ctx, readErr)
				stepExecution.SkipReadCount++
				logger.Warnf("ステップ '%s' アイテム読み込みエラーがスキップされました (スキップ数: %d/%d): %v", s.name, stepExecution.SkipReadCount, s.itemSkipConfig.SkipLimit, readErr)
				return nil, false, nil // アイテムをスキップ
			}
			// スキップもできない場合はエラーを返す
			logger.Errorf("ステップ '%s' Reader error: %v", s.name, readErr)
			stepExecution.AddFailureException(readErr)
			return nil, false, exception.NewBatchError(s.name, "reader error", readErr, false, false)
		}
	}

	if errors.Is(readErr, io.EOF) {
		logger.Debugf("ステップ '%s' Reader returned EOF.", s.name)
		return nil, true, nil
	}
	if readErr != nil { // リトライ/スキップ後もエラーが残っている場合
		logger.Errorf("ステップ '%s' Reader error after retries/skips: %v", s.name, readErr)
		stepExecution.AddFailureException(readErr)
		return nil, false, exception.NewBatchError(s.name, "reader error after retries/skips", readErr, false, false)
	}

	if readItem == nil {
		logger.Debugf("ステップ '%s' Reader returned nil item, skipping.")
		return nil, false, nil
	}

	// Processor で処理
	var processedItem any // Processor[any, any] の出力は any
	var processErr error
	for itemRetryAttempt := 0; itemRetryAttempt < s.itemRetryConfig.MaxAttempts; itemRetryAttempt++ {
		processedItem, processErr = s.processor.Process(ctx, readItem) // readItem は any
		if processErr == nil {
			break // 成功
		}

		// リトライ可能な例外かチェック
		if s.isRetryableException(processErr, s.itemRetryConfig.RetryableExceptions) && itemRetryAttempt < s.itemRetryConfig.MaxAttempts-1 {
			s.notifyRetryProcess(ctx, readItem, processErr)
			logger.Warnf("ステップ '%s' アイテム処理エラーがリトライされます (試行 %d/%d): %v", s.name, itemRetryAttempt+1, s.itemRetryConfig.MaxAttempts, processErr)
			time.Sleep(time.Duration(s.stepRetryConfig.InitialInterval) * time.Second) // リトライ間隔
		} else {
			// リトライ不可または最大リトライ回数に達した場合
			s.notifyItemProcessError(ctx, readItem, processErr)
			if s.isSkippableException(processErr, s.itemSkipConfig.SkippableExceptions) && stepExecution.SkipProcessCount < s.itemSkipConfig.SkipLimit {
				s.notifySkipProcess(ctx, readItem, processErr)
				stepExecution.SkipProcessCount++
				logger.Warnf("ステップ '%s' アイテム処理エラーがスキップされました (スキップ数: %d/%d): %v", s.name, stepExecution.SkipProcessCount, s.itemSkipConfig.SkipLimit, processErr)
				return nil, false, nil // アイテムをスキップ
			}
			// スキップもできない場合はエラーを返す
			logger.Errorf("ステップ '%s' Processor error: %v", s.name, processErr)
			stepExecution.AddFailureException(processErr)
			return nil, false, exception.NewBatchError(s.name, "processor error", processErr, false, false)
		}
	}

	if processErr != nil { // リトライ/スキップ後もエラーが残っている場合
		logger.Errorf("ステップ '%s' Processor error after retries/skips: %v", s.name, processErr)
		stepExecution.AddFailureException(processErr)
		return nil, false, exception.NewBatchError(s.name, "processor error after retries/skips", processErr, false, false)
	}

	if processedItem == nil {
		logger.Debugf("ステップ '%s' Processor returned nil item (filtered).")
		return nil, false, nil // フィルタリングされたアイテム
	}

	// 処理済みアイテムの型アサート
	// Processor は []*entity.WeatherDataToStore を返すことを期待
	processedItemsSlice, ok := processedItem.([]*entity.WeatherDataToStore)
	if !ok {
		err := exception.NewBatchErrorf(s.name, "processor returned unexpected type: %T, expected []*entity.WeatherDataToStore", processedItem)
		logger.Errorf("%v", err)
		stepExecution.AddFailureException(err)
		return nil, false, err
	}

	return processedItemsSlice, false, nil
}

// isRetryableException はエラーがリトライ可能かどうかを判定します。
// BatchError のフラグを優先し、次に具体的なエラー型をチェックします。
func (s *JSLAdaptedStep) isRetryableException(err error, retryableExceptions []string) bool {
	if err == nil {
		return false
	}
	// BatchError の IsRetryable フラグを優先
	if be, ok := err.(*exception.BatchError); ok {
		return be.IsRetryable()
	}

	// 設定されたリトライ可能な例外をチェック
	for _, re := range retryableExceptions {
		switch re {
		case "io.EOF":
			if errors.Is(err, io.EOF) {
				return true
			}
		case "net.OpError":
			var netOpErr *net.OpError
			if errors.As(err, &netOpErr) {
				return true
			}
		// 他のリトライ可能な標準エラーやカスタムエラーがあればここに追加
		// 例: case "context.DeadlineExceeded": if errors.Is(err, context.DeadlineExceeded) { return true }
		default:
			// 未知の例外文字列はログに警告を出すか無視する
			logger.Warnf("isRetryableException: 未知のリトライ可能例外タイプ '%s' が設定されています。errors.Is/As でのチェックはできません。", re)
		}
	}
	return false
}

// isSkippableException はエラーがスキップ可能かどうかを判定します。
// BatchError のフラグを優先し、次に具体的なエラー型をチェックします。
func (s *JSLAdaptedStep) isSkippableException(err error, skippableExceptions []string) bool {
	if err == nil {
		return false
	}
	// BatchError の IsSkippable フラグを優先
	if be, ok := err.(*exception.BatchError); ok {
		return be.IsSkippable()
	}

	// 設定されたスキップ可能な例外をチェック
	for _, se := range skippableExceptions {
		switch se {
		case "json.UnmarshalTypeError":
			var unmarshalTypeErr *json.UnmarshalTypeError
			if errors.As(err, &unmarshalTypeErr) {
				return true
			}
		// 他のスキップ可能な標準エラーやカスタムエラーがあればここに追加
		default:
			logger.Warnf("isSkippableException: 未知のスキップ可能例外タイプ '%s' が設定されています。errors.Is/As でのチェックはできません。", se)
		}
	}
	return false
}
