package step

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"time"
	"sync" // ★ 追加

	"sample/pkg/batch/config"
	"sample/pkg/batch/database"
	core "sample/pkg/batch/job/core"
	job "sample/pkg/batch/repository/job"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// JSLAdaptedStep は ItemReader, ItemProcessor, ItemWriter を使用するステップの実装です。
// core.Step インターフェースを実装します。
// Reader, Processor, Writer はジェネリックインターフェースですが、
// JSLAdaptedStep は core.Step (非ジェネリック) を実装するため、
// これらのフィールドは `any` 型引数を持つジェネリックインターフェースとして保持し、
// 内部で適切な型アサーションを行います。
type JSLAdaptedStep struct {
	name                      string
	reader                    core.ItemReader[any]
	processor                 core.ItemProcessor[any, any]
	writer                    core.ItemWriter[any]
	chunkSize                 int
	commitInterval            int
	stepRetryConfig           *config.RetryConfig
	itemRetryConfig           config.ItemRetryConfig
	itemSkipConfig            config.ItemSkipConfig
	stepListeners             []core.StepExecutionListener
	itemReadListeners         []core.ItemReadListener
	itemProcessListeners      []core.ItemProcessListener
	itemWriteListeners        []core.ItemWriteListener
	skipListeners             []core.SkipListener
	retryItemListeners        []core.RetryItemListener
	chunkListeners            []core.ChunkListener
	jobRepository             job.JobRepository
	executionContextPromotion *core.ExecutionContextPromotion
	txManager                 database.TransactionManager

	// ★ 追加: サーキットブレーカーの状態管理フィールド
	circuitBreakerState    core.CircuitBreakerState
	lastOpenTime           time.Time
	consecutiveFailures    int // 連続失敗回数 (サーキットブレーカー用)
	circuitBreakerMutex    sync.Mutex // 状態変更の同期用
}

// JSLAdaptedStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*JSLAdaptedStep)(nil)

// NewJSLAdaptedStep は新しい JSLAdaptedStep のインスタンスを作成します。
// ステップの依存関係と設定、および各種リスナーを受け取ります。
// reader, processor, writer は any 型引数を持つジェネリックインターフェースとして受け取ります。
func NewJSLAdaptedStep(
	name string,
	reader core.ItemReader[any],
	processor core.ItemProcessor[any, any],
	writer core.ItemWriter[any],
	chunkSize int,
	commitInterval int,
	stepRetryConfig *config.RetryConfig,
	itemRetryConfig config.ItemRetryConfig,
	itemSkipConfig config.ItemSkipConfig,
	jobRepository job.JobRepository,
	stepListeners []core.StepExecutionListener,
	itemReadListeners []core.ItemReadListener,
	itemProcessListeners []core.ItemProcessListener,
	itemWriteListeners []core.ItemWriteListener,
	skipListeners []core.SkipListener,
	retryItemListeners []core.RetryItemListener,
	chunkListeners []core.ChunkListener,
	executionContextPromotion *core.ExecutionContextPromotion,
	txManager database.TransactionManager,
) *JSLAdaptedStep {
	return &JSLAdaptedStep{
		name:                      name,
		reader:                    reader,
		processor:                 processor,
		writer:                    writer,
		chunkSize:                 chunkSize,
		commitInterval:            commitInterval,
		stepRetryConfig:           stepRetryConfig,
		itemRetryConfig:           itemRetryConfig,
		itemSkipConfig:            itemSkipConfig,
		stepListeners:             stepListeners,
		itemReadListeners:         itemReadListeners,
		itemProcessListeners:      itemProcessListeners,
		itemWriteListeners:        itemWriteListeners,
		skipListeners:             skipListeners,
		retryItemListeners:        retryItemListeners,
		chunkListeners:            chunkListeners,
		jobRepository:             jobRepository,
		executionContextPromotion: executionContextPromotion,
		txManager:                 txManager,
		// ★ 追加: サーキットブレーカーの状態初期化
		circuitBreakerState:    core.CBStateClosed, // 初期状態はクローズ
		lastOpenTime:           time.Time{},        // ゼロ値
		consecutiveFailures:    0,                  // 初期値
	}
}

// StepName はステップ名を返します。core.Step インターフェースの実装です。
func (s *JSLAdaptedStep) StepName() string {
	return s.name
}

// ID はステップのIDを返します。core.FlowElement インターフェースの実装です。
func (s *JSLAdaptedStep) ID() string {
	return s.name
}

// RegisterListener はこのステップに StepExecutionListener を登録します。
// NewJSLAdaptedStep でリスナーを受け取るように変更したため、このメソッドは不要になる可能性がありますが、
// 実行時に動的にリスナーを追加するユースケースのために残しておくこともできます。
func (s *JSLAdaptedStep) RegisterListener(l core.StepExecutionListener) {
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

// notifyBeforeChunk は登録されている ChunkListener の BeforeChunk メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyBeforeChunk(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.chunkListeners {
		l.BeforeChunk(ctx, stepExecution)
	}
}

// notifyAfterChunk は登録されている ChunkListener の AfterChunk メソッドを呼び出します。
func (s *JSLAdaptedStep) notifyAfterChunk(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.chunkListeners {
		l.AfterChunk(ctx, stepExecution)
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
// 変更: items []interface{} を受け取るように変更
func (s *JSLAdaptedStep) notifySkipInWrite(ctx context.Context, items []interface{}, err error) {
	for _, l := range s.itemWriteListeners {
		l.OnSkipInWrite(ctx, items, err)
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

// promoteExecutionContext は StepExecutionContext の指定されたキーを JobExecutionContext にプロモートします。
func (s *JSLAdaptedStep) promoteExecutionContext(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) {
	if s.executionContextPromotion == nil || len(s.executionContextPromotion.Keys) == 0 {
		logger.Debugf("ステップ '%s': ExecutionContextPromotion の設定がないか、プロモートするキーが指定されていません。", s.name)
		return
	}

	logger.Debugf("ステップ '%s': ExecutionContext のプロモーションを開始します。", s.name)
	for _, key := range s.executionContextPromotion.Keys {
		if val, ok := stepExecution.ExecutionContext.GetNested(key); ok {
			jobLevelKey := key
			if mappedKey, found := s.executionContextPromotion.JobLevelKeys[key]; found {
				jobLevelKey = mappedKey
			}
			jobExecution.ExecutionContext.PutNested(jobLevelKey, val)
			logger.Debugf("ステップ '%s': StepExecutionContext のキー '%s' を JobExecutionContext のキー '%s' にプロモートしました。", s.name, key, jobLevelKey)
		} else {
			logger.Warnf("ステップ '%s': StepExecutionContext にプロモート対象のキー '%s' が見つかりませんでした。", s.name, key)
		}
	}
	logger.Debugf("ステップ '%s': ExecutionContext のプロモーションが完了しました。", s.name)
}

// Execute はチャンク処理を実行します。core.Step インターフェースの実装です。
// StepExecution のライフサイクル管理（開始/終了マーク、リスナー通知）をここで行います。
func (s *JSLAdaptedStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	logger.Infof("ステップ '%s' (Execution ID: %s) を始めるよ。", s.name, stepExecution.ID)

	// StepExecution の開始時刻を設定し、状態をマーク
	// TransitionTo を使用して状態遷移を厳密化
	if err := stepExecution.TransitionTo(core.BatchStatusStarted); err != nil {
		logger.Errorf("ステップ '%s': StepExecution の状態を STARTED に更新できませんでした: %v", s.name, err)
		stepExecution.AddFailureException(err)
		stepExecution.MarkAsFailed(err) // 強制的に失敗状態に
		return exception.NewBatchError(s.name, "StepExecution の状態更新エラー", err, false, false)
	}
	stepExecution.StartTime = time.Now()

	// StepExecution の ExecutionContext から Reader/Writer の状態を復元 (リスタート時)
	// ★ 追加: サーキットブレーカーの状態を復元
	if cbStateStr, ok := stepExecution.ExecutionContext.GetString("circuitBreaker.state"); ok {
		s.circuitBreakerState = core.CircuitBreakerState(cbStateStr)
	}
	if lastOpenTimeStr, ok := stepExecution.ExecutionContext.GetString("circuitBreaker.lastOpenTime"); ok {
		if t, err := time.Parse(time.RFC3339, lastOpenTimeStr); err == nil {
			s.lastOpenTime = t
		}
	}
	if consecutiveFailures, ok := stepExecution.ExecutionContext.GetInt("circuitBreaker.consecutiveFailures"); ok {
		s.consecutiveFailures = consecutiveFailures
	}
	logger.Debugf("ステップ '%s': サーキットブレーカーの状態を復元したよ: State=%s, LastOpenTime=%v, ConsecutiveFailures=%d",
		s.name, s.circuitBreakerState, s.lastOpenTime, s.consecutiveFailures)

	if len(stepExecution.ExecutionContext) > 0 {
		logger.Debugf("ステップ '%s': ExecutionContext から Reader/Writer の状態を復元するよ。", s.name)
		if readerECVal, ok := stepExecution.ExecutionContext.Get("reader_context"); ok {
			if readerEC, isEC := readerECVal.(core.ExecutionContext); isEC {
				if err := s.reader.SetExecutionContext(ctx, readerEC); err != nil {
					logger.Errorf("ステップ '%s': Reader の ExecutionContext 復元に失敗したよ: %v", s.name, err)
					stepExecution.AddFailureException(err)
					return exception.NewBatchError(s.name, "Reader の ExecutionContext 復元エラー", err, false, false)
				}
			} else {
				logger.Warnf("ステップ '%s': Reader の ExecutionContext が予期しない型だよ: %T", s.name, readerECVal)
			}
		}
		if writerECVal, ok := stepExecution.ExecutionContext.Get("writer_context"); ok {
			if writerEC, isEC := writerECVal.(core.ExecutionContext); isEC {
				if err := s.writer.SetExecutionContext(ctx, writerEC); err != nil {
					logger.Errorf("ステップ '%s': Writer の ExecutionContext 復元に失敗したよ: %v", s.name, err)
					stepExecution.AddFailureException(err)
					return exception.NewBatchError(s.name, "Writer の ExecutionContext 復元エラー", err, false, false)
				}
			} else {
				logger.Warnf("ステップ '%s': Writer の ExecutionContext が予期しない型だよ: %T", s.name, writerECVal)
			}
		}
	}

	// ステップ実行前処理の通知 (ExecutionContext 復元後)
	s.notifyBeforeStep(ctx, stepExecution)


	// ステップ実行後処理 (defer で必ず実行)
	defer func() {
		// ステップの終了時刻を設定
		stepExecution.EndTime = time.Now()

		// Reader, Writer の ExecutionContext をステップの ExecutionContext に保存
		if ec, err := s.reader.GetExecutionContext(ctx); err == nil {
			// Reader の ExecutionContext 全体を "reader_context" キーで保存
			stepExecution.ExecutionContext.Put("reader_context", ec)
		} else {
			logger.Errorf("ステップ '%s': Reader の ExecutionContext 取得に失敗したよ: %v", s.name, err)
		}
		if ec, err := s.writer.GetExecutionContext(ctx); err == nil {
			// Writer の ExecutionContext 全体を "writer_context" キーで保存
			stepExecution.ExecutionContext.Put("writer_context", ec)
		} else {
			// 修正: ログメッセージをより正確に
			logger.Errorf("ステップ '%s': Writer の ExecutionContext 取得に失敗したよ: %v", s.name, err)
		}

		// ★ 追加: サーキットブレーカーの状態を StepExecution.ExecutionContext に保存
		stepExecution.ExecutionContext.Put("circuitBreaker.state", string(s.circuitBreakerState))
		if !s.lastOpenTime.IsZero() {
			stepExecution.ExecutionContext.Put("circuitBreaker.lastOpenTime", s.lastOpenTime.Format(time.RFC3339))
		} else {
			stepExecution.ExecutionContext.Put("circuitBreaker.lastOpenTime", "") // ゼロ値の場合は空文字列
		}
		stepExecution.ExecutionContext.Put("circuitBreaker.consecutiveFailures", s.consecutiveFailures)
		logger.Debugf("ステップ '%s': サーキットブレーカーの状態を保存したよ: State=%s, LastOpenTime=%v, ConsecutiveFailures=%d",
			s.name, s.circuitBreakerState, s.lastOpenTime, s.consecutiveFailures)

		// ステップ実行後処理の通知
		s.notifyAfterStep(ctx, stepExecution)

		// ExecutionContext のプロモーション
		s.promoteExecutionContext(ctx, jobExecution, stepExecution)
	}()

	logger.Infof("ステップ '%s' は汎用チャンク処理ステップとして実行されるよ。", s.name)
	return s.executeDefaultChunkProcessing(ctx, jobExecution, stepExecution)
}

// executeDefaultChunkProcessing は一般的なチャンク指向ステップの実行ロジックをカプセル化します。
// Reader, Processor, Writer を使用し、チャンクレベルのリトライとアイテムレベルのスキップ/リトライを処理します。
func (s *JSLAdaptedStep) executeDefaultChunkProcessing(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	s.circuitBreakerMutex.Lock() // 状態変更の同期
	defer s.circuitBreakerMutex.Unlock()

	stepRetryConfig := s.stepRetryConfig

	// ★ 修正: Circuit Breaker Logic
	switch s.circuitBreakerState {
	case core.CBStateOpen:
		resetInterval := time.Duration(stepRetryConfig.CircuitBreakerResetInterval) * time.Second
		if time.Since(s.lastOpenTime) < resetInterval {
			logger.Warnf("ステップ '%s': サーキットブレーカーがオープン状態だよ。リセット間隔 (%v) が経過していないため、処理をスキップするよ。", s.name, resetInterval)
			return exception.NewBatchErrorf(s.name, "サーキットブレーカーがオープン状態です。リセット間隔が経過していません。")
		}
		logger.Infof("ステップ '%s': サーキットブレーカーがハーフオープン状態に遷移するよ。リセット間隔 (%v) が経過したよ。", s.name, resetInterval)
		s.circuitBreakerState = core.CBStateHalfOpen
		// Fallthrough to Half-Open logic, allowing one attempt
	case core.CBStateHalfOpen:
		logger.Infof("ステップ '%s': サーキットブレーカーがハーフオープン状態だよ。1回の試行を許可するよ。", s.name)
		// Allow one attempt, then decide to close or re-open
	case core.CBStateClosed:
		// Proceed as normal
	}
	// --- End Circuit Breaker State Check ---

	// CommitInterval が ItemCount と異なる場合、ログを出す (現状は ItemCount と同じ意味で扱われるため)
	if s.commitInterval > 0 && s.commitInterval != s.chunkSize {
		logger.Warnf("ステップ '%s': Chunk.CommitInterval (%d) が Chunk.ItemCount (%d) と異なりますが、現在の実装では ItemCount が優先されます。", s.name, s.commitInterval, s.chunkSize)
	}

	var overallChunkProcessingErr error
	var totalReadCount int = 0
	var totalWriteCount int = 0
	var chunkCount int = 0

	// チャンク処理全体のリトライループ (これはステップレベルのリトライ)
	for retryAttempt := 0; retryAttempt < stepRetryConfig.MaxAttempts; retryAttempt++ {
		logger.Debugf("ステップ '%s' チャンク処理試行: %d/%d (サーキットブレーカー状態: %s)", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts, s.circuitBreakerState)

		// ★ 修正: チャンク処理の内部ロジックをヘルパーメソッドに切り出し
		chunkAttemptErr := s.processOneChunkAttempt(ctx, jobExecution, stepExecution, retryAttempt, &totalReadCount, &totalWriteCount, &chunkCount)

		if chunkAttemptErr != nil {
			// この試行が失敗した
			overallChunkProcessingErr = chunkAttemptErr // 記録するエラー
			s.consecutiveFailures++                     // ステップの連続失敗回数をインクリメント

			// Circuit Breaker: Half-Open 状態での失敗
			if s.circuitBreakerState == core.CBStateHalfOpen {
				logger.Warnf("ステップ '%s': サーキットブレーカーがハーフオープン状態で失敗したよ。オープン状態に戻すよ。", s.name)
				s.circuitBreakerState = core.CBStateOpen
				s.lastOpenTime = time.Now()
				// Half-Open で失敗したら、ステップレベルのリトライはせず、すぐに Circuit Breaker Open エラーを返す
				return exception.NewBatchErrorf(s.name, "サーキットブレーカーがハーフオープン状態で失敗し、オープン状態に戻りました。")
			}

			// Circuit Breaker: Closed 状態での連続失敗
			if s.consecutiveFailures >= stepRetryConfig.CircuitBreakerThreshold {
				logger.Errorf("ステップ '%s' サーキットブレーカーがオープンしたよ。連続失敗回数 (%d) が閾値 (%d) を超えたよ。", s.name, s.consecutiveFailures, stepRetryConfig.CircuitBreakerThreshold)
				s.circuitBreakerState = core.CBStateOpen
				s.lastOpenTime = time.Now()
				stepExecution.AddFailureException(exception.NewBatchErrorf(s.name, "回路遮断器オープン: 連続失敗回数 (%d) が閾値 (%d) を超えたよ", s.consecutiveFailures, stepRetryConfig.CircuitBreakerThreshold))
				jobExecution.AddFailureException(stepExecution.Failures[len(jobExecution.Failures)-1])
				return exception.NewBatchErrorf(s.name, "回路遮断器オープンによりステップ '%s' が失敗したよ", s.name)
			}

			// ステップレベルのリトライのための指数バックオフ
			if retryAttempt < stepRetryConfig.MaxAttempts-1 {
				delay := time.Duration(float64(stepRetryConfig.InitialInterval) * math.Pow(stepRetryConfig.Factor, float64(retryAttempt))) * time.Millisecond
				if delay > time.Duration(stepRetryConfig.MaxInterval)*time.Millisecond {
					delay = time.Duration(stepRetryConfig.MaxInterval) * time.Millisecond
				}

				logger.Warnf("ステップ '%s' チャンク処理試行 %d/%d が失敗したよ。リトライ間隔: %v", s.name, retryAttempt+1, stepRetryConfig.MaxAttempts, delay)
				time.Sleep(delay)

				// リトライ前に Reader の ExecutionContext をリセット
				if readerEC, ok := stepExecution.ExecutionContext.Get("reader_context"); ok {
					if readerECVal, isEC := readerEC.(core.ExecutionContext); isEC {
						s.reader.SetExecutionContext(ctx, readerECVal)
					} else {
						logger.Warnf("ステップ '%s': リトライ時の Reader の ExecutionContext が予期しない型だよ: %T", s.name, readerEC)
						s.reader.SetExecutionContext(ctx, core.NewExecutionContext())
					}
				} else {
					s.reader.SetExecutionContext(ctx, core.NewExecutionContext())
				}
				s.writer.SetExecutionContext(ctx, core.NewExecutionContext())
			} else {
				// 最大リトライ回数に達した場合
				logger.Errorf("ステップ '%s' チャンク処理が最大リトライ回数 (%d) 失敗したよ。ステップを終了するよ。", s.name, stepRetryConfig.MaxAttempts)
				stepExecution.AddFailureException(exception.NewBatchErrorf(s.name, "チャンク処理が最大リトライ回数 (%d) 失敗したよ", stepRetryConfig.MaxAttempts))
				jobExecution.AddFailureException(stepExecution.Failures[len(jobExecution.Failures)-1])
				return exception.NewBatchErrorf(s.name, "チャンク処理が最大リトライ回数 (%d) 失敗したよ", s.name)
			}
		} else {
			// この試行がエラーなく完了した場合
			s.consecutiveFailures = 0 // 成功したので連続失敗回数をリセット

			// Circuit Breaker: Half-Open 状態での成功
			if s.circuitBreakerState == core.CBStateHalfOpen {
				logger.Infof("ステップ '%s': サーキットブレーカーがハーフオープン状態で成功したよ。クローズ状態に戻すよ。", s.name)
				s.circuitBreakerState = core.CBStateClosed
				s.lastOpenTime = time.Time{} // リセット
			}

			logger.Infof("ステップ '%s' チャンク処理ステップが正常に完了したよ。合計チャンク数: %d, 合計読み込みアイテム数: %d, 合計書き込みアイテム数: %d, 合計フィルタリング数: %d",
				s.name, chunkCount, totalReadCount, totalWriteCount, stepExecution.FilterCount)
			stepExecution.ReadCount = totalReadCount
			stepExecution.WriteCount = totalWriteCount
			stepExecution.MarkAsCompleted()
			return nil // 全体として成功
		}
	} // リトライループ終了

	// ここに到達するのは、リトライ回数を使い果たして失敗した場合のみ
	return overallChunkProcessingErr // 最後に記録されたエラーを返す
}

// processOneChunkAttempt は単一のチャンク処理試行を実行します。
// 成功した場合は nil を返し、失敗した場合はエラーを返します。
func (s *JSLAdaptedStep) processOneChunkAttempt(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution, retryAttempt int, totalReadCount, totalWriteCount, chunkCount *int) error {
	chunkSize := s.chunkSize
	processedItemsChunk := make([]any, 0, chunkSize)
	itemCountInChunk := 0
	chunkAttemptError := false // このチャンク試行でエラーが発生したかを示すフラグ
	eofReached := false

	// チャンク開始前処理の通知
	s.notifyBeforeChunk(ctx, stepExecution)

	// トランザクションを開始
	tx, err := s.txManager.Begin(ctx, nil)
	if err != nil {
		logger.Errorf("ステップ '%s': トランザクションの開始に失敗したよ: %v", s.name, err)
		stepExecution.AddFailureException(exception.NewBatchError(s.name, "トランザクション開始エラー", err, true, false))
		jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
		return err // トランザクション開始失敗は即座にエラーを返す
	}

	// defer のトランザクション処理
	defer func() {
		if chunkAttemptError {
			if rbErr := s.txManager.Rollback(tx); rbErr != nil {
				logger.Errorf("ステップ '%s': トランザクションのロールバック中にエラーが発生したよ: %v", s.name, rbErr)
			} else {
				logger.Debugf("ステップ '%s': トランザクションをロールバックしました。", s.name)
			}
			stepExecution.RollbackCount++
		} else {
			if cmErr := s.txManager.Commit(tx); cmErr != nil {
				logger.Errorf("ステップ '%s': トランザクションのコミットに失敗したよ: %v", s.name, cmErr)
				stepExecution.AddFailureException(exception.NewBatchError(s.name, "トランザクションコミットエラー", cmErr, true, false))
				jobExecution.AddFailureException(stepExecution.Failures[len(stepExecution.Failures)-1])
				chunkAttemptError = true // コミット失敗もチャンクエラーとして扱う
			} else {
				logger.Debugf("ステップ '%s': トランザクションをコミットしました。", s.name)
			}
			stepExecution.CommitCount++
		}
		// チャンク終了後処理の通知 (エラーの有無に関わらず)
		s.notifyAfterChunk(ctx, stepExecution)
	}()

	// アイテムの読み込み、処理、チャンクへの追加を行うインナーループ
	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context がキャンセルされたため、ステップ '%s' のチャンク処理を中断するよ: %v", s.name, ctx.Err())
			stepExecution.MarkAsStopped()
			jobExecution.AddFailureException(ctx.Err())
			chunkAttemptError = true
			return ctx.Err()
		default:
		}

		// 単一アイテムの読み込みと処理
		processedItem, currentEOFReached, filtered, itemErr := s.processSingleItem(ctx, stepExecution, retryAttempt)
		*totalReadCount++ // 読み込みカウントをインクリメント
		stepExecution.ReadCount = *totalReadCount // StepExecution に反映

		if itemErr != nil {
			// Reader または Processor でエラーが発生した場合 (スキップ不可/リトライ不可)
			logger.Errorf("ステップ '%s' アイテム処理でエラーが発生したよ (試行 %d/%d): %v", s.name, retryAttempt+1, s.stepRetryConfig.MaxAttempts, itemErr)
			chunkAttemptError = true // この試行はエラー
			return itemErr           // アイテム処理エラーはチャンク試行のエラーとして返す
		}

		if currentEOFReached {
			eofReached = true
			// EOF に達したが、まだチャンクにアイテムが残っている可能性があるので、
			// このイテレーションでチャンク処理を試みるためにループを継続
			// 次の if itemCountInChunk >= chunkSize || eofReached で処理される
		}

		// processSingleItem が nil アイテムを返した場合 (スキップまたはフィルタリングされた場合)
		if processedItem == nil {
			if filtered { // filtered が true の場合のみフィルタリングカウントを増やす
				stepExecution.FilterCount++
			}
			if !eofReached { // EOF でない限り、次のアイテムへ
				continue
			}
			// EOF で processedItem が nil の場合、チャンク処理を試みるためにループを継続
		}

		// 処理済みアイテムをチャンクに追加
		if processedItem != nil {
			// processedItem がスライスの場合、その要素を個々に追加
			val := reflect.ValueOf(processedItem)
			if val.Kind() == reflect.Slice {
				for i := 0; i < val.Len(); i++ {
					processedItemsChunk = append(processedItemsChunk, val.Index(i).Interface())
				}
			} else {
				// それ以外の場合（単一アイテム）、そのまま追加
				processedItemsChunk = append(processedItemsChunk, processedItem)
			}
			itemCountInChunk = len(processedItemsChunk)
		}

		// チャンクが満たされたら、または EOF に達したら処理
		if itemCountInChunk >= chunkSize || eofReached {
			if len(processedItemsChunk) > 0 { // チャンクにアイテムがある場合のみ書き込み
				var writeErr error
				// Writer でデータを書き込み (チャンク全体を一度に書き込む)
				// Writer のリトライ/スキップロジックをここに実装
				for itemRetryAttempt := 0; itemRetryAttempt < s.itemRetryConfig.MaxAttempts; itemRetryAttempt++ {
					// Context キャンセルチェック
					select {
					case <-ctx.Done():
						logger.Warnf("Context がキャンセルされたため、ステップ '%s' のアイテム書き込みを中断するよ: %v", s.name, ctx.Err())
						stepExecution.MarkAsStopped()
						jobExecution.AddFailureException(ctx.Err())
						chunkAttemptError = true
						return ctx.Err()
					default:
					}

					// Writer.Write にトランザクションを渡すように変更
					writeErr = s.writer.Write(ctx, tx, processedItemsChunk)
					if writeErr == nil {
						break // 成功
					}

					// リトライ可能な例外かチェック
					if s.isRetryableException(writeErr, s.itemRetryConfig.RetryableExceptions) && itemRetryAttempt < s.itemRetryConfig.MaxAttempts-1 {
						s.notifyRetryWrite(ctx, processedItemsChunk, writeErr)
						logger.Warnf("ステップ '%s' アイテム書き込みエラーがリトライされるよ (試行 %d/%d): %v", s.name, itemRetryAttempt+1, s.itemRetryConfig.MaxAttempts, writeErr)
						time.Sleep(time.Duration(s.itemRetryConfig.InitialInterval) * time.Millisecond) // リトライ間隔 (ミリ秒)
					} else {
						// リトライ不可または最大リトライ回数に達した場合
						s.notifyItemWriteError(ctx, processedItemsChunk, writeErr)
						if s.isSkippableException(writeErr, s.itemSkipConfig.SkippableExceptions) && stepExecution.SkipWriteCount < s.itemSkipConfig.SkipLimit {
							s.notifySkipInWrite(ctx, processedItemsChunk, writeErr)
							stepExecution.SkipWriteCount += len(processedItemsChunk) // Skip count by chunk size
							logger.Warnf("ステップ '%s' アイテム書き込みエラーがスキップされたよ (スキップ数: %d/%d): %v", s.name, stepExecution.SkipWriteCount, s.itemSkipConfig.SkipLimit, writeErr)
							writeErr = nil // エラーをクリアして続行
						} else {
							// スキップもできない場合はエラー
							logger.Errorf("ステップ '%s' Writer error: %v", s.name, writeErr)
							stepExecution.AddFailureException(writeErr)
							chunkAttemptError = true // チャンク全体のエラーとする
							return writeErr          // 書き込みエラーはチャンク試行のエラーとして返す
						}
					}
				}

				if writeErr != nil { // リトライ/スキップ後もエラーが残っている場合
					logger.Errorf("ステップ '%s' Writer error after retries/skips: %v", s.name, writeErr)
					stepExecution.AddFailureException(writeErr)
					chunkAttemptError = true
					return writeErr
				}

				*totalWriteCount += len(processedItemsChunk) // 書き込みカウントをインクリメント (成功したアイテム数)
				stepExecution.WriteCount = *totalWriteCount // StepExecution に反映
			}

			if chunkAttemptError {
				return errors.New("chunk processing failed due to item error or write error") // エラーを返す
			}

			// Reader/Writer の ExecutionContext を取得し、StepExecution に保存
			readerEC, err := s.reader.GetExecutionContext(ctx)
			if err != nil {
				logger.Errorf("ステップ '%s': Reader の ExecutionContext 取得に失敗したよ: %v", s.name, err)
				chunkAttemptError = true
				return err
			}
			writerEC, err := s.writer.GetExecutionContext(ctx)
			if err != nil {
				logger.Errorf("ステップ '%s': Writer の ExecutionContext 取得に失敗したよ: %v", s.name, err)
				chunkAttemptError = true
				return err
			}
			stepExecution.ExecutionContext.Put("reader_context", readerEC)
			stepExecution.ExecutionContext.Put("writer_context", writerEC)

			// StepExecution を更新してチェックポイントを永続化
			if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
				logger.Errorf("ステップ '%s': StepExecution の更新 (チェックポイント) に失敗したよ: %v", s.name, err)
				chunkAttemptError = true // StepExecution の永続化エラーもチャンクエラー
				return err
			}

			*chunkCount++ // チャンク処理成功
			// チャンクをリセット
			processedItemsChunk = make([]any, 0, chunkSize)
			itemCountInChunk = 0

			// EOF に達した場合はループを抜ける
			if eofReached {
				logger.Debugf("ステップ '%s' Reader 終端到達。最終チャンク処理完了。", s.name)
				break
			}
		}

		// Reader の終端に達した場合
		if eofReached {
			logger.Debugf("ステップ '%s' Reader からデータの終端に達したよ。", s.name)
			break // インナーループを抜ける
		}
	} // インナーループ終了

	if chunkAttemptError {
		return errors.New("chunk processing failed") // チャンク試行が失敗したことを示すエラー
	}
	return nil // チャンク試行成功
}

// processSingleItem は Reader から1アイテム読み込み、Processor で処理します。
// 処理結果のアイテム、EOFに達したかを示すフラグ、フィルタリングされたかを示すフラグ、エラーを返します。
func (s *JSLAdaptedStep) processSingleItem(ctx context.Context, stepExecution *core.StepExecution, chunkAttempt int) (processedItem any, eofReached bool, filtered bool, err error) {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, false, false, ctx.Err()
	default:
	}

	// Reader から読み込み
	var readItem any // Reader[any] の出力は any
	var readErr error
	for itemRetryAttempt := 0; itemRetryAttempt < s.itemRetryConfig.MaxAttempts; itemRetryAttempt++ {
		// Context キャンセルチェック
		select {
		case <-ctx.Done():
			return nil, false, false, ctx.Err()
		default:
		}

		readItem, readErr = s.reader.Read(ctx)
		if readErr == nil || errors.Is(readErr, io.EOF) {
			break // 成功またはEOF
		}

		// リトライ可能な例外かチェック
		if s.isRetryableException(readErr, s.itemRetryConfig.RetryableExceptions) && itemRetryAttempt < s.itemRetryConfig.MaxAttempts-1 {
			s.notifyRetryRead(ctx, readErr)
			logger.Warnf("ステップ '%s' アイテム読み込みエラーがリトライされるよ (試行 %d/%d): %v", s.name, itemRetryAttempt+1, s.itemRetryConfig.MaxAttempts, readErr)
			time.Sleep(time.Duration(s.itemRetryConfig.InitialInterval) * time.Millisecond) // リトライ間隔 (ミリ秒)
		} else {
			// リトライ不可または最大リトライ回数に達した場合
			s.notifyItemReadError(ctx, readErr) // err を渡す
			if s.isSkippableException(readErr, s.itemSkipConfig.SkippableExceptions) && stepExecution.SkipReadCount < s.itemSkipConfig.SkipLimit {
				s.notifySkipRead(ctx, readErr)
				stepExecution.SkipReadCount++
				logger.Warnf("ステップ '%s' アイテム読み込みエラーがスキップされたよ (スキップ数: %d/%d): %v", s.name, stepExecution.SkipReadCount, s.itemSkipConfig.SkipLimit, readErr)
				return nil, false, true, nil // アイテムをスキップ (フィルタリングとして扱う)
			}
			// スキップもできない場合はエラーを返す
			logger.Errorf("ステップ '%s' Reader error: %v", s.name, readErr)
			stepExecution.AddFailureException(readErr)
			return nil, false, false, exception.NewBatchError(s.name, "reader error", readErr, false, false)
		}
	}

	if errors.Is(readErr, io.EOF) {
		logger.Debugf("ステップ '%s' Reader returned EOF.", s.name)
		return nil, true, false, nil // EOF, not filtered
	}
	if readErr != nil { // リトライ/スキップ後もエラーが残っている場合
		logger.Errorf("ステップ '%s' Reader error after retries/skips: %v", s.name, readErr)
		stepExecution.AddFailureException(readErr)
		return nil, false, false, exception.NewBatchError(s.name, "reader error after retries/skips", readErr, false, false)
	}

	if readItem == nil {
		logger.Debugf("ステップ '%s' Reader returned nil item, skipping.")
		return nil, false, true, nil // Skipped, considered filtered for counts
	}

	// Processor で処理
	var processedItemResult any // Processor[any, any] の出力は any
	var processErr error

	// プロセッサーが設定されていない場合は、読み込んだアイテムをそのまま返す
	if s.processor == nil {
		processedItemResult = readItem
	} else {
		for itemRetryAttempt := 0; itemRetryAttempt < s.itemRetryConfig.MaxAttempts; itemRetryAttempt++ {
			// Context キャンセルチェック
			select {
			case <-ctx.Done():
				return nil, false, false, ctx.Err()
			default:
			}

			processedItemResult, processErr = s.processor.Process(ctx, readItem) // readItem は any
			if processErr == nil {
				break // 成功
			}

			// リトライ可能な例外かチェック
			if s.isRetryableException(processErr, s.itemRetryConfig.RetryableExceptions) && itemRetryAttempt < s.itemRetryConfig.MaxAttempts-1 {
				s.notifyRetryProcess(ctx, readItem, processErr)
				logger.Warnf("ステップ '%s' アイテム処理エラーがリトライされるよ (試行 %d/%d): %v", s.name, itemRetryAttempt+1, s.itemRetryConfig.MaxAttempts, processErr)
				time.Sleep(time.Duration(s.itemRetryConfig.InitialInterval) * time.Millisecond) // リトライ間隔 (ミリ秒)
			} else {
				// リトライ不可または最大リトライ回数に達した場合
				s.notifyItemProcessError(ctx, readItem, processErr)
				if s.isSkippableException(processErr, s.itemSkipConfig.SkippableExceptions) && stepExecution.SkipProcessCount < s.itemSkipConfig.SkipLimit {
					s.notifySkipProcess(ctx, readItem, processErr)
					stepExecution.SkipProcessCount++
					logger.Warnf("ステップ '%s' アイテム処理エラーがスキップされたよ (スキップ数: %d/%d): %v", s.name, stepExecution.SkipProcessCount, s.itemSkipConfig.SkipLimit, processErr)
					return nil, false, true, nil // アイテムをスキップ (フィルタリングとして扱う)
				}
				// スキップもできない場合はエラーを返す
				logger.Errorf("ステップ '%s' Processor error: %v", s.name, processErr)
				stepExecution.AddFailureException(processErr)
				return nil, false, false, exception.NewBatchError(s.name, "processor error", processErr, false, false)
			}
		}
	}

	if processErr != nil { // リトライ/スキップ後もエラーが残っている場合
		logger.Errorf("ステップ '%s' Processor error after retries/skips: %v", s.name, processErr)
		stepExecution.AddFailureException(processErr)
		return nil, false, false, exception.NewBatchError(s.name, "processor error after retries/skips", processErr, false, false)
	}

	if processedItemResult == nil {
		logger.Debugf("ステップ '%s' Processor returned nil item (filtered).")
		return nil, false, true, nil // フィルタリングされたアイテム
	}

	return processedItemResult, false, false, nil
}

// isRetryableException はエラーがリトライ可能かどうかを判定します。
// BatchError のフラグを優先し、次に具体的なエラー型をチェックします。
func (s *JSLAdaptedStep) isRetryableException(err error, retryableExceptions []string) bool {
	if err == nil {
		return false
	}
	// Context キャンセルはリトライ可能ではない
	if errors.Is(err, context.Canceled) {
		return false
	}
	// BatchError の IsRetryable フラグを優先
	if be, ok := err.(*exception.BatchError); ok {
		return be.IsRetryable()
	}

	// 設定されたリトライ可能な例外をチェック
	for _, re := range retryableExceptions {
		// 完全一致または部分一致で型名をチェック
		if exception.IsErrorOfType(err, re) {
			return true
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
	// Context キャンセルはスキップ可能ではない
	if errors.Is(err, context.Canceled) {
		return false
	}
	// BatchError の IsSkippable フラグを優先
	if be, ok := err.(*exception.BatchError); ok {
		return be.IsSkippable()
	}

	// 設定されたスキップ可能な例外をチェック
	for _, se := range skippableExceptions {
		// 完全一致または部分一致で型名をチェック
		if exception.IsErrorOfType(err, se) {
			return true
		}
	}
	return false
}
