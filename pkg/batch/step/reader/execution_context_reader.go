package reader // パッケージ名を 'reader' に変更

import (
	"context"
	"fmt"
	"io" // io.EOF のためにインポート
	"reflect"

	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core"
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	logger "sample/pkg/batch/util/logger" // Keep logger
)

// ExecutionContextReader は JobExecution.ExecutionContext からデータを読み込む Reader です。
// これは、前のステップの出力を次のステップの入力として利用するシナリオで役立ちます。
type ExecutionContextReader struct {
	// 読み込むデータのキー
	dataKey string
	// 読み込むデータが格納されているスライス
	data []any
	// 現在読み込み済みのインデックス
	currentIndex int

	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewExecutionContextReader は新しい ExecutionContextReader のインスタンスを作成します。
// ComponentBuilder のシグネチャに合わせ、cfg, repo, properties を受け取ります。
// properties から dataKey を設定します。
func NewExecutionContextReader(cfg *config.Config, repo job.JobRepository, properties map[string]string) (*ExecutionContextReader, error) { // repo の型を job.JobRepository に変更
	_ = cfg // 未使用の引数を無視
	_ = repo // 未使用の引数を無視

	reader := &ExecutionContextReader{
		dataKey:          "processed_weather_data", // デフォルトのキー
		data:             make([]any, 0),
		currentIndex:     0,
		executionContext: core.NewExecutionContext(),
	}

	// properties から dataKey を設定
	if key, ok := properties["dataKey"]; ok && key != "" {
		reader.dataKey = key
	}

	return reader, nil
}

// Open は ItemReader インターフェースの実装です。
// ExecutionContext から状態を復元し、必要に応じてリソースを開きます。
func (r *ExecutionContextReader) Open(ctx context.Context, ec core.ExecutionContext) error { // ★ 追加
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("ExecutionContextReader.Open が呼び出されました。")
	// SetExecutionContext と同様のロジックで状態を復元
	return r.SetExecutionContext(ctx, ec)
}

// Read は ExecutionContext からアイテムを一つずつ読み込みます。
func (r *ExecutionContextReader) Read(ctx context.Context) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 初回読み込み時、またはリスタート後にデータがロードされていない場合にデータをロード
	if r.data == nil || len(r.data) == 0 {
		logger.Debugf("ExecutionContextReader: ExecutionContext からデータ '%s' をロードします。", r.dataKey)
		if rawData, ok := r.executionContext.Get(r.dataKey); ok {
			// rawData が []any 型であることを確認
			if loadedData, ok := rawData.([]any); ok {
				r.data = loadedData
				logger.Debugf("ExecutionContextReader: ExecutionContext からデータ '%s' をロードしました。アイテム数: %d", r.dataKey, len(r.data))
			} else {
				// 予期しない型の場合
				logger.Errorf("ExecutionContextReader: ExecutionContext のデータ '%s' の型が予期せぬものです: %T, 期待される型: []any", r.dataKey, rawData)
				return nil, fmt.Errorf("ExecutionContextReader: 予期しないデータ型: %T", rawData)
			}
		} else {
			logger.Debugf("ExecutionContextReader: ExecutionContext にデータ '%s' が見つかりませんでした。", r.dataKey)
			r.data = make([]any, 0) // データがない場合は空のスライス
		}
	}

	if r.currentIndex >= len(r.data) {
		logger.Debugf("ExecutionContextReader: 全てのアイテムを読み込み終えました。")
		r.data = nil // 次回 Read 時に再度ロードするためにリセット
		r.currentIndex = 0
		return nil, io.EOF // アイテムがないことを示す
	}

	item := r.data[r.currentIndex]
	r.currentIndex++

	logger.Debugf("ExecutionContextReader: アイテムを読み込みました (インデックス: %d, 型: %s)", r.currentIndex-1, reflect.TypeOf(item))
	return item, nil
}

// Close はリソースを解放します。
func (r *ExecutionContextReader) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("ExecutionContextReader.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は ExecutionContext を設定します。
// ここで JobExecution.ExecutionContext の内容を受け取り、内部の dataKey に対応するデータをロードします。
func (r *ExecutionContextReader) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	r.executionContext = ec // まず全体をコピー

	// currentIndex の復元
	// キー名を "reader_context_currentIndex" に統一
	if idx, ok := ec.GetInt("reader_context_currentIndex"); ok {
		r.currentIndex = idx
		logger.Debugf("ExecutionContextReader: ExecutionContext から reader_context_currentIndex を復元しました: %d", r.currentIndex)
	} else {
		r.currentIndex = 0 // 見つからない場合は初期値
		logger.Debugf("ExecutionContextReader: ExecutionContext に reader_context_currentIndex が見つかりませんでした。0 に初期化します。")
	}

	// data の復元 (ExecutionContextWriter が []any をそのまま保存していると仮定)
	// キー名を "reader_context_data" に統一
	if rawData, ok := ec.Get("reader_context_data"); ok {
		if loadedData, ok := rawData.([]any); ok {
			r.data = loadedData // 既存データを内部ECにセット
			logger.Debugf("ExecutionContextReader: ExecutionContext から reader_context_data を復元しました。アイテム数: %d", len(loadedData))
		} else {
			logger.Warnf("ExecutionContextReader: ExecutionContext の既存データ 'reader_context_data' の型が予期せぬものです: %T", rawData)
			// 型が合わない場合は、新しい空のスライスで初期化
			r.data = make([]any, 0)
		}
	} else {
		r.data = nil // 見つからない場合はnil (次回 Read 時に JobExecution.ExecutionContext からロード)
		logger.Debugf("ExecutionContextReader: ExecutionContext に reader_context_data が見つかりませんでした。次回 Read 時にロードします。")
	}

	return nil
}

// GetExecutionContext は ExecutionContext を取得します。
// 現在の内部状態を ExecutionContext に保存して返します。
func (r *ExecutionContextReader) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	// 現在の内部状態を r.executionContext に保存
	// キー名を "reader_context_currentIndex" と "reader_context_data" に統一
	r.executionContext.Put("reader_context_currentIndex", r.currentIndex)
	r.executionContext.Put("reader_context_data", r.data) // 現在のデータを保存

	return r.executionContext, nil
}

// ExecutionContextReader が ItemReader[any] インターフェースを満たすことを確認
var _ core.ItemReader[any] = (*ExecutionContextReader)(nil) // reader.ItemReader[any] から ItemReader[any] に変更
