// src/main/go/batch/step/reader/execution_context_reader.go
package reader

import (
	"context"
	"fmt"
	"io" // io.EOF のためにインポート
	"reflect"

	core "sample/src/main/go/batch/job/core"
	logger "sample/src/main/go/batch/util/logger"
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
// 読み込むデータのキーを指定します。
func NewExecutionContextReader() *ExecutionContextReader {
	return &ExecutionContextReader{
		dataKey:          "processed_weather_data", // デフォルトのキー。JSLで設定可能にするべき。
		data:             make([]any, 0),
		currentIndex:     0,
		executionContext: core.NewExecutionContext(),
	}
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
	if idx, ok := ec.GetInt("reader_context_currentIndex"); ok {
		r.currentIndex = idx
		logger.Debugf("ExecutionContextReader: ExecutionContext から currentIndex を復元しました: %d", r.currentIndex)
	} else {
		r.currentIndex = 0 // 見つからない場合は初期値
		logger.Debugf("ExecutionContextReader: ExecutionContext に currentIndex が見つかりませんでした。0 に初期化します。")
	}

	// data の復元 (ExecutionContextWriter が []any をそのまま保存していると仮定)
	if rawData, ok := ec.Get("reader_context_data"); ok {
		if loadedData, ok := rawData.([]any); ok {
			r.data = loadedData
			logger.Debugf("ExecutionContextReader: ExecutionContext から data を復元しました。アイテム数: %d", len(r.data))
		} else {
			logger.Warnf("ExecutionContextReader: ExecutionContext のデータ 'reader_context_data' の型が予期せぬものです: %T", rawData)
			r.data = make([]any, 0) // 型が合わない場合は空のスライス
		}
	} else {
		r.data = nil // 見つからない場合はnil (次回 Read 時に JobExecution.ExecutionContext からロード)
		logger.Debugf("ExecutionContextReader: ExecutionContext に data が見つかりませんでした。次回 Read 時にロードします。")
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
	// 新しい ExecutionContext を作成し、現在の状態を保存
	newEC := core.NewExecutionContext()
	newEC.Put("reader_context_currentIndex", r.currentIndex)
	newEC.Put("reader_context_data", r.data) // 現在のデータを保存

	r.executionContext = newEC // 内部の ExecutionContext も更新
	return newEC, nil
}

// ExecutionContextReader が Reader[any] インターフェースを満たすことを確認
var _ Reader[any] = (*ExecutionContextReader)(nil)
