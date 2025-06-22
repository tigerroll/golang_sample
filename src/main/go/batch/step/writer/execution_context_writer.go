// src/main/go/batch/step/writer/execution_context_writer.go
package writer

import (
	"context"
	"database/sql" // トランザクションを受け取るため
	"fmt"

	core "sample/src/main/go/batch/job/core"
	logger "sample/src/main/go/batch/util/logger"
)

// ExecutionContextWriter は、受け取ったアイテムを JobExecution.ExecutionContext に書き込む Writer です。
// これは、あるステップの出力を次のステップの入力として利用するシナリオで役立ちます。
type ExecutionContextWriter struct {
	// 書き込むデータのキー
	dataKey string
	// ExecutionContext を保持するためのフィールド (ここでは JobExecution.ExecutionContext への参照は持たない)
	executionContext core.ExecutionContext
}

// NewExecutionContextWriter は新しい ExecutionContextWriter のインスタンスを作成します。
// 書き込むデータのキーを指定します。
func NewExecutionContextWriter() *ExecutionContextWriter {
	return &ExecutionContextWriter{
		dataKey:          "processed_weather_data", // デフォルトのキー。JSLで設定可能にするべき。
		executionContext: core.NewExecutionContext(),
	}
}

// Write はアイテムのチャンクを JobExecution.ExecutionContext に保存します。
// この Writer はデータベーストランザクションを直接使用しないため、tx は無視されます。
func (w *ExecutionContextWriter) Write(ctx context.Context, tx *sql.Tx, items []any) error { // tx を追加
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(items) == 0 {
		logger.Debugf("ExecutionContextWriter: 書き込むアイテムがありません。")
		return nil
	}

	// アイテムを JSON にシリアライズして ExecutionContext に保存
	// ExecutionContext は map[string]interface{} なので、直接スライスを Put できる
	// ただし、永続化時に JSON に変換されるため、JSON互換の型である必要がある
	// ここでは、items をそのまま保存する。
	// 永続化の際に serialization.MarshalExecutionContext がこれを処理する。
	// 既存のデータがあれば追加するロジックが必要
	currentData, ok := w.executionContext.Get(w.dataKey).([]any)
	if !ok || currentData == nil {
		currentData = make([]any, 0, len(items))
	}
	currentData = append(currentData, items...)
	w.executionContext.Put(w.dataKey, currentData)

	logger.Debugf("ExecutionContextWriter: ExecutionContext にアイテム %d 件を書き込みました。キー: '%s'", len(items), w.dataKey)
	// tx は使用しないが、インターフェースの一貫性のために受け取る
	_ = tx
	return nil
}

// Close はリソースを解放します。
func (w *ExecutionContextWriter) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("ExecutionContextWriter.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は ExecutionContext を設定します。
// ここで JobExecution.ExecutionContext の内容を受け取り、内部の dataKey に対応するデータをロードします。
func (w *ExecutionContextWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	w.executionContext = ec // まず全体をコピー

	// 既存のデータがあればロード
	if rawData, ok := ec.Get(w.dataKey); ok {
		if loadedData, ok := rawData.([]any); ok {
			w.executionContext.Put(w.dataKey, loadedData) // 既存データを内部ECにセット
			logger.Debugf("ExecutionContextWriter: ExecutionContext から既存データ '%s' をロードしました。アイテム数: %d", w.dataKey, len(loadedData))
		} else {
			logger.Warnf("ExecutionContextWriter: ExecutionContext の既存データ '%s' の型が予期せぬものです: %T", w.dataKey, rawData)
			// 型が合わない場合は、新しい空のスライスで初期化
			w.executionContext.Put(w.dataKey, []any{})
		}
	} else {
		w.executionContext.Put(w.dataKey, []any{}) // データがなければ空のスライスで初期化
		logger.Debugf("ExecutionContextWriter: ExecutionContext にデータ '%s' が見つかりませんでした。空のスライスで初期化します。", w.dataKey)
	}

	return nil
}

// GetExecutionContext は ExecutionContext を取得します。
// 現在の内部状態を ExecutionContext に保存して返します。
func (w *ExecutionContextWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	// ExecutionContextWriter は内部の executionContext を直接更新しているため、それを返す
	return w.executionContext, nil
}

// Writer インターフェースが実装されていることを確認
var _ Writer[any] = (*ExecutionContextWriter)(nil)
