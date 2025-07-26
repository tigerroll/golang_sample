// pkg/batch/step/writer/execution_context_writer.go
package writer

import (
	"context"
	"database/sql" // トランザクションを受け取るため
	"fmt"          // For fmt.Errorf
	"reflect"      // For type assertions on 'any' items

	core "github.com/tigerroll/go_sample/pkg/batch/job/core"
	logger "github.com/tigerroll/go_sample/pkg/batch/util/logger"     // Fixed: Completed import path
	exception "github.com/tigerroll/go_sample/pkg/batch/util/exception" // Added for potential custom error handling
)

// ExecutionContextWriter は JobExecution.ExecutionContext にデータを書き込む Writer です。
// これは、ステップの出力を JobExecution.ExecutionContext に保存するシナリオで役立ちます。
type ExecutionContextWriter struct {
	// データを書き込むキー
	dataKey string
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewExecutionContextWriter は新しい ExecutionContextWriter のインスタンスを作成します。
// データを書き込むキーを指定します。
func NewExecutionContextWriter() *ExecutionContextWriter {
	return &ExecutionContextWriter{
		dataKey:          "processed_weather_data", // デフォルトのキー。JSLで設定可能にするべき。
		executionContext: core.NewExecutionContext(),
	}
}

// Write はアイテムのチャンクを ExecutionContext に保存します。
// 引数を []any に変更し、トランザクションを受け取るように変更します。
func (w *ExecutionContextWriter) Write(ctx context.Context, tx *sql.Tx, items []any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(items) == 0 {
		logger.Debugf("ExecutionContextWriter: 書き込むアイテムがありません。")
		return nil
	}

	// ExecutionContext にデータを保存
	// 既存のデータを上書きするか、追加するかは要件による。
	// ここでは、新しいデータで上書きするシンプルな実装とする。
	// 複数のチャンクで呼び出される場合、このロジックは最後のチャンクのデータのみを保持することになる。
	// チャンクごとにデータを追加したい場合は、ExecutionContextから既存のリストを取得し、appendする必要がある。
	// 例:
	// if existingData, ok := w.executionContext.Get(w.dataKey); ok {
	//     if existingSlice, isSlice := existingData.([]any); isSlice {
	//         w.executionContext.Put(w.dataKey, append(existingSlice, items...))
	//     } else {
	//         // 既存のデータがスライスでない場合は上書き
	//         w.executionContext.Put(w.dataKey, items)
	//     }
	// } else {
	//     w.executionContext.Put(w.dataKey, items)
	// }

	// シンプルに上書きする実装
	w.executionContext.Put(w.dataKey, items)
	logger.Debugf("ExecutionContextWriter: ExecutionContext にアイテム %d 件を保存しました (キー: %s)。", len(items), w.dataKey)

	// トランザクションは直接使用しないが、インターフェースの一貫性のために受け取る
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
func (w *ExecutionContextWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	w.executionContext = ec
	logger.Debugf("ExecutionContextWriter.SetExecutionContext が呼び出されました。")

	// dataKey の復元 (もしExecutionContextに保存されているなら)
	if key, ok := ec.GetString("writer_data_key"); ok {
		w.dataKey = key
		logger.Debugf("ExecutionContextWriter: ExecutionContext から dataKey を復元しました: %s", w.dataKey)
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
	// 新しい ExecutionContext を作成し、現在の状態を保存
	newEC := core.NewExecutionContext()
	newEC.Put("writer_data_key", w.dataKey)
	// ここで、Writeメソッドで保存されたデータ自体をExecutionContextに含めるかどうかは、
	// そのデータが次のステップで必要かどうかによる。
	// 通常、ExecutionContextWriterはデータをExecutionContextに「書き込む」ので、
	// そのデータは既にExecutionContextに存在すると考えられる。
	// そのため、GetExecutionContextで再度データをコピーする必要はないかもしれない。
	// ただし、ExecutionContextReaderがこのWriterのExecutionContextからデータを読み込む場合、
	// ここでデータが正しく含まれている必要がある。
	// 現状のExecutionContextReaderはJobExecution.ExecutionContextから直接読み込むため、
	// ここでデータをコピーする必要はない。

	w.executionContext = newEC // 内部の ExecutionContext も更新
	logger.Debugf("ExecutionContextWriter.GetExecutionContext が呼び出されました。")
	return newEC, nil
}

// ExecutionContextWriter が Writer[any] インターフェースを満たすことを確認
var _ Writer[any] = (*ExecutionContextWriter)(nil)
