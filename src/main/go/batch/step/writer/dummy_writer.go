package writer

import (
	"context"

	core "sample/src/main/go/batch/job/core" // core パッケージをインポート
	"sample/src/main/go/batch/util/logger"   // logger パッケージをインポート
)

// DummyWriter は何も行わないダミーの Writer です。
// Writer[any] インターフェースを実装します。
type DummyWriter struct{
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewDummyWriter は新しい DummyWriter のインスタンスを作成します。
func NewDummyWriter() *DummyWriter {
	return &DummyWriter{
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Write は Writer インターフェースの実装です。
// アイテムのスライスを受け取り、何も行わずに nil を返します。
func (w *DummyWriter) Write(ctx context.Context, items []any) error { // ★ シグネチャを []any に変更
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(items) > 0 {
		logger.Debugf("DummyWriter.Write が呼び出されました。何も行いません。アイテム数: %d", len(items))
	} else {
		logger.Debugf("DummyWriter.Write が呼び出されました。書き込むアイテムはありません。")
	}
	return nil // 何も行わない
}

// Close は Writer インターフェースの実装です。
// DummyWriter は閉じるリソースがないため、何もしません。
func (w *DummyWriter) Close(ctx context.Context) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("DummyWriter.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は Writer インターフェースの実装です。
// 渡された ExecutionContext を内部に設定します。
func (w *DummyWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	w.executionContext = ec
	logger.Debugf("DummyWriter.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は Writer インターフェースの実装です。
// 現在の ExecutionContext を返します。
func (w *DummyWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("DummyWriter.GetExecutionContext が呼び出されました。")
	return w.executionContext, nil
}

// DummyWriter が Writer[any] インターフェースを満たすことを確認
var _ Writer[any] = (*DummyWriter)(nil)
