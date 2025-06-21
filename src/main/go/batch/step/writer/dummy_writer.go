package writer

import (
  "context"

  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
)

// DummyWriter は何も行わないダミーの Writer です。
// Writer インターフェース (step/writer/writer.go で定義) を実装します。
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
// 何も行わずに nil を返します。
func (w *DummyWriter) Write(ctx context.Context, items interface{}) error {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }
  // logger.Debugf("DummyWriter.Write が呼び出されました。何も行いません。") // Logger はこのパッケージにないためコメントアウトまたは削除
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
  // logger.Debugf("DummyWriter.Close が呼び出されました。")
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
  // logger.Debugf("DummyWriter.SetExecutionContext が呼び出されました。")
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
  // logger.Debugf("DummyWriter.GetExecutionContext が呼び出されました。")
  return w.executionContext, nil
}

// DummyWriter が Writer インターフェースを満たすことを確認
var _ Writer = (*DummyWriter)(nil)
