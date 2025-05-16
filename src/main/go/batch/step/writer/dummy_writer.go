package writer

import "context"

// DummyWriter は何も行わないダミーの Writer です。
// Writer インターフェース (step/writer/writer.go で定義) を実装します。
type DummyWriter struct{}

// NewDummyWriter は新しい DummyWriter のインスタンスを作成します。
func NewDummyWriter() *DummyWriter { return &DummyWriter{} }

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

// DummyWriter が Writer インターフェースを満たすことを確認
var _ Writer = (*DummyWriter)(nil)

