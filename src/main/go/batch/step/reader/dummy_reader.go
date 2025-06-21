package reader

import (
  "context"
  "io" // io パッケージをインポート

  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
)

// DummyReader は常に io.EOF を返すダミーの Reader です。
// Reader インターフェース (step/reader/reader.go で定義) を実装します。
type DummyReader struct{
  // ExecutionContext を保持するためのフィールド
  executionContext core.ExecutionContext
}

// NewDummyReader は新しい DummyReader のインスタンスを作成します。
func NewDummyReader() *DummyReader {
  return &DummyReader{
    executionContext: core.NewExecutionContext(), // 初期化
  }
}

// Read は Reader インターフェースの実装です。
// 常に io.EOF を返してデータの終端を示します。
func (r *DummyReader) Read(ctx context.Context) (interface{}, error) {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return nil, ctx.Err()
  default:
  }
  // logger.Debugf("DummyReader.Read が呼び出されました。io.EOF を返します。") // Logger はこのパッケージにないためコメントアウトまたは削除
  return nil, io.EOF // 常に終端を示す
}

// Close は Reader インターフェースの実装です。
// DummyReader は閉じるリソースがないため、何もしません。
func (r *DummyReader) Close(ctx context.Context) error {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }
  // logger.Debugf("DummyReader.Close が呼び出されました。")
  return nil
}

// SetExecutionContext は Reader インターフェースの実装です。
// 渡された ExecutionContext を内部に設定します。
func (r *DummyReader) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }
  r.executionContext = ec
  // logger.Debugf("DummyReader.SetExecutionContext が呼び出されました。")
  return nil
}

// GetExecutionContext は Reader インターフェースの実装です。
// 現在の ExecutionContext を返します。
func (r *DummyReader) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return nil, ctx.Err()
  default:
  }
  // logger.Debugf("DummyReader.GetExecutionContext が呼び出されました。")
  return r.executionContext, nil
}

// DummyReader が Reader インターフェースを満たすことを確認
var _ Reader = (*DummyReader)(nil)
