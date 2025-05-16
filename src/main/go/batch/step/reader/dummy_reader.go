package reader

import (
  "context"
  "io" // io パッケージをインポート
)

// DummyReader は常に io.EOF を返すダミーの Reader です。
// Reader インターフェース (step/reader/reader.go で定義) を実装します。
type DummyReader struct{}

// NewDummyReader は新しい DummyReader のインスタンスを作成します。
func NewDummyReader() *DummyReader { return &DummyReader{} }

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

// DummyReader が Reader インターフェースを満たすことを確認
var _ Reader = (*DummyReader)(nil)

