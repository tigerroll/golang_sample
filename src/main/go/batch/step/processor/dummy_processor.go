package processor

import "context"

// DummyProcessor は入力アイテムをそのまま返すダミーの Processor です。
// Processor インターフェース (step/processor/processor.go で定義) を実装します。
type DummyProcessor struct{}

// NewDummyProcessor は新しい DummyProcessor のインスタンスを作成します。
func NewDummyProcessor() *DummyProcessor { return &DummyProcessor{} }

// Process は Processor インターフェースの実装です。
// 入力として受け取ったアイテムをそのまま返します。
func (p *DummyProcessor) Process(ctx context.Context, item interface{}) (interface{}, error) {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return nil, ctx.Err()
  default:
  }
  // logger.Debugf("DummyProcessor.Process が呼び出されました。入力アイテムをそのまま返します。") // Logger はこのパッケージにないためコメントアウトまたは削除
  return item, nil // 入力をそのまま返す
}

// DummyProcessor が Processor インターフェースを満たすことを確認
var _ Processor = (*DummyProcessor)(nil)

