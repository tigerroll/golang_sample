package processor

import "context"

// Processor はデータを処理するステップのインターフェースです。
// 入力アイテム型と出力アイテム型に合わせてジェネリクスを導入することも検討できます。
type Processor interface {
  Process(ctx context.Context, item interface{}) (interface{}, error) // 処理対象のアイテムと結果を interface{} で扱う例
}
