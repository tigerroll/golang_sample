package processor

import "context"

// Processor はデータを処理するステップのインターフェースです。
// I は入力アイテムの型、O は出力アイテムの型です。
type Processor[I, O any] interface {
  Process(ctx context.Context, item I) (O, error) // 処理対象のアイテムと結果を I, O 型で扱う
}
