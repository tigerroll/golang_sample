package itemprocessor // パッケージ名を 'itemprocessor' に変更

import "context"

// ItemProcessor はアイテムを処理するステップのインターフェースです。
// I は入力アイテムの型、O は出力アイテムの型です。
type ItemProcessor[I, O any] interface {
  Process(ctx context.Context, item I) (O, error) // 処理対象のアイテムと結果を I, O 型で扱う
}
