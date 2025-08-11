package processor // パッケージ名を 'processor' に変更

import "context"
import core "sample/pkg/batch/job/core" // core パッケージをインポート

// ItemProcessor はアイテムを処理するステップのインターフェースです。
// I は入力アイテムの型、O は出力アイテムの型です。
type ItemProcessor[I, O any] interface {
  Process(ctx context.Context, item I) (O, error) // 処理対象のアイテムと結果を I, O 型で扱う
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ★ 追加: ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ★ 追加: ExecutionContext を取得
}
