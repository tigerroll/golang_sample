package reader

import "context"
import core "github.com/tigerroll/go_sample/pkg/batch/job/core" // core パッケージをインポート

// Reader はデータを読み込むステップのインターフェースです。
// O は読み込まれるアイテムの型です。
type Reader[O any] interface {
  Read(ctx context.Context) (O, error) // 読み込んだデータを O 型で返す
  Close(ctx context.Context) error // リソースを解放するためのメソッド
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ExecutionContext を取得
}
