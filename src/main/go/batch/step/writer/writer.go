package writer

import "context"
import core "sample/src/main/go/batch/job/core" // core パッケージをインポート

// Writer はデータを書き込むステップのインターフェースです。
// I は書き込むアイテムの型です。
type Writer[I any] interface {
  Write(ctx context.Context, item I) error // 書き込むデータを I 型で扱う
  Close(ctx context.Context) error // リソースを解放するためのメソッド
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ExecutionContext を取得
}
