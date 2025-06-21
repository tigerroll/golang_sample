package writer

import "context"
import core "sample/src/main/go/batch/job/core" // core パッケージをインポート

// Writer はデータを書き込むステップのインターフェースです。
// 処理対象のアイテム型に合わせてジェネリクスを導入することも検討できます。
type Writer interface {
  Write(ctx context.Context, item interface{}) error // 書き込むデータを interface{} で扱う例 (単一アイテムを想定)
  Close(ctx context.Context) error // リソースを解放するためのメソッド
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ExecutionContext を取得
}
