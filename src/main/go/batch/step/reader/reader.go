package reader

import "context"
import core "sample/src/main/go/batch/job/core" // core パッケージをインポート

// Reader はデータを読み込むステップのインターフェースです。
// 処理対象のアイテム型に合わせてジェネリクスを導入することも検討できます。
type Reader interface {
  Read(ctx context.Context) (interface{}, error) // 読み込んだデータを interface{} で返す例
  Close(ctx context.Context) error // リソースを解放するためのメソッド
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ExecutionContext を取得
}
