package reader // パッケージ名を 'reader' に変更

import "context"
import core "sample/pkg/batch/job/core" // core パッケージをインポート

// ItemReader はデータを読み込むステップのインターフェースです。
// O は読み込まれるアイテムの型です。
type ItemReader[O any] interface {
  Open(ctx context.Context, ec core.ExecutionContext) error // ★ 追加: リソースを開き、ExecutionContextから状態を復元
  Read(ctx context.Context) (O, error) // 読み込んだデータを O 型で返す
  Close(ctx context.Context) error // リソースを解放するためのメソッド
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ExecutionContext を取得
}
