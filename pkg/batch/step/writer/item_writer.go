package itemwriter // パッケージ名を 'itemwriter' に変更

import "context"
import "database/sql" // sql パッケージをインポート
import core "sample/pkg/batch/job/core" // core パッケージをインポート

// ItemWriter はデータを書き込むステップのインターフェースです。
// I は書き込まれるアイテムの型です。
type ItemWriter[I any] interface {
  Write(ctx context.Context, tx *sql.Tx, items []I) error // 書き込むデータを I 型のスライスで扱い、トランザクションを受け取る
  Close(ctx context.Context) error // リソースを解放するためのメソッド
  SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error // ExecutionContext を設定
  GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) // ExecutionContext を取得
}
