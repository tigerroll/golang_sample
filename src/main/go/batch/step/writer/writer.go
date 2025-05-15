package writer

import "context"

// Writer はデータを書き込むステップのインターフェースです。
// 処理対象のアイテム型に合わせてジェネリクスを導入することも検討できます。
type Writer interface {
  Write(ctx context.Context, items interface{}) error // 書き込むデータを interface{} で扱う例 (チャンク処理を想定しスライスなどの interface{} を受け取る)
}