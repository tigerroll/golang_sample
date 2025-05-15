package reader

import "context"

// Reader はデータを読み込むステップのインターフェースです。
// 処理対象のアイテム型に合わせてジェネリクスを導入することも検討できます。
type Reader interface {
  Read(ctx context.Context) (interface{}, error) // 読み込んだデータを interface{} で返す例
}