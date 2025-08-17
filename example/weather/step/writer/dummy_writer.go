package weatherwriter // パッケージ名を 'weatherwriter' に変更

import (
	"context"
	"sample/pkg/batch/database" // database パッケージをインポート

	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	writer "sample/pkg/batch/step/writer" // ItemWriter インターフェースをインポート (エイリアスを writer に変更)
	logger "sample/pkg/batch/util/logger"
)

// DummyWriter は何も行わないダミーの Writer です。
// ItemWriter[any] インターフェースを実装します。
type DummyWriter struct { // 構造体名は変更しない
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewDummyWriter は新しい DummyWriter のインスタンスを作成します。
// ComponentBuilder のシグネチャに合わせ、cfg, repo, properties を受け取りますが、現時点では利用しません。
func NewDummyWriter(cfg *config.Config, repo job.JobRepository, properties map[string]string) (*DummyWriter, error) { // repo の型を job.JobRepository に変更
	_ = cfg        // 未使用の引数を無視
	_ = repo       // 未使用の引数を無視
	_ = properties
	return &DummyWriter{
		executionContext: core.NewExecutionContext(), // 初期化
	}, nil
}

// Open は ItemWriter インターフェースの実装です。
// DummyWriter はリソースを開く必要がないため、SetExecutionContext を呼び出すだけです。
func (w *DummyWriter) Open(ctx context.Context, ec core.ExecutionContext) error { // ★ 追加
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("DummyWriter.Open が呼び出されました。")
	return w.SetExecutionContext(ctx, ec)
}

// Write は ItemWriter インターフェースの実装です。
// アイテムのスライスを受け取り、何も行わずに nil を返します。
func (w *DummyWriter) Write(ctx context.Context, tx database.Tx, items []any) error { // ★ シグネチャを []any に変更し、tx を database.Tx に変更
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(items) > 0 {
		logger.Debugf("DummyWriter.Write が呼び出されました。何も行いません。アイテム数: %d", len(items))
	} else {
		logger.Debugf("DummyWriter.Write が呼び出されました。書き込むアイテムはありません。")
	}
	// tx は使用しないが、インターフェースの一貫性のために受け取る
	_ = tx
	return nil // 何も行わない
}

// Close は ItemWriter インターフェースの実装です。
// DummyWriter は閉じるリソースがないため、何もしません。
func (w *DummyWriter) Close(ctx context.Context) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("DummyWriter.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は ItemWriter インターフェースの実装です。
// 渡された ExecutionContext を内部に設定します。
func (w *DummyWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	w.executionContext = ec
	logger.Debugf("DummyWriter.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は ItemWriter インターフェースの実装です。
// 現在の ExecutionContext を返します。
func (w *DummyWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("DummyWriter.GetExecutionContext が呼び出されました。")
	return w.executionContext, nil
}

// DummyWriter が ItemWriter[any] インターフェースを満たすことを確認
var _ writer.ItemWriter[any] = (*DummyWriter)(nil) // writer.ItemWriter[any] に変更
