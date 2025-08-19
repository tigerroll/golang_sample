package reader // パッケージ名を 'weatherreader' から 'reader' に変更

import (
	"context"
	"io"           // io パッケージをインポート

	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	logger "sample/pkg/batch/util/logger" // logger パッケージをインポート
)

// DummyReader は常に io.EOF を返すダミーの Reader です。
// ItemReader[any] インターフェースを実装します。
type DummyReader struct { // 構造体名は変更しない
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewDummyReader は新しい DummyReader のインスタンスを作成します。
// ComponentBuilder のシグネチャに合わせ、cfg, repo, properties を受け取りますが、現時点では利用しません。
func NewDummyReader(cfg *config.Config, repo job.JobRepository, properties map[string]string) (*DummyReader, error) { // repo の型を job.JobRepository に変更
	_ = cfg        // 未使用の引数を無視
	_ = repo
	_ = properties
	return &DummyReader{
		executionContext: core.NewExecutionContext(), // 初期化
	}, nil
}

// Open は ItemReader インターフェースの実装です。
// DummyReader はリソースを開く必要がないため、SetExecutionContext を呼び出すだけです。
func (r *DummyReader) Open(ctx context.Context, ec core.ExecutionContext) error { // ★ 追加
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("DummyReader.Open が呼び出されました。")
	return r.SetExecutionContext(ctx, ec)
}

// Read は ItemReader インターフェースの実装です。
// 常に io.EOF を返してデータの終端を示します。
func (r *DummyReader) Read(ctx context.Context) (any, error) { // O は any
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("DummyReader.Read が呼び出されました。io.EOF を返します。")
	return nil, io.EOF // 常に終端を示す
}

// Close は ItemReader インターフェースの実装です。
// DummyReader は閉じるリソースがないため、何もしません。
func (r *DummyReader) Close(ctx context.Context) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("DummyReader.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は ItemReader インターフェースの実装です。
// 渡された ExecutionContext を内部に設定します。
func (r *DummyReader) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	r.executionContext = ec
	logger.Debugf("DummyReader.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は ItemReader インターフェースの実装です。
// 現在の ExecutionContext を返します。
func (r *DummyReader) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("DummyReader.GetExecutionContext が呼び出されました。")
	return r.executionContext, nil
}

// DummyReader が ItemReader[any] インターフェースを満たすことを確認
var _ core.ItemReader[any] = (*DummyReader)(nil) // reader.ItemReader[any] に変更
