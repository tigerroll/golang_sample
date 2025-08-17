package weatherprocessor // パッケージ名を 'weatherprocessor' に変更

import (
	"context"
	"fmt"          // Add fmt for Sprintf
	"time"

	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	processor "sample/pkg/batch/step/processor" // ItemProcessor インターフェースをインポート (エイリアスを processor に変更)
	logger "sample/pkg/batch/util/logger"
)

// DummyProcessor は入力アイテムをそのまま返すダミーの Processor です。
// ItemProcessor[any, any] インターフェースを実装します。
type DummyProcessor struct{
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext // ★ 追加
}

// NewDummyProcessor は新しい DummyProcessor のインスタンスを作成します。
// ComponentBuilder のシグネチャに合わせ、cfg, repo, properties を受け取りますが、現時点では利用しません。
func NewDummyProcessor(cfg *config.Config, repo job.JobRepository, properties map[string]string) (*DummyProcessor, error) { // repo の型を job.JobRepository に変更
	_ = cfg        // 未使用の引数を無視
	_ = repo       // 未使用の引数を無視
	_ = properties
	return &DummyProcessor{
		executionContext: core.NewExecutionContext(), // ★ 追加: 初期化
	}, nil
}

// Process は ItemProcessor インターフェースの実装です。
// 入力として受け取ったアイテムをそのまま返すか、汎用的なダミーデータを返します。
func (p *DummyProcessor) Process(ctx context.Context, item any) (any, error) { // I は any, O は any
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	logger.Debugf("DummyProcessor.Process が呼び出されました。入力アイテムをダミーのデータに変換します。")

	// 汎用的なダミーデータを返す
	// 例: 入力アイテムをそのまま返す、またはシンプルな文字列を返す
	return any(fmt.Sprintf("Processed dummy item: %v at %s", item, time.Now().Format(time.RFC3339))), nil
}

// SetExecutionContext は ItemProcessor インターフェースの実装です。
func (p *DummyProcessor) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error { // ★ 追加
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.executionContext = ec
	logger.Debugf("DummyProcessor.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は ItemProcessor インターフェースの実装です。
func (p *DummyProcessor) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) { // ★ 追加
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("DummyProcessor.GetExecutionContext が呼び出されました。")
	return p.executionContext, nil
}

// DummyProcessor が ItemProcessor[any, any] インターフェースを満たすことを確認
var _ processor.ItemProcessor[any, any] = (*DummyProcessor)(nil) // processor.ItemProcessor[any, any] に変更
