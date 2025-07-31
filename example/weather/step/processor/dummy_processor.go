package weatherprocessor // パッケージ名を 'weatherprocessor' に変更

import (
	"context"
	itemprocessor "sample/pkg/batch/step/processor" // Renamed import
	logger "sample/pkg/batch/util/logger"
	"fmt" // Add fmt for Sprintf
	"time"
)

// DummyProcessor は入力アイテムをそのまま返すダミーの Processor です。
// ItemProcessor[any, any] インターフェースを実装します。
type DummyProcessor struct{}

// NewDummyProcessor は新しい DummyProcessor のインスタンスを作成します。
func NewDummyProcessor() *DummyProcessor { return &DummyProcessor{} }

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

// DummyProcessor が ItemProcessor[any, any] インターフェースを満たすことを確認
var _ itemprocessor.ItemProcessor[any, any] = (*DummyProcessor)(nil) // ここは itemprocessor を参照
