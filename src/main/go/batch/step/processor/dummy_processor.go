// src/main/go/batch/step/processor/dummy_processor.go
package processor

import (
	"context"
	"fmt" // Add fmt for Sprintf
	"time"

	logger "sample/src/main/go/batch/util/logger"
)

// DummyProcessor は入力アイテムをそのまま返すダミーの Processor です。
// Processor[any, any] インターフェースを実装します。
type DummyProcessor struct{}

// NewDummyProcessor は新しい DummyProcessor のインスタンスを作成します。
func NewDummyProcessor() *DummyProcessor { return &DummyProcessor{} }

// Process は Processor インターフェースの実装です。
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

// DummyProcessor が Processor[any, any] インターフェースを満たすことを確認
var _ Processor[any, any] = (*DummyProcessor)(nil)
