package processor

import (
	"context"
	"time" // time パッケージをインポート

	entity "sample/src/main/go/batch/domain/entity" // entity パッケージをインポート
)

// DummyProcessor は入力アイテムをそのまま返すダミーの Processor です。
// Processor インターフェース (step/processor/processor.go で定義) を実装します。
type DummyProcessor struct{}

// NewDummyProcessor は新しい DummyProcessor のインスタンスを作成します。
func NewDummyProcessor() *DummyProcessor { return &DummyProcessor{} }

// Process は Processor インターフェースの実装です。
// 入力として受け取ったアイテムを []*entity.WeatherDataToStore 型に変換して返します。
// これは JSLAdaptedStep が期待する型に合わせるためのダミー実装です。
func (p *DummyProcessor) Process(ctx context.Context, item interface{}) (interface{}, error) {
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// logger.Debugf("DummyProcessor.Process が呼び出されました。入力アイテムをダミーの WeatherDataToStore に変換します。")

	// ダミーの WeatherDataToStore を作成
	// 実際の処理では item を変換するロジックが入る
	dummyData := &entity.WeatherDataToStore{
		Latitude:      35.0,
		Longitude:     135.0,
		Time:          time.Now(),
		WeatherCode:   1,
		Temperature2M: 25.5,
	}

	// JSLAdaptedStep が []*entity.WeatherDataToStore を期待するため、スライスで返す
	return []*entity.WeatherDataToStore{dummyData}, nil
}

// DummyProcessor が Processor インターフェースを満たすことを確認
var _ Processor = (*DummyProcessor)(nil)
