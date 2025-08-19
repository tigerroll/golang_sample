package listener

import (
	"context"
	core "sample/pkg/batch/job/core"
	"sample/pkg/batch/util/logger"
)

// LoggingItemReadListener はアイテム読み込みエラーイベントをログ出力する ItemReadListener の実装です。
type LoggingItemReadListener struct{}

// NewLoggingItemReadListener は新しい LoggingItemReadListener のインスタンスを作成します。
func NewLoggingItemReadListener() *LoggingItemReadListener {
	return &LoggingItemReadListener{}
}

// OnReadError は読み込みエラー時に呼び出されます。
func (l *LoggingItemReadListener) OnReadError(ctx context.Context, err error) {
	logger.Errorf("アイテムの読み込み中にエラーが発生しました: %v", err)
}

// LoggingItemReadListener が core.ItemReadListener インターフェースを満たすことを確認
var _ core.ItemReadListener = (*LoggingItemReadListener)(nil)
