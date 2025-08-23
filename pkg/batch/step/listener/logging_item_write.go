package listener

import (
	"context"
	core "sample/pkg/batch/job/core"
	"sample/pkg/batch/util/logger"
)

// LoggingItemWriteListener はアイテム書き込みイベントをログ出力する ItemWriteListener の実装です。
type LoggingItemWriteListener struct{}

// NewLoggingItemWriteListener は新しい LoggingItemWriteListener のインスタンスを作成します。
func NewLoggingItemWriteListener() *LoggingItemWriteListener {
	return &LoggingItemWriteListener{}
}

// OnWriteError は書き込みエラー時に呼び出されます。
func (l *LoggingItemWriteListener) OnWriteError(ctx context.Context, items []interface{}, err error) {
	logger.Errorf("アイテムの書き込み中にエラーが発生しました (アイテム数: %d): %v", len(items), err)
}

// OnSkipInWrite は書き込み中にスキップされたアイテムに対して呼び出されます。
// core.ItemWriteListener インターフェースに合わせてシグネチャを修正しました。
func (l *LoggingItemWriteListener) OnSkipInWrite(ctx context.Context, items []interface{}, err error) {
	logger.Warnf("アイテムの書き込み中にスキップされました (アイテム数: %d): %v", len(items), err)
}

// LoggingItemWriteListener が core.ItemWriteListener インターフェースを満たすことを確認
var _ core.ItemWriteListener = (*LoggingItemWriteListener)(nil)
