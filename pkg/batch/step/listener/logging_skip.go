package listener

import (
	"context"
	core "sample/pkg/batch/job/core"
	"sample/pkg/batch/util/logger"
)

// LoggingSkipListener はアイテムスキップイベントをログ出力する SkipListener の実装です。
type LoggingSkipListener struct{}

// NewLoggingSkipListener は新しい LoggingSkipListener のインスタンスを作成します。
func NewLoggingSkipListener() *LoggingSkipListener {
	return &LoggingSkipListener{}
}

// OnSkipRead は読み込み中にスキップされたアイテムに対して呼び出されます。
func (l *LoggingSkipListener) OnSkipRead(ctx context.Context, err error) {
	logger.Warnf("アイテムの読み込み中にスキップされました: %v", err)
}

// OnSkipProcess は処理中にスキップされたアイテムに対して呼び出されます。
func (l *LoggingSkipListener) OnSkipProcess(ctx context.Context, item interface{}, err error) {
	logger.Warnf("アイテムの処理中にスキップされました (アイテム: %+v): %v", item, err)
}

// OnSkipWrite は書き込み中にスキップされたアイテムに対して呼び出されます。
func (l *LoggingSkipListener) OnSkipWrite(ctx context.Context, item interface{}, err error) {
	logger.Warnf("アイテムの書き込み中にスキップされました (アイテム: %+v): %v", item, err)
}

// LoggingSkipListener が core.SkipListener インターフェースを満たすことを確認
var _ core.SkipListener = (*LoggingSkipListener)(nil)
