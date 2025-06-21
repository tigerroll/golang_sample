package listener

import (
	"context"
	"sample/src/main/go/batch/util/logger"
)

// SkipListener はアイテムスキップイベントを処理するためのインターフェースです。
type SkipListener interface {
	OnSkipRead(ctx context.Context, err error)
	OnSkipProcess(ctx context.Context, item interface{}, err error)
	OnSkipWrite(ctx context.Context, item interface{}, err error)
}

// LoggingSkipListener はスキップイベントをログ出力する SkipListener の実装です。
type LoggingSkipListener struct{}

// NewLoggingSkipListener は新しい LoggingSkipListener のインスタンスを作成します。
func NewLoggingSkipListener() *LoggingSkipListener {
	return &LoggingSkipListener{}
}

// OnSkipRead は読み込み中にアイテムがスキップされたときに呼び出されます。
func (l *LoggingSkipListener) OnSkipRead(ctx context.Context, err error) {
	logger.Warnf("アイテムの読み込み中にスキップされました: %v", err)
}

// OnSkipProcess は処理中にアイテムがスキップされたときに呼び出されます。
func (l *LoggingSkipListener) OnSkipProcess(ctx context.Context, item interface{}, err error) {
	logger.Warnf("アイテムの処理中にスキップされました (アイテム: %+v): %v", item, err)
}

// OnSkipWrite は書き込み中にアイテムがスキップされたときに呼び出されます。
func (l *LoggingSkipListener) OnSkipWrite(ctx context.Context, item interface{}, err error) {
	logger.Warnf("アイテムの書き込み中にスキップされました (アイテム: %+v): %v", item, err)
}

// LoggingSkipListener が SkipListener インターフェースを満たすことを確認
var _ SkipListener = (*LoggingSkipListener)(nil)
