package listener

import (
	"context"
	core "sample/pkg/batch/job/core"
	"sample/pkg/batch/util/logger"
)

// LoggingItemProcessListener はアイテム処理イベントをログ出力する ItemProcessListener の実装です。
type LoggingItemProcessListener struct{}

// NewLoggingItemProcessListener は新しい LoggingItemProcessListener のインスタンスを作成します。
func NewLoggingItemProcessListener() *LoggingItemProcessListener {
	return &LoggingItemProcessListener{}
}

// OnProcessError は処理エラー時に呼び出されます。
func (l *LoggingItemProcessListener) OnProcessError(ctx context.Context, item interface{}, err error) {
	logger.Errorf("アイテムの処理中にエラーが発生しました (アイテム: %+v): %v", item, err)
}

// OnSkipInProcess は処理中にスキップされたアイテムに対して呼び出されます。
func (l *LoggingItemProcessListener) OnSkipInProcess(ctx context.Context, item interface{}, err error) {
	logger.Warnf("アイテムの処理中にスキップされました (アイテム: %+v): %v", item, err)
}

// LoggingItemProcessListener が core.ItemProcessListener インターフェースを満たすことを確認
var _ core.ItemProcessListener = (*LoggingItemProcessListener)(nil)
