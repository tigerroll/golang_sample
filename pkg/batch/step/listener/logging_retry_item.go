package listener

import (
	"context"
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	"sample/pkg/batch/util/logger"
)

// LoggingRetryItemListener はアイテムレベルのリトライイベントをログ出力する RetryItemListener の実装です。
type LoggingRetryItemListener struct{}

// NewLoggingRetryItemListener は新しい LoggingRetryItemListener のインスタンスを作成します。
func NewLoggingRetryItemListener() *LoggingRetryItemListener {
	return &LoggingRetryItemListener{}
}

// OnRetryRead は読み込みエラーがリトライされるときに呼び出されます。
func (l *LoggingRetryItemListener) OnRetryRead(ctx context.Context, err error) {
	logger.Warnf("アイテムの読み込みエラーがリトライされます: %v", err)
}

// OnRetryProcess は処理エラーがリトライされるときに呼び出されます。
func (l *LoggingRetryItemListener) OnRetryProcess(ctx context.Context, item interface{}, err error) {
	logger.Warnf("アイテムの処理エラーがリトライされます (アイテム: %+v): %v", item, err)
}

// OnRetryWrite は書き込みエラーがリトライされるときに呼び出されます。
func (l *LoggingRetryItemListener) OnRetryWrite(ctx context.Context, items []interface{}, err error) {
	logger.Warnf("アイテムの書き込みエラーがリトライされます (アイテム数: %d): %v", len(items), err)
}

// LoggingRetryItemListener が RetryItemListener インターフェースを満たすことを確認
var _ core.RetryItemListener = (*LoggingRetryItemListener)(nil)
