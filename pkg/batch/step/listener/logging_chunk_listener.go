package listener

import (
	"context"
	core "sample/pkg/batch/job/core"
	logger "sample/pkg/batch/util/logger"
)

// LoggingChunkListener はチャンク処理の開始と完了をログに出力する ChunkListener の実装です。
type LoggingChunkListener struct{}

// NewLoggingChunkListener は新しい LoggingChunkListener のインスタンスを作成します。
func NewLoggingChunkListener() *LoggingChunkListener {
	return &LoggingChunkListener{}
}

// BeforeChunk はチャンク処理が開始される直前に呼び出されます。
func (l *LoggingChunkListener) BeforeChunk(ctx context.Context, stepExecution *core.StepExecution) {
	logger.Infof("ChunkListener: ステップ '%s' のチャンク処理を開始します。", stepExecution.StepName)
}

// AfterChunk はチャンク処理が完了した後に呼び出されます。成功・失敗に関わらず呼び出されます。
func (l *LoggingChunkListener) AfterChunk(ctx context.Context, stepExecution *core.StepExecution) {
	logger.Infof("ChunkListener: ステップ '%s' のチャンク処理が完了しました。", stepExecution.StepName)
}
