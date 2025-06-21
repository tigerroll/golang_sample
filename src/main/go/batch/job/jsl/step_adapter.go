package jsl

import (
	"context"
	"fmt"

	"sample/src/main/go/batch/job/core"
	"sample/src/main/go/batch/step/processor"
	"sample/src/main/go/batch/step/reader"
	"sample/src/main/go/batch/step/writer"
	exception "sample/src/main/go/batch/util/exception" // Alias for clarity
	logger "sample/src/main/go/batch/util/logger"       // Alias for clarity
)

// JSLAdaptedStep is a concrete implementation of core.Step that adapts JSL definitions.
// It performs chunk-oriented processing using the provided reader, processor, and writer.
type JSLAdaptedStep struct {
	id        string
	reader    reader.Reader
	processor processor.Processor
	writer    writer.Writer
	chunkSize int
}

// NewJSLAdaptedStep creates a new JSLAdaptedStep.
func NewJSLAdaptedStep(id string, r reader.Reader, p processor.Processor, w writer.Writer, chunkSize int) *JSLAdaptedStep {
	return &JSLAdaptedStep{
		id:        id,
		reader:    r,
		processor: p,
		writer:    w,
		chunkSize: chunkSize,
	}
}

// StepName implements core.Step.
func (s *JSLAdaptedStep) StepName() string {
	return s.id
}

// Execute implements core.Step. It orchestrates the read, process, and write operations in chunks.
func (s *JSLAdaptedStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	logger.Infof("ステップ '%s' を実行中...", s.id)
	stepExecution.MarkAsStarted()

	items := make([]interface{}, 0, s.chunkSize)
	readCount := 0
	writeCount := 0

	for {
		// Context の完了をチェック
		select {
		case <-ctx.Done():
			stepExecution.MarkAsFailed(ctx.Err())
			return exception.NewBatchError("jsl_step", fmt.Sprintf("ステップ '%s' がキャンセルされました", s.id), ctx.Err())
		default:
			// 処理を続行
		}

		// Read
		item, err := s.reader.Read(ctx)
		if err != nil {
			// リーダーがアイテムを使い果たしたことを示す特定のエラーをチェック
			// 例: exception.NewBatchError("reader", "No more items", nil) のようなエラーを返す場合
			if batchErr, ok := err.(*exception.BatchError); ok && batchErr.Message == "No more items" {
				logger.Debugf("リーダーがアイテムを使い果たしました。")
				break // 読み込むアイテムがもうない
			}
			stepExecution.AddFailureException(err)
			stepExecution.MarkAsFailed(err)
			return exception.NewBatchError("jsl_step", fmt.Sprintf("ステップ '%s' の読み込み中にエラーが発生しました", s.id), err)
		}
		if item == nil { // リーダーが nil を返してアイテムがないことを示す場合
			break
		}
		readCount++

		// Process (optional)
		processedItem := item
		if s.processor != nil {
			pItem, err := s.processor.Process(ctx, item)
			if err != nil {
				stepExecution.AddFailureException(err)
				stepExecution.MarkAsFailed(err)
				return exception.NewBatchError("jsl_step", fmt.Sprintf("ステップ '%s' の処理中にエラーが発生しました", s.id), err)
			}
			processedItem = pItem
		}

		if processedItem != nil {
			items = append(items, processedItem)
		}

		// Write in chunks
		if len(items) >= s.chunkSize {
			err = s.writer.Write(ctx, items)
			if err != nil {
				stepExecution.AddFailureException(err)
				stepExecution.MarkAsFailed(err)
				return exception.NewBatchError("jsl_step", fmt.Sprintf("ステップ '%s' の書き込み中にエラーが発生しました", s.id), err)
			}
			writeCount += len(items)
			items = make([]interface{}, 0, s.chunkSize) // チャンクをリセット
		}
	}

	// Write any remaining items in the last chunk
	if len(items) > 0 {
		err := s.writer.Write(ctx, items)
		if err != nil {
			stepExecution.AddFailureException(err)
			stepExecution.MarkAsFailed(err)
			return exception.NewBatchError("jsl_step", fmt.Sprintf("ステップ '%s' の最終書き込み中にエラーが発生しました", s.id), err)
		}
		writeCount += len(items)
	}

	stepExecution.ReadCount = readCount
	stepExecution.WriteCount = writeCount
	stepExecution.MarkAsCompleted()
	logger.Infof("ステップ '%s' が完了しました。読み込み数: %d, 書き込み数: %d", s.id, readCount, writeCount)
	return nil
}
