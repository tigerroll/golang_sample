// src/main/go/batch/step/tasklet.go
package step

import (
	"context"
	core "sample/src/main/go/batch/job/core"
)

// Tasklet は単一の操作を実行するステップのインターフェースです。
// JSR352のTaskletに相当します。
type Tasklet interface {
	// Execute はTaskletのビジネスロジックを実行します。
	// 処理が成功した場合は COMPLETED などの ExitStatus を返し、エラーが発生した場合はエラーを返します。
	Execute(ctx context.Context, stepExecution *core.StepExecution) (core.ExitStatus, error)
	// Close はリソースを解放するためのメソッドです。
	Close(ctx context.Context) error
	// SetExecutionContext は ExecutionContext を設定します。
	SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error
	// GetExecutionContext は ExecutionContext を取得します。
	GetExecutionContext(ctx context.Context) (core.ExecutionContext, error)
}

