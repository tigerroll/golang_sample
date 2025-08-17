package job

import (
	"context"

	core "sample/pkg/batch/job/core"
)

// StepExecution は StepExecution の永続化と取得に関する操作を定義します。
type StepExecution interface { // ★ 変更: インターフェース名を StepExecution に
	// SaveStepExecution は新しい StepExecution を永続化します。
	SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error

	// UpdateStepExecution は既存の StepExecution の状態を更新します。
	UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error

	// FindStepExecutionByID は指定された ID の StepExecution を検索します。
	FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error)

	// FindStepExecutionsByJobExecutionID は指定された JobExecution ID に関連する全ての StepExecution を検索します。
	FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error)
}
