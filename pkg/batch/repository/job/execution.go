package job

import (
	"context"

	core "sample/pkg/batch/job/core"
)

// JobExecution は JobExecution の永続化と取得に関する操作を定義します。
type JobExecution interface { // ★ 変更: インターフェース名を JobExecution に
	// SaveJobExecution は新しい JobExecution を永久化します。
	SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error

	// UpdateJobExecution は既存の JobExecution の状態を更新します。
	UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error

	// FindJobExecutionByID は指定された ID の JobExecution を検索します。
	// 関連する StepExecution もロードされることを想定します。
	FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error)

	// FindLatestJobExecution は指定された JobInstance の最新の JobExecution を検索します。
	FindLatestJobExecution(ctx context.Context, jobInstanceID string) (*core.JobExecution, error)

	// FindJobExecutionsByJobInstance は指定された JobInstance に関連する全ての JobExecution を検索します。
	FindJobExecutionsByJobInstance(ctx context.Context, jobInstance *core.JobInstance) ([]*core.JobExecution, error)
}
