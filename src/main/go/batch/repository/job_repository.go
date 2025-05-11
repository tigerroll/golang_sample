package repository

import (
  "context"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
)

// JobRepository はバッチ実行に関するメタデータを永続化・管理するためのインターフェースです。
// Spring Batch の JobRepository に相当します。
type JobRepository interface {
  // SaveJobExecution は新しい JobExecution を永続化します。
  // JobExecution の ID はこのメソッドの呼び出し前に設定されている必要があります (例: UUID)。
  SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error

  // UpdateJobExecution は既存の JobExecution の状態を更新します。
  UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error

  // FindJobExecutionByID は指定された ID の JobExecution を検索します。
  FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error)

  // FindLatestJobExecution は指定されたジョブ名の最新の JobExecution を検索します。
  FindLatestJobExecution(ctx context.Context, jobName string) (*core.JobExecution, error)

  // SaveStepExecution は新しい StepExecution を永続化します。
  SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error

  // UpdateStepExecution は既存の StepExecution の状態を更新します。
  UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error

  // FindStepExecutionByID は指定された ID の StepExecution を検索します。
  FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error)

  // FindStepExecutionsByJobExecutionID は指定された JobExecution ID に関連する全ての StepExecution を検索します。
  FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error)

  // TODO: 他に必要なメソッドを追加 (例: FindJobExecutions, GetJobNames, CountJobExecutions など)
  // TODO: ExecutionContext の永続化・復元に関するメソッドもここに追加

  // Close はリポジトリが使用するリソース (データベース接続など) を解放します。
  Close() error
}
