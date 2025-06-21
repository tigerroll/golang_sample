package repository

import (
  "context"
  "database/sql" // sql パッケージをインポート

  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
)

// JobRepository はバッチ実行に関するメタデータを永続化・管理するためのインターフェースです。
// Spring Batch の JobRepository に相当します。
type JobRepository interface {
  // --- JobInstance 関連メソッド ---

  // SaveJobInstance は新しい JobInstance を永続化します。
  SaveJobInstance(ctx context.Context, jobInstance *core.JobInstance) error

  // FindJobInstanceByJobNameAndParameters は指定されたジョブ名とパラメータに一致する JobInstance を検索します。
  // JSR352 では JobParameters の一意性で JobInstance を識別します。
  FindJobInstanceByJobNameAndParameters(ctx context.Context, jobName string, params core.JobParameters) (*core.JobInstance, error)

  // FindJobInstanceByID は指定された ID の JobInstance を検索します。
  FindJobInstanceByID(ctx context.Context, instanceID string) (*core.JobInstance, error)

  // GetJobInstanceCount は指定されたジョブ名の JobInstance の数を返します。
  GetJobInstanceCount(ctx context.Context, jobName string) (int, error)

  // GetJobNames はリポジトリに存在する全てのジョブ名を返します。
  GetJobNames(ctx context.Context) ([]string, error)

  // --- JobExecution 関連メソッド ---

  // SaveJobExecution は新しい JobExecution を永続化します。
  // JobExecution の ID はこのメソッドの呼び出し前に設定されている必要があります (例: UUID)。
  // JobInstanceID も設定されている必要があります。
  SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error

  // UpdateJobExecution は既存の JobExecution の状態を更新します。
  UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error

  // FindJobExecutionByID は指定された ID の JobExecution を検索します。
  // 関連する StepExecution もロードされることを想定します。
  FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error)

  // FindLatestJobExecution は指定された JobInstance の最新の JobExecution を検索します。
  // JSR352 では JobInstance に紐づく JobExecution を検索することが一般的です。
  FindLatestJobExecution(ctx context.Context, jobInstanceID string) (*core.JobExecution, error)

  // FindJobExecutionsByJobInstance は指定された JobInstance に関連する全ての JobExecution を検索します。
  FindJobExecutionsByJobInstance(ctx context.Context, jobInstance *core.JobInstance) ([]*core.JobExecution, error)


  // --- StepExecution 関連メソッド ---

  // SaveStepExecution は新しい StepExecution を永続化します。
  SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error

  // UpdateStepExecution は既存の StepExecution の状態を更新します。
  UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error

  // FindStepExecutionByID は指定された ID の StepExecution を検索します。
  FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error)

  // FindStepExecutionsByJobExecutionID は指定された JobExecution ID に関連する全ての StepExecution を検索します。
  FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error)

  // TODO: CheckpointData の永続化・復元に関するメソッドもここに追加

  // GetDB はデータベース接続を返します。トランザクション管理のために使用されます。
  GetDB() *sql.DB

  // Close はリポジトリが使用するリソース (データベース接続など) を解放します。
  Close() error
}
