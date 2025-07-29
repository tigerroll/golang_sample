package joboperator

import (
  "context"

  core "sample/pkg/batch/job/core"
)

// JobOperator はバッチ実行の管理操作を行うためのインターフェースです。
// JSR352 の JobOperator に相当します。
type JobOperator interface {
  // Restart は指定された JobExecution を再開します。
  // 再開された JobExecution インスタンスを返します。
  // TODO: 実装 (フェーズ3)
  Restart(ctx context.Context, executionID string) (*core.JobExecution, error)

  // Stop は指定された JobExecution を停止します。
  // TODO: 実装
  Stop(ctx context.Context, executionID string) error

  // Abandon は指定された JobExecution を放棄します。
  // TODO: 実装
  Abandon(ctx context.Context, executionID string) error

  // GetJobExecution は指定された ID の JobExecution を取得します。
  GetJobExecution(ctx context.Context, executionID string) (*core.JobExecution, error)

  // GetJobExecutions は指定された JobInstance に関連する全ての JobExecution を取得します。
  GetJobExecutions(ctx context.Context, instanceID string) ([]*core.JobExecution, error)

  // GetLastJobExecution は指定された JobInstance の最新の JobExecution を取得します。
  GetLastJobExecution(ctx context.Context, instanceID string) (*core.JobExecution, error)

  // GetJobInstance は指定された ID の JobInstance を取得します。
  GetJobInstance(ctx context.Context, instanceID string) (*core.JobInstance, error)

  // GetJobInstances は指定されたジョブ名とパラメータに一致する JobInstance を検索します。
  GetJobInstances(ctx context.Context, jobName string, params core.JobParameters) ([]*core.JobInstance, error) // JSR352では複数返す場合がある

  // GetJobNames は登録されている全てのジョブ名を取得します。
  GetJobNames(ctx context.Context) ([]string, error)

  // GetParameters は指定された JobExecution の JobParameters を取得します。
  GetParameters(ctx context.Context, executionID string) (core.JobParameters, error)
}
