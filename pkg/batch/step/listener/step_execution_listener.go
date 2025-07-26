package listener

import "context"
import core "github.com/tigerroll/go_sample/pkg/batch/job/core" // core パッケージをインポート

type StepExecutionListener interface {
  // BeforeStep メソッドシグネチャを変更し、StepExecution を追加
  BeforeStep(ctx context.Context, stepExecution *core.StepExecution)
  // AfterStep メソッドシグネチャを変更し、StepExecution を追加
  AfterStep(ctx context.Context, stepExecution *core.StepExecution)
}
