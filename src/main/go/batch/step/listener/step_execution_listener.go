package listener

import "context" // context パッケージをインポート

type StepExecutionListener interface {
  // BeforeStep メソッドに ctx context.Context を追加
  BeforeStep(ctx context.Context, stepName string, data interface{})
  // AfterStep メソッドに ctx context.Context を追加
  AfterStep(ctx context.Context, stepName string, data interface{}, err error)
}
