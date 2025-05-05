package listener

import "context"

type StepExecutionListener interface {
  BeforeStep(ctx context.Context, stepName string, data interface{})
  AfterStep(ctx context.Context, stepName string, data interface{}, err error)
}
