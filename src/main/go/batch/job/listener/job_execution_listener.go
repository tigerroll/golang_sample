package listener

import "context"

// JobExecutionListener はジョブの実行ライフサイクルイベントを処理するためのインターフェースです。
type JobExecutionListener interface {
  // BeforeJob はジョブの実行が開始される直前に呼び出されます。
  BeforeJob(ctx context.Context, jobName string)
  // AfterJob はジョブの実行が完了した後に呼び出されます。成功・失敗に関わらず呼び出されます。
  AfterJob(ctx context.Context, jobName string, err error)
}
