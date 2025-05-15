package listener

import "context"
import core "sample/src/main/go/batch/job/core" // core パッケージをインポート

// JobExecutionListener はジョブの実行ライフサイクルイベントを処理するためのインターフェースです。
// メソッドシグネチャを変更し、JobExecution を受け取るようにします。
type JobExecutionListener interface {
  // BeforeJob はジョブの実行が開始される直前に呼び出されます。
  BeforeJob(ctx context.Context, jobExecution *core.JobExecution)
  // AfterJob はジョブの実行が完了した後に呼び出されます。成功・失敗に関わらず呼び出されます。
  AfterJob(ctx context.Context, jobExecution *core.JobExecution)
}