package joblauncher

import (
  "context"

  core "sample/pkg/batch/job/core"
)

// JobLauncher は Job を JobParameters とともに起動するためのインターフェースです。
// Spring Batchの JobLauncher に相当します。
type JobLauncher interface {
  // Launch は指定された Job を JobParameters とともに起動します。
  // 起動された JobExecution インスタンスを返します。
  // ここで返されるエラーは、ジョブ自体の実行エラーではなく、起動処理自体のエラーです。
  // 型を core パッケージから参照するように変更
  Launch(ctx context.Context, jobName string, params core.JobParameters) (*core.JobExecution, error) // ★ 修正: jobName を直接受け取る
}
