package job

import (
  "context"
)

// JobLauncher は Job を JobParameters とともに起動するためのインターフェースです。
// Spring Batchの JobLauncher に相当します。
type JobLauncher interface {
  // Run は指定された Job を JobParameters とともに起動します。
  // 起動された JobExecution インスタンスを返します。
  // ここで返されるエラーは、ジョブ自体の実行エラーではなく、起動処理自体のエラーです。
  Launch(ctx context.Context, job Job, params JobParameters) (*JobExecution, error)
}
