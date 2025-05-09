package job

import (
  "context"
  //"fmt" // 必要に応じてコメントを外す

  core "sample/src/main/go/batch/job/core"
  logger "sample/src/main/go/batch/util/logger"
)

// JobLauncher は Job を JobParameters とともに起動するためのインターフェースです。
// Spring Batchの JobLauncher に相当します。
// NOTE: このインターフェース定義は src/main/go/batch/job/joblauncher.go に移動または統合されました。
// このファイルからは削除します。
/*
type JobLauncher interface {
  // Launch は指定された Job を JobParameters とともに起動します。
  // 起動された JobExecution インスタンスを返します。
  // ここで返されるエラーは、ジョブ自体の実行エラーではなく、起動処理自体のエラーです。
  // 型を core パッケージから参照するように変更
  Launch(ctx context.Context, job core.Job, params core.JobParameters) (*core.JobExecution, error)
}
*/

// SimpleJobLauncher は JobLauncher インターフェースのシンプルな実装です。
// JobExecution の基本的なライフサイクル管理を行います。
type SimpleJobLauncher struct {
  // 必要に応じて JobRepository などの依存関係を追加
  // jobRepository JobRepository
}

// NewSimpleJobLauncher は新しい SimpleJobLauncher のインスタンスを作成します。
func NewSimpleJobLauncher(/* JobRepository などの依存関係 */) *SimpleJobLauncher {
  return &SimpleJobLauncher{
    // jobRepository: jobRepository
  }
}

// Launch は指定された Job を JobParameters とともに起動し、JobExecution を管理します。
// job 引数の型を core.Job に、params を core.JobParameters に、戻り値を *core.JobExecution に変更
// JobLauncher インターフェースを満たすようにメソッドシグネチャを維持
func (l *SimpleJobLauncher) Launch(ctx context.Context, job core.Job, params core.JobParameters) (*core.JobExecution, error) {
  // core.Job インターフェースに JobName() string メソッドを追加した前提で取得
  jobName := job.JobName()

  // JobExecution の作成と状態更新 (STARTED 前)
  // core.NewJobExecution を呼び出し
  jobExecution := core.NewJobExecution(jobName, params)
  // JobExecution は NewJobExecution 時点では JobStatusStarting になっているので、ここでは不要
  // jobExecution.Status = core.JobStatusStarting // 起動処理中 (core.JobStatusStarting を参照)


  logger.Infof("Job '%s' (Execution ID: %s) の起動処理を開始します。", jobName, jobExecution.ID)

  // ここで通常、JobRepository を使用して JobExecution を永続化します (ここではスキップ)
  // err := l.jobRepository.Save(ctx, jobExecution)
  // if err != nil {
  //   jobExecution.MarkAsFailed(fmt.Errorf("JobExecution の永続化に失敗しました: %w", err))
  //   logger.Errorf("Job '%s' の起動処理が失敗しました: %v", jobName, jobExecution.Failureliye)
  //   return jobExecution, fmt.Errorf("起動処理エラー: %w", err)
  // }

  // JobExecution の状態を Started に更新
  jobExecution.MarkAsStarted() // core.JobExecution のメソッドを呼び出し
  logger.Infof("Job '%s' (Execution ID: %s) を実行します。", jobName, jobExecution.ID)

  // core.Job の Run メソッドを実行し、JobExecution を渡す
  // Run メソッド内で JobExecution の最終状態が設定されることを期待
  runErr := job.Run(ctx, jobExecution) // JobExecution を渡すように変更

  // Run メソッドの実行結果に関わらず、JobExecution の最終状態は Run メソッド内で設定されているはず
  // ここでは JobExecution の状態を確認し、JobLauncher としてのエラーを返すかどうかを判断
  if runErr != nil {
    // ジョブ自体の実行でエラーが発生した場合、そのエラーを JobLauncher の戻り値として返す
    // JobExecution の状態更新 (MarkAsFailed) は Run メソッド内で既に実行されている
    // logger.Errorf("Job '%s' (Execution ID: %s) の実行でエラーが発生しました: %v", jobName, jobExecution.ID, runErr)
    // JobExecution は成功しても失敗しても返される
    return jobExecution, runErr
  } else {
     // ジョブがエラーなく完了した場合
     // JobExecution の状態更新 (MarkAsCompleted) は Run メソッド内で既に実行されている
     // logger.Infof("Job '%s' (Execution ID: %s) の実行が正常に完了しました。", jobName, jobExecution.ID)
     return jobExecution, nil
  }

  // ここで通常、JobRepository を使用して JobExecution の最終状態を永続化します (ここではスキップ)
  // saveErr := l.jobRepository.Update(ctx, jobExecution)
  // if saveErr != nil {
  //   logger.Errorf("JobExecution の最終状態の永続化に失敗しました: %v", saveErr)
  //   // 永続化エラーを JobExecution に追加することも検討
  //   // jobExecution.AddFailureException(fmt.Errorf("JobExecution 最終状態の永続化エラー: %w", saveErr))
  //   // JobLauncher の起動処理自体のエラーとするか検討
  //   // return jobExecution, fmt.Errorf("JobExecution 最終状態の永続化エラー: %w", saveErr)
  // }

  // 起動処理自体は成功し、JobExecution の実行結果を返します。
  // Job 自体のエラーは JobExecution に含まれています。
  // JobLauncher.Launch は起動処理自体のエラーを返すインターフェース定義なので、
  // ジョブ実行エラー runErr をそのまま返すように修正しました。

  // return jobExecution, nil // JobLauncher 処理自体にエラーがなければ nil を返す

}
