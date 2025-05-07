package job

import (
  "context"
  //"fmt"

  core   "sample/src/main/go/batch/job/core"
  logger "sample/src/main/go/batch/util/logger"
)

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
func (l *SimpleJobLauncher) Launch(ctx context.Context, job core.Job, params core.JobParameters) (*core.JobExecution, error) {
  // ジョブ名を取得 (core.Job インターフェースに名前取得メソッドを追加するか、別の方法で取得する必要があります)
  // 例として、core.Job を実装する構造体が JobName() string メソッドを持つと仮定
  jobName := "UnknownJob" // デフォルト値
  // 型アサーションは job.(...) のままでOK。WeatherJob は job パッケージにいるので参照可能。
  if namedJob, ok := job.(interface{ JobName() string }); ok {
    jobName = namedJob.JobName()
  } else if weatherJob, ok := job.(*WeatherJob); ok { // WeatherJob 専用の対応 (WeatherJob は job パッケージにいる)
    // WeatherJob が core.Job を実装していることを前提とする
    jobName = weatherJob.config.Batch.JobName
  }

  // JobExecution の作成と状態更新 (STARTED 前)
  // core.NewJobExecution を呼び出し
  jobExecution := core.NewJobExecution(jobName, params)
  jobExecution.Status = core.JobStatusStarting // 起動処理中 (core.JobStatusStarting を参照)

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

  // core.Job の Run メソッドを実行
  runErr := job.Run(ctx) // ここで Job 自体のロジックが実行されます

  // Run メソッドの実行結果に応じて JobExecution の状態を更新
  if runErr != nil {
    jobExecution.MarkAsFailed(runErr) // core.JobExecution のメソッドを呼び出し
    logger.Errorf("Job '%s' (Execution ID: %s) がエラーで完了しました: %v", jobName, jobExecution.ID, runErr)
  } else {
    jobExecution.MarkAsCompleted() // core.JobExecution のメソッドを呼び出し
    logger.Infof("Job '%s' (Execution ID: %s) が正常に完了しました。", jobName, jobExecution.ID)
  }

  // ここで通常、JobRepository を使用して JobExecution の最終状態を永続化します (ここではスキップ)
  // saveErr := l.jobRepository.Update(ctx, jobExecution)
  // if saveErr != nil {
  //   logger.Errorf("JobExecution の最終状態の永続化に失敗しました: %v", saveErr)
  //   // 永続化エラーを JobExecution に追加することも検討
  //   // jobExecution.AddFailureException(fmt.Errorf("JobExecution 最終状態の永続化エラー: %w", saveErr))
  // }

  // 起動処理自体は成功し、JobExecution の実行結果を返します。
  // Job 自体のエラーは JobExecution に含まれています。
  return jobExecution, nil
}

// Job インターフェースに JobName() string を追加するのが適切ですが、
// 例として WeatherJob 専用の JobName 取得を追加しました。
// より汎用的にするには core.Job インターフェースを修正してください。
// type Job interface {
//   Run(ctx context.Context) error
//   JobName() string // core.Job インターフェースに追加する場合
// }
