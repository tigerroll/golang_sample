package job

import (
  "context"
  "fmt" // fmt パッケージをインポート (エラーメッセージで使用)

  core "sample/src/main/go/batch/job/core"
  "sample/src/main/go/batch/repository" // repository パッケージをインポート
  logger "sample/src/main/go/batch/util/logger"
)

// JobLauncher は Job を JobParameters とともに起動するためのインターフェースです。
// ... (既存の JobLauncher インターフェース定義は削除されているはずです) ...

// SimpleJobLauncher は JobLauncher インターフェースのシンプルな実装です。
// JobExecution の基本的なライフサイクル管理と JobRepository を使用した永続化を行います。
type SimpleJobLauncher struct {
  jobRepository repository.JobRepository // JobRepository を依存として追加
}

// NewSimpleJobLauncher は新しい SimpleJobLauncher のインスタンスを作成します。
// JobRepository の実装を受け取るように変更します。
func NewSimpleJobLauncher(jobRepository repository.JobRepository) *SimpleJobLauncher {
  return &SimpleJobLauncher{
    jobRepository: jobRepository, // JobRepository を初期化
  }
}

// Launch は指定された Job を JobParameters とともに起動し、JobExecution を管理します。
// job 引数の型を core.Job に、params を core.JobParameters に、戻り値を *core.JobExecution に変更
// JobLauncher インターフェースを満たすようにメソッドシグネチャを維持
func (l *SimpleJobLauncher) Launch(ctx context.Context, job core.Job, params core.JobParameters) (*core.JobExecution, error) {
  jobName := job.JobName()

  // JobExecution の作成 (まだ永続化されていない状態)
  jobExecution := core.NewJobExecution(jobName, params)
    // NewJobExecution 時点では Status は JobStatusStarting です

  logger.Infof("Job '%s' (Execution ID: %s) の起動処理を開始します。", jobName, jobExecution.ID)

  // Step 1: JobExecution を JobRepository に保存 (Initial Save)
  // JobExecution の作成直後に永続化します。ステータスは STARTING です。
  err := l.jobRepository.SaveJobExecution(ctx, jobExecution)
  if err != nil {
    // 保存に失敗した場合は、ジョブ実行を開始せずにエラーを返します。
    logger.Errorf("JobExecution (ID: %s) の初期永続化に失敗しました: %v", jobExecution.ID, err)
    return jobExecution, fmt.Errorf("起動処理エラー: JobExecution の初期保存に失敗しました: %w", err)
  }
    logger.Debugf("JobExecution (ID: %s) を JobRepository に初期保存しました。", jobExecution.ID)


  // Step 2: JobExecution の状態を Started に更新し、永続化
  jobExecution.MarkAsStarted() // StartTime, LastUpdated, Status を更新
  err = l.jobRepository.UpdateJobExecution(ctx, jobExecution)
  if err != nil {
    // 更新に失敗した場合、ジョブ実行を開始したものの、状態を正しく記録できなかったことになります。
        // これは深刻な問題ですが、ジョブ自体は実行を開始したとみなします。
        // エラーを記録し、ジョブ実行自体は進めますが、最終的な JobExecution を返す際にエラー情報を含めるべきです。
    logger.Errorf("JobExecution (ID: %s) の Started 状態への更新に失敗しました: %v", jobExecution.ID, err)
        // JobExecution に永続化エラーを追加することも検討
        jobExecution.AddFailureException(fmt.Errorf("JobExecution 状態更新エラー (Started): %w", err))
    // エラーはログ出力に留め、ジョブの Run 処理に進みます。
  } else {
        logger.Debugf("JobExecution (ID: %s) を JobRepository で Started に更新しました。", jobExecution.ID)
    }


  logger.Infof("Job '%s' (Execution ID: %s) を実行します。", jobName, jobExecution.ID)

  // core.Job の Run メソッドを実行し、JobExecution を渡す
  // Run メソッド内で JobExecution の最終状態が設定されることを期待
  // Run メソッドはジョブ自体の実行エラーを返します
  runErr := job.Run(ctx, jobExecution)

  // Step 3: ジョブ実行完了後の JobExecution の状態を永続化
  // Run メソッド内で JobExecution の最終状態 (Completed or Failed) は既に設定されています。
  // ここではその最終状態を JobRepository に保存します。
  updateErr := l.jobRepository.UpdateJobExecution(ctx, jobExecution)
  if updateErr != nil {
    // 最終状態の永続化に失敗した場合
    logger.Errorf("JobExecution (ID: %s) の最終状態の更新に失敗しました: %v", jobExecution.ID, updateErr)
        // このエラーを JobExecution に追加
        jobExecution.AddFailureException(fmt.Errorf("JobExecution 最終状態更新エラー: %w", updateErr))
        // もし Run メソッドが成功していたとしても、永続化エラーがあれば JobLauncher レベルではエラーとみなす
        if runErr == nil {
            runErr = fmt.Errorf("JobExecution 最終状態の永続化に失敗しました: %w", updateErr)
        } else {
            // Run エラーと永続化エラーをラップすることも検討
            runErr = fmt.Errorf("Job実行エラー (%w), 永続化エラー (%w)", runErr, updateErr)
        }
  } else {
        logger.Debugf("JobExecution (ID: %s) を JobRepository で最終状態 (%s) に更新しました。", jobExecution.ID, jobExecution.Status)
    }


  // JobLauncher の Launch メソッドは、起動処理自体のエラーを返すインターフェース定義ですが、
    // ここではジョブ自体の実行エラー (runErr) と最終状態の永続化エラー (updateErr) を含めて返します。
  // JobExecution オブジェクトは常に返します。
  return jobExecution, runErr
}
