package listener

import (
  "context"
  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  "sample/src/main/go/batch/util/logger"
)

type LoggingJobListener struct{
  config *config.LoggingConfig
}

// NewLoggingJobListener が LoggingConfig を受け取るように修正
func NewLoggingJobListener(cfg *config.LoggingConfig) *LoggingJobListener {
  return &LoggingJobListener{
    config: cfg,
  }
}

// BeforeJob メソッドシグネチャを変更し、JobExecution を受け取るようにします。
func (l *LoggingJobListener) BeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
  // JobExecution から必要な情報を取得してログ出力
  logger.Infof("Job '%s' (Execution ID: %s) の実行を開始します。", jobExecution.JobName, jobExecution.ID)
}

// AfterJob メソッドシグネチャを変更し、JobExecution を受け取るようにします。
func (l *LoggingJobListener) AfterJob(ctx context.Context, jobExecution *core.JobExecution) {
  // JobExecution から最終状態やエラー情報を取得してログ出力
  if jobExecution.Status == core.JobStatusFailed {
    logger.Errorf("Job '%s' (Execution ID: %s) がエラーで完了しました: %v",
      jobExecution.JobName, jobExecution.ID, jobExecution.Failureliye)
  } else {
    logger.Infof("Job '%s' (Execution ID: %s) の実行が正常に完了しました。最終状態: %s",
      jobExecution.JobName, jobExecution.ID, jobExecution.Status)
  }
}
