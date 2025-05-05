package listener

import (
  "context"
  "sample/src/main/go/batch/util/logger"
)

type LoggingJobListener struct{}

func NewLoggingJobListener() *LoggingJobListener {
  return &LoggingJobListener{}
}

func (l *LoggingJobListener) BeforeJob(ctx context.Context, jobName string) {
  logger.Infof("Job '%s' の実行を開始します。", jobName)
}

func (l *LoggingJobListener) AfterJob(ctx context.Context, jobName string, err error) {
  if err != nil {
    logger.Errorf("Job '%s' がエラーで完了しました: %v", jobName, err)
  } else {
    logger.Infof("Job '%s' の実行が正常に完了しました。", jobName)
  }
}
