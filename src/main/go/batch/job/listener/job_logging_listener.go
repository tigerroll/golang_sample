package listener

import (
  "context"
  config "sample/src/main/go/batch/config" // config パッケージをインポート
  "sample/src/main/go/batch/util/logger"
)

type LoggingJobListener struct{
  config *config.LoggingConfig // 小さい設定構造体を使用
}

// NewLoggingJobListener が LoggingConfig を受け取るように修正
func NewLoggingJobListener(cfg *config.LoggingConfig) *LoggingJobListener {
  return &LoggingJobListener{
    config: cfg, // 設定を保持
  }
}

func (l *LoggingJobListener) BeforeJob(ctx context.Context, jobName string) {
  // Context や config の Level を使ってログレベルを動的に制御することも可能ですが、ここでは既存の logger を使用
  logger.Infof("Job '%s' の実行を開始します。", jobName)
}

func (l *LoggingJobListener) AfterJob(ctx context.Context, jobName string, err error) {
  // Context や config の Level を使ってログレベルを動的に制御することも可能ですが、ここでは既存の logger を使用
  if err != nil {
    logger.Errorf("Job '%s' がエラーで完了しました: %v", jobName, err)
  } else {
    logger.Infof("Job '%s' の実行が正常に完了しました。", jobName)
  }
}
