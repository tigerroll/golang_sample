package listener

import (
  "context"
  "time"
  "sample/src/main/go/batch/util/logger"
)

type LoggingListener struct{}

func NewLoggingListener() *LoggingListener {
  return &LoggingListener{}
}

func (l *LoggingListener) BeforeStep(ctx context.Context, stepName string, data interface{}) {
  logger.Infof("ステップ '%s' を開始します。", stepName)
  logger.Debugf("ステップ '%s' 開始時のデータ: %+v", stepName, data)
}

func (l *LoggingListener) AfterStep(ctx context.Context, stepName string, data interface{}, err error) {
  l.AfterStepWithDuration(ctx, stepName, data, err, 0) // デフォルトの呼び出し用
}

func (l *LoggingListener) AfterStepWithDuration(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  if err != nil {
    logger.Errorf("ステップ '%s' でエラーが発生しました: %v", stepName, err)
  } else {
    logger.Infof("ステップ '%s' が完了しました (処理時間: %s)。", stepName, duration.String())
    logger.Debugf("ステップ '%s' 完了時のデータ: %+v", stepName, data)
  }
}
