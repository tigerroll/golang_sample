package listener

import (
  "context" // context パッケージをインポート
  "time"

  logger  "sample/src/main/go/batch/util/logger"
)

type LoggingListener struct{}

func NewLoggingListener() *LoggingListener {
  return &LoggingListener{}
}

// BeforeStep メソッドに ctx context.Context を追加
func (l *LoggingListener) BeforeStep(ctx context.Context, stepName string, data interface{}) {
  logger.Infof("ステップ '%s' を開始します。", stepName)
  logger.Debugf("ステップ '%s' 開始時のデータ: %+v", stepName, data)
}

// AfterStep メソッドに ctx context.Context を追加
func (l *LoggingListener) AfterStep(ctx context.Context, stepName string, data interface{}, err error) {
  // AfterStepWithDuration も Context を受け取るように修正
  l.AfterStepWithDuration(ctx, stepName, data, err, 0) // デフォルトの呼び出し用
}

// AfterStepWithDuration に ctx context.Context を追加
func (l *LoggingListener) AfterStepWithDuration(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  // Context をログメッセージに含めるなどの処理を追加することも検討
  if err != nil {
    logger.Errorf("ステップ '%s' でエラーが発生しました: %v", stepName, err)
  } else {
    logger.Infof("ステップ '%s' が完了しました (処理時間: %s)。", stepName, duration.String())
    logger.Debugf("ステップ '%s' 完了時のデータ: %+v", stepName, data)
  }
}
