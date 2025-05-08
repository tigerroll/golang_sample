package listener

import (
  "context"
  "time"

  config  "sample/src/main/go/batch/config" // config パッケージをインポート
  logger  "sample/src/main/go/batch/util/logger"
)

type LoggingListener struct{
  config *config.LoggingConfig // 小さい設定構造体を使用
}

// NewLoggingListener が LoggingConfig を受け取るように修正
func NewLoggingListener(cfg *config.LoggingConfig) *LoggingListener {
  return &LoggingListener{
    config: cfg, // 設定を保持
  }
}

// BeforeStep メソッドに ctx context.Context を追加
func (l *LoggingListener) BeforeStep(ctx context.Context, stepName string, data interface{}) {
  // Context や config の Level を使ってログレベルを動的に制御することも可能ですが、ここでは既存の logger を使用
  logger.Infof("ステップ '%s' を開始します。", stepName)
  logger.Debugf("ステップ '%s' 開始時のデータ: %+v", stepName, data)
}

// AfterStep メソッドに ctx context.Context を追加
func (l *LoggingListener) AfterStep(ctx context.Context, stepName string, data interface{}, err error) {
  // AfterStepWithDuration も Context と LoggingConfig を考慮するように修正
  l.AfterStepWithDuration(ctx, stepName, data, err, 0) // デフォルトの呼び出し用
}

// AfterStepWithDuration に ctx context.Context を追加
func (l *LoggingListener) AfterStepWithDuration(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  // Context や config の Level を使ってログレベルを動的に制御することも可能ですが、ここでは既存の logger を使用
  if err != nil {
    logger.Errorf("ステップ '%s' でエラーが発生しました: %v", stepName, err)
  } else {
    logger.Infof("ステップ '%s' が完了しました (処理時間: %s)。", stepName, duration.String())
    logger.Debugf("ステップ '%s' 完了時のデータ: %+v", stepName, data)
  }
}
