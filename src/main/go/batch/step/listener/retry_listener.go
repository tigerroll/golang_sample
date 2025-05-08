package listener

import (
  "context" // context パッケージをインポート
  "time"

  config  "sample/src/main/go/batch/config"
  logger  "sample/src/main/go/batch/util/logger"
)

type RetryListener struct {
  config    *config.Config
  attempt   int
  startTime time.Time
}

func NewRetryListener(cfg *config.Config) *RetryListener {
  return &RetryListener{
    config: cfg,
    attempt: 0,
  }
}

// BeforeStep メソッドに ctx context.Context を追加
func (l *RetryListener) BeforeStep(ctx context.Context, stepName string, data interface{}) {
  l.attempt++
  if l.attempt == 1 {
    l.startTime = time.Now()
    logger.Infof("ステップ '%s' (リトライ試行 %d) を開始します。", stepName, l.attempt)
  } else {
    logger.Warnf("ステップ '%s' (リトライ試行 %d) を開始します。", stepName, l.attempt)
  }
  logger.Debugf("ステップ '%s' 開始時のデータ: %+v", stepName, data)
}

// AfterStep メソッドに ctx context.Context を追加
func (l *RetryListener) AfterStep(ctx context.Context, stepName string, data interface{}, err error) {
  duration := time.Since(l.startTime)
  if err != nil {
    logger.Errorf("ステップ '%s' (リトライ試行 %d) でエラーが発生しました: %v (処理時間: %s)", stepName, l.attempt, err, duration.String())
    // Context キャンセルによるエラーか確認することも検討
    if l.attempt >= l.config.Batch.Retry.MaxAttempts {
      logger.Errorf("ステップ '%s' は最大リトライ回数に達しました。", stepName)
    }
  } else {
    logger.Infof("ステップ '%s' (リトライ試行 %d) が完了しました (処理時間: %s)。", stepName, l.attempt, duration.String())
  }
  l.attempt = 0 // リセット
}
