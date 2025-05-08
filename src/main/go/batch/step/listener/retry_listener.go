package listener

import (
  "context"
  "time"

  config  "sample/src/main/go/batch/config" // config パッケージをインポート
  logger  "sample/src/main/go/batch/util/logger"
)

type RetryListener struct {
  config    *config.RetryConfig // 小さい設定構造体を使用
  attempt   int
  startTime time.Time
}

// NewRetryListener が RetryConfig を受け取るように修正
func NewRetryListener(cfg *config.RetryConfig) *RetryListener {
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
    // config フィールドから必要な設定にアクセス
    if l.attempt >= l.config.MaxAttempts {
      logger.Errorf("ステップ '%s' は最大リトライ回数に達しました。", stepName)
    }
  } else {
    logger.Infof("ステップ '%s' (リトライ試行 %d) が完了しました (処理時間: %s)。", stepName, l.attempt, duration.String())
  }
  l.attempt = 0 // リセット
}
