package listener

import (
  "context"
  //"time"

  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  "sample/src/main/go/batch/util/logger"
)

type LoggingListener struct{
  config *config.LoggingConfig
}

// NewLoggingListener が LoggingConfig を受け取るように修正
func NewLoggingListener(cfg *config.LoggingConfig) *LoggingListener {
  return &LoggingListener{
    config: cfg,
  }
}

// BeforeStep メソッドシグネチャを変更し、StepExecution を追加
func (l *LoggingListener) BeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
  // StepExecution からステップ名を取得してログ出力
  logger.Infof("ステップ '%s' (Execution ID: %s) を開始します。", stepExecution.StepName, stepExecution.ID)
  // DebugfのデータはStepExecutionには直接含まれないため、StepExecution自体をログ出力するか、別の方法でデータを渡す必要あり。
  // ここではStepExecutionオブジェクト自体をログ出力する例として残します。
  logger.Debugf("ステップ '%s' 開始時の StepExecution: %+v", stepExecution.StepName, stepExecution)
}

// AfterStep メソッドシグネチャを変更し、StepExecution を追加
func (l *LoggingListener) AfterStep(ctx context.Context, stepExecution *core.StepExecution) {
  // AfterStepWithDuration を呼び出す際に StepExecution を渡すように修正
  l.AfterStepWithDuration(ctx, stepExecution)
}

// AfterStepWithDuration メソッドシグネチャを変更し、StepExecution を受け取るように修正
func (l *LoggingListener) AfterStepWithDuration(ctx context.Context, stepExecution *core.StepExecution) {
  // StepExecution から必要な情報 (エラー、処理時間) を取得してログ出力
  duration := stepExecution.EndTime.Sub(stepExecution.StartTime) // StepExecution にEndTimeが設定されている前提

  if len(stepExecution.Failures) > 0 {
    logger.Errorf("ステップ '%s' (Execution ID: %s) でエラーが発生しました: %v (処理時間: %s)",
      stepExecution.StepName, stepExecution.ID, stepExecution.Failures, duration.String())
  } else {
    logger.Infof("ステップ '%s' (Execution ID: %s) が完了しました (処理時間: %s)。最終状態: %s",
      stepExecution.StepName, stepExecution.ID, duration.String(), stepExecution.Status)
    // DebugfのデータはStepExecutionには直接含まれないため、StepExecution自体をログ出力するか、別の方法でデータを渡す必要あり。
    // ここではStepExecutionオブジェクト自体をログ出力する例として残します。
    logger.Debugf("ステップ '%s' 完了時の StepExecution: %+v", stepExecution.StepName, stepExecution)
  }
}
