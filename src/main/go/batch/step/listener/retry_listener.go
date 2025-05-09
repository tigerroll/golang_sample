package listener

import (
  "context"
  //"time"

  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  "sample/src/main/go/batch/util/logger"
)

// RetryListener は StepExecutionListener インターフェースを満たします。
type RetryListener struct {
  config *config.RetryConfig // 小さい設定構造体を使用
  // attempt フィールドは StepExecution に含まれるため不要になる可能性があるが、
  // リトライロジックの実装方法によってはリスナー側で持つ必要が出てくるかも。
  // StepExecutionのRetryCountなどを使用するのがよりSpring Batch的。
  // ここではシンプルにリスナー側で管理する例として残します。
  // attempt int
  // startTime time.Time // StepExecutionにStartTimeが含まれるため不要
}

// NewRetryListener が RetryConfig を受け取るように修正
func NewRetryListener(cfg *config.RetryConfig) *RetryListener {
  return &RetryListener{
    config: cfg,
    // attempt: 0, // リセットは AfterStep で行う
  }
}

// BeforeStep メソッドシグネチャを変更し、StepExecution を追加
func (l *RetryListener) BeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
  // StepExecution からステップ名を取得
  stepName := stepExecution.StepName
  // StepExecution からリトライ回数などを取得 (StepExecutionにRetryCountフィールドを追加するなど)
  // ここではStepExecutionのFailureliyeスライスの長さをリトライ回数として概算します。
  // 正確なリトライ回数は StepExecution に専用のフィールドを持たせるか、Execution Contextで管理すべきです。
  attempt := len(stepExecution.Failureliye) + 1 // 最初の試行はリトライ0回目

  if attempt == 1 {
    // StepExecutionにStartTimeが設定されている前提
    logger.Infof("ステップ '%s' (リトライ試行 %d) (Execution ID: %s) を開始します。", stepName, attempt, stepExecution.ID)
  } else {
    logger.Warnf("ステップ '%s' (リトライ試行 %d) (Execution ID: %s) を開始します。", stepName, attempt, stepExecution.ID)
  }
  // DebugfのデータはStepExecutionには直接含まれないため、StepExecution自体をログ出力する例として残します。
  logger.Debugf("ステップ '%s' 開始時の StepExecution: %+v", stepName, stepExecution)
}

// AfterStep メソッドシグネチャを変更し、StepExecution を追加
func (l *RetryListener) AfterStep(ctx context.Context, stepExecution *core.StepExecution) {
  // StepExecution から必要な情報 (エラー、処理時間) を取得してログ出力
  stepName := stepExecution.StepName
  duration := stepExecution.EndTime.Sub(stepExecution.StartTime) // StepExecution にEndTimeが設定されている前提
  attempt := len(stepExecution.Failureliye) // AfterStep時点では、失敗していれば Failureliye にエラーが追加されているはず

  if len(stepExecution.Failureliye) > 0 {
    logger.Errorf("ステップ '%s' (リトライ試行 %d) (Execution ID: %s) でエラーが発生しました: %v (処理時間: %s)",
      stepName, attempt, stepExecution.ID, stepExecution.Failureliye, duration.String())
    // config フィールドから必要な設定にアクセスしてリトライ上限を確認
    if attempt >= l.config.MaxAttempts {
      logger.Errorf("ステップ '%s' は最大リトライ回数に達しました。", stepName)
    }
  } else {
    logger.Infof("ステップ '%s' (リトライ試行 %d) (Execution ID: %s) が完了しました (処理時間: %s)。最終状態: %s",
      stepName, attempt+1, stepExecution.ID, duration.String(), stepExecution.Status) // 成功時は試行回数+1を表示
  }
  // attempt は StepExecution に含まれる情報を使うようにすれば、リスナー側でのリセットは不要になる
  // l.attempt = 0 // リセット
}
