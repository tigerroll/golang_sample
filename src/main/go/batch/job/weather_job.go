package job

import (
  "context"
  "fmt"

  config "sample/src/main/go/batch/config"
  core "sample/src/main/go/batch/job/core" // core パッケージをインポート
  jobListener "sample/src/main/go/batch/job/listener" // jobListener パッケージをインポート
  repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
  logger "sample/src/main/go/batch/util/logger"
)

// WeatherJob は天気予報データを取得・処理・保存するバッチジョブです。
// core.Job インターフェースを実装します。
type WeatherJob struct {
  jobRepository repository.JobRepository // JobRepository を追加
  steps         []core.Step            // 実行するステップのリスト
  config        *config.Config
  // stepListeners map[string][]stepListener.StepExecutionListener // ステップに紐づくリスナーはステップ自身が持つように変更
  jobListeners []jobListener.JobExecutionListener // jobListener パッケージの JobExecutionListener を使用
}

// WeatherJob が core.Job インターフェースを満たすことを確認します。
var _ core.Job = (*WeatherJob)(nil)

// NewWeatherJob は新しい WeatherJob のインスタンスを作成します。
// JobRepository とステップリスト、config を受け取るように変更
func NewWeatherJob(
  jobRepository repository.JobRepository, // JobRepository を追加
  steps []core.Step, // ステップリストを追加
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    jobRepository: jobRepository, // JobRepository を初期化
    steps:         steps,         // ステップリストを初期化
    config:        cfg,
    // stepListeners: make(map[string][]stepListener.StepExecutionListener), // 削除または変更
    jobListeners: make([]jobListener.JobExecutionListener, 0), // jobListener パッケージの JobExecutionListener を使用
  }
}

// RegisterStepListener は指定されたステップ名に StepExecutionListener を登録します。
// このメソッドはステップ自身にリスナーを登録するように変更されるため、Job レベルからは削除または変更が必要
// func (j *WeatherJob) RegisterStepListener(stepName string, l stepListener.StepExecutionListener) {
//   if _, ok := j.stepListeners[stepName]; !ok {
//     j.stepListeners[stepName] = make([]stepListener.StepExecutionListener, 0)
//   }
//   j.stepListeners[stepName] = append(j.stepListeners[stepName], l)
// }

// RegisterJobListener は JobExecutionListener を登録します。
func (j *WeatherJob) RegisterJobListener(l jobListener.JobExecutionListener) { // jobListener パッケージの JobExecutionListener を使用
  j.jobListeners = append(j.jobListeners, l)
}

// notifyBeforeJob は登録されている JobExecutionListener の BeforeJob メソッドを呼び出します。
// JobLauncher の責務として移動することを検討
func (j *WeatherJob) notifyBeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.BeforeJob(ctx, jobExecution)
  }
}

// notifyAfterJob は登録されている JobExecutionListener の AfterJob メソッドを呼び出します。
func (j *WeatherJob) notifyAfterJob(ctx context.Context, jobExecution *core.JobExecution) {
  for _, l := range j.jobListeners {
    l.AfterJob(ctx, jobExecution)
  }
}

// notifyBeforeStep は指定されたステップ名に登録されている StepExecutionListener の BeforeStep メソッドを呼び出します。
// ステップ自身がリスナーを持つように変更されたため、Job レベルからは削除または変更が必要
// func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
//   if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
//     for _, l := range stepListeners {
//       l.BeforeStep(ctx, stepExecution)
//     }
//   }
// }

// notifyAfterStep は指定されたステップ名に登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
// ステップ自身がリスナーを持つように変更されたため、Job レベルからは削除または変更が必要
// func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
//   if stepListeners, ok := j.stepListeners[stepExecution.StepName]; ok {
//     for _, l := range stepListeners {
//       // LoggingListener の AfterStepWithDuration を特別に呼び出す例 (必要に応じて調整)
//       if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
//         loggingListener.AfterStepWithDuration(ctx, stepExecution)
//       } else {
//         l.AfterStep(ctx, stepExecution)
//       }
//     }
//   }
// }

// Run メソッドは core.Job インターフェースの実装です。
// ジョブ全体の実行フローを制御します。複数ステップを順番に実行するように変更します。
func (j *WeatherJob) Run(ctx context.Context, jobExecution *core.JobExecution) error {
  logger.Infof("Weather Job を開始します。Job Execution ID: %s", jobExecution.ID)

  // Job 実行前処理の通知 (JobLauncher から呼び出されるように変更予定だが、一旦ここに残す)
  j.notifyBeforeJob(ctx, jobExecution)

  // Job 実行後処理 (defer で必ず実行) (JobLauncher から呼び出されるように変更予定だが、一旦ここに残す)
  defer func() {
    // ジョブがまだ失敗としてマークされていなければ完了としてマーク
    if jobExecution.Status != core.JobStatusFailed {
      jobExecution.MarkAsCompleted()
    }

    // Job 実行後処理の通知
    j.notifyAfterJob(ctx, jobExecution)

    // リポジトリのリソース解放 (Close メソッドを持つ場合)
    // WeatherRepository は WeatherJobFactory で生成され、WeatherJob に依存として渡されるため、
    // WeatherJob の Run メソッドの defer で Close するのが適切。
    // JobRepository は main で生成され、defer で Close されるため、ここでは不要。
    // ただし、WeatherRepository は JobFactory で生成されているため、JobFactory または main で Close する方が適切かもしれない。
    // ここでは WeatherJob が WeatherRepository への参照を持つ前提で Close する例を示すが、設計によって変更が必要。
    // WeatherJob 構造体から WeatherRepository フィールドは削除されたため、ここでは WeatherRepository の Close は行わない。
    // WeatherRepository の Close は WeatherJobFactory または main 関数で行うべき。
    // if closer, ok := j.repo.(interface{ Close() error }); ok { // repo フィールドは削除
    //   if err := closer.Close(); err != nil {
    //     logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
    //     jobExecution.AddFailureException(fmt.Errorf("リポジトリのクローズエラー: %w", err))
    //     // クローズエラーが発生した場合、ジョブを失敗としてマーク
    //     if jobExecution.Status != core.JobStatusFailed {
    //       jobExecution.MarkAsFailed(fmt.Errorf("リポジトリのクローズエラー: %w", err))
    //     }
    //   }
    // }
  }()

  // ステップを順番に実行
  for _, step := range j.steps {
    stepName := step.StepName()
    logger.Infof("ステップ '%s' の実行を開始します。", stepName)

    // StepExecution の作成と初期永続化
    stepExecution := core.NewStepExecution(stepName, jobExecution)
    // NewStepExecution 内で jobExecution.StepExecutions に追加済み

    // StepExecution を JobRepository に保存
    err := j.jobRepository.SaveStepExecution(ctx, stepExecution)
    if err != nil {
      logger.Errorf("StepExecution (ID: %s) の初期永続化に失敗しました: %v", stepExecution.ID, err)
      // Step の実行を開始せずにジョブ全体を失敗としてマーク
      stepExecution.MarkAsFailed(fmt.Errorf("StepExecution の初期永続化に失敗しました: %w", err))
      j.jobRepository.UpdateStepExecution(ctx, stepExecution) // 失敗状態を永続化
      jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, err))
      return fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, err) // ジョブ全体のエラーとして返す
    }
    logger.Debugf("StepExecution (ID: %s) を JobRepository に初期保存しました。", stepExecution.ID)


    // StepExecution の状態を Started に更新し、永続化
    stepExecution.MarkAsStarted() // StartTime, Status を更新
    err = j.jobRepository.UpdateStepExecution(ctx, stepExecution)
    if err != nil {
      logger.Errorf("StepExecution (ID: %s) の Started 状態への更新に失敗しました: %v", stepExecution.ID, err)
      // 永続化エラーを記録するが、ステップ実行自体は進める
      stepExecution.AddFailureException(fmt.Errorf("StepExecution 状態更新エラー (Started): %w", err))
      // エラーはログ出力に留め、ステップの Execute 処理に進む
    } else {
      logger.Debugf("StepExecution (ID: %s) を JobRepository で Started に更新しました。", stepExecution.ID)
    }


    // ステップの Execute メソッドを実行
    // Execute メソッド内で StepExecution の最終状態が設定されることを期待
    stepErr := step.Execute(ctx, jobExecution, stepExecution)

    // ステップ実行完了後の StepExecution の状態を永続化
    // Execute メソッド内で StepExecution の最終状態 (Completed or Failed) は既に設定されています。
    // ここではその最終状態を JobRepository に保存します。
    updateErr := j.jobRepository.UpdateStepExecution(ctx, stepExecution)
    if updateErr != nil {
      logger.Errorf("StepExecution (ID: %s) の最終状態の更新に失敗しました: %v", stepExecution.ID, updateErr)
      // このエラーを StepExecution に追加
      stepExecution.AddFailureException(fmt.Errorf("StepExecution 最終状態更新エラー: %w", updateErr))
      // ステップ自体が成功していても、永続化エラーがあればジョブ全体を失敗とみなす
      if stepErr == nil {
        stepErr = fmt.Errorf("StepExecution 最終状態の永続化に失敗しました: %w", updateErr)
      } else {
        // ステップ実行エラーと永続化エラーをラップすることも検討
        stepErr = fmt.Errorf("ステップ実行エラー (%w), 永続化エラー (%w)", stepErr, updateErr)
      }
    } else {
      logger.Debugf("StepExecution (ID: %s) を JobRepository で最終状態 (%s) に更新しました。", stepExecution.ID, stepExecution.Status)
    }


    // ステップ実行エラーが発生した場合
    if stepErr != nil {
      logger.Errorf("ステップ '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
        stepName, stepExecution.ID, stepErr)
      // JobExecution を失敗としてマークし、エラーを追加
      jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, stepErr))
      // JobExecution の最終状態は defer で更新されるが、ここで明示的に更新することも検討
      // j.jobRepository.UpdateJobExecution(ctx, jobExecution)
      return fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, stepErr) // ジョブ全体のエラーとして返す
    }

    // ステップが正常に完了した場合
    logger.Infof("ステップ '%s' (Execution ID: %s) が正常に完了しました。最終状態: %s",
      stepName, stepExecution.ID, stepExecution.Status)

    // TODO: ここで条件付き遷移のロジックを評価し、次に実行するステップを決定する (Phase 2)
    //       現在の実装では常に次のステップへ進む（リストの最後まで）

  } // ステップループ終了

  // 全てのステップがエラーなく完了した場合
  logger.Infof("全てのステップが正常に完了しました。Job Execution ID: %s", jobExecution.ID)
  // JobExecution の最終状態は defer で Completed としてマークされる

  return nil // ジョブ全体の実行結果としてエラーがなければ nil を返す
}

// JobName はジョブ名を返します。core.Job インターフェースの実装です。
func (j *WeatherJob) JobName() string {
  return j.config.Batch.JobName
}

// ★ WeatherJob から processChunkLoop, processSingleItem, writeChunk メソッドは削除されます。
// これらのロジックは ChunkOrientedStep に移動しました。
// func (j *WeatherJob) processChunkLoop(...) error { ... }
// func (j *WeatherJob) processSingleItem(...) ([]*entity.WeatherDataToStore, bool, error) { ... }
// func (j *WeatherJob) writeChunk(...) error { ... }
