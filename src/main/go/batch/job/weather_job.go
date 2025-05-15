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
  // steps         []core.Step            // 実行するステップのリスト (フロー定義に置き換え)
  config        *config.Config
  jobListeners []jobListener.JobExecutionListener // jobListener パッケージの JobExecutionListener を使用
  flow          *core.FlowDefinition // ★ フロー定義を追加
}

// WeatherJob が core.Job インターフェースを満たすことを確認します。
// GetFlow メソッドを追加したので、このチェックは成功するはずです。
var _ core.Job = (*WeatherJob)(nil)

// NewWeatherJob は新しい WeatherJob のインスタンスを作成します。
// JobRepository とステップリスト、config を受け取るように変更
// ★ ステップリストの代わりに FlowDefinition を受け取るように変更
func NewWeatherJob(
  jobRepository repository.JobRepository, // JobRepository を追加
  // steps []core.Step, // ステップリストを削除
  cfg *config.Config,
  flow *core.FlowDefinition, // ★ FlowDefinition を追加
) *WeatherJob {
  return &WeatherJob{
    jobRepository: jobRepository, // JobRepository を初期化
    // steps:         steps,         // ステップリストを削除
    config:        cfg,
    jobListeners: make([]jobListener.JobExecutionListener, 0), // jobListener パッケージの JobExecutionListener を使用
    flow:          flow, // ★ FlowDefinition を初期化
  }
}

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

// Run メソッドは core.Job インターフェースの実装です。
// ジョブ全体の実行フローを制御します。複数ステップを順番に実行するように変更します。
// ★ フロー定義に基づいてステップを実行するように変更します。
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

  // ★ フロー定義に基づいてステップを実行するロジック ★
  // フェーズ2 ステップ2 で本格的に実装しますが、コンパイルを通すために StartElement を実行する最小限のロジックを追加します。
  currentElementName := j.flow.StartElement // フローの開始要素名を取得

  // TODO: リスタート時には jobExecution.CurrentStepName から開始するロジックを追加

  // フローの要素が存在する限りループ (フェーズ2 ステップ2 でフロー制御ロジックに置き換え)
  // 現在は StartElement のみを実行する簡略化されたループ
  for currentElementName != "" {
    // Context の完了をチェック
    select {
    case <-ctx.Done():
      logger.Warnf("Context がキャンセルされたため、Job '%s' の実行を中断します: %v", j.JobName(), ctx.Err())
      jobExecution.MarkAsFailed(ctx.Err()) // ジョブを失敗としてマーク
      return ctx.Err() // Context エラーは即座に返す
    default:
    }

    // 現在の要素（ステップまたはDecision）を取得
    currentElement, ok := j.flow.GetElement(currentElementName)
    if !ok {
      err := fmt.Errorf("フロー定義に要素 '%s' が見つかりません", currentElementName)
      logger.Errorf("%v", err)
      jobExecution.MarkAsFailed(err) // ジョブを失敗としてマーク
      return err // エラーを返してジョブを失敗させる
    }

    // 要素のタイプに応じて処理を分岐
    switch element := currentElement.(type) {
    case core.Step:
      // ステップの場合の実行ロジック
      stepName := element.StepName()
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

      // JobExecution の CurrentStepName を更新し永続化 (リスタート対応のため)
      jobExecution.CurrentStepName = stepName
      err = j.jobRepository.UpdateJobExecution(ctx, jobExecution)
      if err != nil {
        logger.Errorf("JobExecution (ID: %s) の CurrentStepName 更新に失敗しました: %v", jobExecution.ID, err)
        // 永続化エラーを記録するが、ステップ実行自体は進める
        jobExecution.AddFailureException(fmt.Errorf("JobExecution CurrentStepName 更新エラー: %w", err))
      }


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
      stepErr := element.Execute(ctx, jobExecution, stepExecution)

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

      // TODO: ここで条件付き遷移のロジックを評価し、次に実行する要素を決定する (フェーズ 2 ステップ 2)
      //       現在の実装では StartElement の実行後、ループを抜けます。
      currentElementName = "" // StartElement の実行後、ループを終了させるための暫定処理

    // case core.Decision:
    //   // Decision の場合の後続ロジック (フェーズ3)
    //   // TODO: Decision.Decide を呼び出し、結果に基づいて次の要素名を決定

    default:
      // 未知の要素タイプの場合
      err := fmt.Errorf("フロー定義に未知の要素タイプ '%T' が含まれています (要素名: '%s')", currentElement, currentElementName)
      logger.Errorf("%v", err)
      jobExecution.MarkAsFailed(err) // ジョブを失敗としてマーク
      return err // エラーを返してジョブを失敗させる
    }
  } // フロー実行ループ終了

  // ここに到達するのは、フローが正常に終了した場合、または StartElement の実行が完了した場合（現在の簡略化されたロジック）
  logger.Infof("Job '%s' (Execution ID: %s) のフロー実行が完了しました。", j.JobName(), jobExecution.ID)

  // JobExecution の最終状態は defer で Completed としてマークされる

  return nil // ジョブ全体の実行結果としてエラーがなければ nil を返す
}

// JobName はジョブ名を返します。core.Job インターフェースの実装です。
func (j *WeatherJob) JobName() string {
  return j.config.Batch.JobName
}

// GetFlow はジョブのフロー定義を返します。core.Job インターフェースの実装です。
// ★ core.Job インターフェースに追加したメソッドの実装
func (j *WeatherJob) GetFlow() *core.FlowDefinition {
  return j.flow
}

// ★ WeatherJob から processChunkLoop, processSingleItem, writeChunk メソッドは削除されます。
// これらのロジックは ChunkOrientedStep に移動しました。
// func (j *WeatherJob) processChunkLoop(...) error { ... }
// func (j *WeatherJob) processSingleItem(...) ([]*entity.WeatherDataToStore, bool, error) { ... }
// func (j *WeatherJob) writeChunk(...) error { ... }
