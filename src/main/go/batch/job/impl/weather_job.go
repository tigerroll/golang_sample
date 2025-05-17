// (新規作成または移動・修正)
package impl // パッケージ名を impl に変更

import (
  "context"
  "fmt"
  // "time" // time パッケージは直接使用されていないため削除

  config "sample/src/main/go/batch/config" // config パッケージをインポート
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
// この関数は impl パッケージに属します。
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

// RegisterListener は JobExecutionListener を登録します。
func (j *WeatherJob) RegisterListener(l jobListener.JobExecutionListener) { // jobListener パッケージの JobExecutionListener を使用
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
    // ジョブがまだ失敗、停止、または放棄としてマークされていなければ完了としてマーク
    if jobExecution.Status != core.JobStatusFailed &&
      jobExecution.Status != core.JobStatusStopped &&
      jobExecution.Status != core.JobStatusAbandoned {
      jobExecution.MarkAsCompleted()
    }

    // JobExecution の最終状態を JobRepository で更新
    // defer 内で発生したエラーは JobLauncher に伝播しないため、ログ出力のみ行う
    updateErr := j.jobRepository.UpdateJobExecution(ctx, jobExecution)
    if updateErr != nil {
      logger.Errorf("JobExecution (ID: %s) の最終状態の更新に失敗しました: %v", jobExecution.ID, updateErr)
      jobExecution.AddFailureException(fmt.Errorf("JobExecution 最終状態更新エラー (defer): %w", updateErr))
      // 最終状態の永続化に失敗した場合、ジョブの状態を強制的に FAILED にする
      jobExecution.Status = core.JobStatusFailed
      jobExecution.ExitStatus = core.ExitStatusFailed
    } else {
      logger.Debugf("JobExecution (ID: %s) を JobRepository で最終状態 (%s) に更新しました。", jobExecution.ID, jobExecution.Status)
    }

    // Job 実行後処理の通知
    j.notifyAfterJob(ctx, jobExecution)

    // WeatherRepository の Close は WeatherJobFactory または main 関数で行うべき。
  }()

  // ★ フロー定義に基づいてステップを実行するロジック ★
  currentElementName := j.flow.StartElement // フローの開始要素名を取得

  // TODO: リスタート時には jobExecution.CurrentStepName から開始するロジックを追加
  //       if jobExecution.CurrentStepName != "" && jobExecution.Status == core.JobStatusStarted {
  //           currentElementName = jobExecution.CurrentStepName // 前回の停止ステップから再開
  //       }

  // フローの要素が存在する限りループ
  for {
    // Context の完了をチェック
    select {
    case <-ctx.Done():
      logger.Warnf("Context がキャンセルされたため、Job '%s' の実行を中断します: %v", j.JobName(), ctx.Err())
      // Context キャンセル時は JobExecution を FAILED としてマークし、defer で永続化
      jobExecution.MarkAsFailed(ctx.Err())
      // defer で最終状態が永続化されるため、ここではエラーを返してループを抜ける
      return ctx.Err()
    default:
    }

    // currentElementName が空文字列の場合、フローは正常に終了したとみなす (例: <end> 遷移)
    if currentElementName == "" {
      logger.Infof("Job '%s' (Execution ID: %s) のフローが正常に終了しました。", j.JobName(), jobExecution.ID)
      // defer で JobExecution が COMPLETED としてマークされ、永続化される
      return nil // 正常完了
    }

    // 現在の要素（ステップまたはDecision）を取得
    currentElement, ok := j.flow.GetElement(currentElementName)
    if !ok {
      // フロー定義に要素が見つからない場合はエラー
      err := fmt.Errorf("フロー定義に要素 '%s' が見つかりません", currentElementName)
      logger.Errorf("%v", err)
      // JobExecution を FAILED としてマークし、defer で永時化
      jobExecution.MarkAsFailed(err)
      return err // エラーを返してジョブを失敗させる
    }

    var elementExitStatus core.ExitStatus // 実行した要素の ExitStatus
    var elementErr error                // 実行した要素が返したエラー

    // JobExecution の状態更新エラー変数をループ内で宣言
    var updateJobExecErr error

    // 要素のタイプに応じて処理を分岐
    switch element := currentElement.(type) {
    case core.Step:
      // ステップの場合の実行ロジック
      stepName := element.StepName()
      logger.Infof("ステップ '%s' の実行を開始します。", stepName)

      // StepExecution の作成と初期永続化
      stepExecution := core.NewStepExecution(stepName, jobExecution)
      // NewStepExecution 内で jobExecution.StepExecutions に追加済み

      saveErr := j.jobRepository.SaveStepExecution(ctx, stepExecution)
      if saveErr != nil {
        logger.Errorf("StepExecution (ID: %s) の初期永続化に失敗しました: %v", stepExecution.ID, saveErr)
        // Step の実行を開始せずにジョブ全体を失敗としてマーク
        stepExecution.MarkAsFailed(fmt.Errorf("StepExecution の初期永続化に失敗しました: %w", saveErr))
        // 失敗状態を永続化 (defer で JobExecution も更新されるが、StepExecution も更新しておく)
        j.jobRepository.UpdateStepExecution(ctx, stepExecution)
        jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, saveErr))
        // defer で JobExecution が永続化されるため、ここではエラーを返してループを抜ける
        return fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, saveErr)
      }
      logger.Debugf("StepExecution (ID: %s) を JobRepository に初期保存しました。", stepExecution.ID)

      // JobExecution の CurrentStepName を更新し永続化 (リスタート対応のため)
      jobExecution.CurrentStepName = stepName
      // updateJobExecErr 変数を使用
      updateJobExecErr = j.jobRepository.UpdateJobExecution(ctx, jobExecution)
      if updateJobExecErr != nil {
        logger.Errorf("JobExecution (ID: %s) の CurrentStepName 更新に失敗しました: %v", jobExecution.ID, updateJobExecErr)
        jobExecution.AddFailureException(fmt.Errorf("JobExecution CurrentStepName 更新エラー: %w", updateJobExecErr))
        // 永続化エラーは記録するが、ステップ実行自体は進める
      }

      // Mark StepExecution as Started and persist
      stepExecution.MarkAsStarted() // StartTime, Status を更新
      // updateStepExecErr 変数を再利用 (StepExecution 用)
      var updateStepExecErr error // StepExecution の更新エラー変数を宣言
      updateStepExecErr = j.jobRepository.UpdateStepExecution(ctx, stepExecution)
      if updateStepExecErr != nil {
        logger.Errorf("StepExecution (ID: %s) の Started 状態への更新に失敗しました: %v", stepExecution.ID, updateStepExecErr)
        stepExecution.AddFailureException(fmt.Errorf("StepExecution 状態更新エラー (Started): %w", updateStepExecErr))
        // 永続化エラーは記録するが、ステップ実行自体は進める
      } else {
        logger.Debugf("StepExecution (ID: %s) を JobRepository で Started に更新しました。", stepExecution.ID, stepExecution.Status)
      }

      // ステップの Execute メソッドを実行
      // Execute メソッド内で StepExecution の最終状態が設定されることを期待
      elementErr = element.Execute(ctx, jobExecution, stepExecution)

      // ステップ実行完了後の StepExecution の最終状態を永続化
      // Execute メソッド内で StepExecution の最終状態 (Completed or Failed) は既に設定されています。
      // ここではその最終状態を JobRepository に保存します。
      // updateStepExecErr 変数を再利用
      updateStepExecErr = j.jobRepository.UpdateStepExecution(ctx, stepExecution)
      if updateStepExecErr != nil {
        logger.Errorf("StepExecution (ID: %s) の最終状態の更新に失敗しました: %v", stepExecution.ID, updateStepExecErr)
        stepExecution.AddFailureException(fmt.Errorf("StepExecution 最終状態更新エラー: %w", updateStepExecErr))
        // ステップ自体が成功していても永続化エラーがあれば、ステップの ExitStatus を FAILED に変更
        if stepExecution.ExitStatus != core.ExitStatusFailed {
          stepExecution.ExitStatus = core.ExitStatusFailed
          stepExecution.Status = core.JobStatusFailed // Status も FAILED に変更
          // この変更を再度永続化する必要があるか検討。ループの次の遷移評価で FAILED として扱われるため不要かもしれない。
        }
        // elementErr が nil の場合、永続化エラーを要素エラーとして扱う
        if elementErr == nil {
          elementErr = fmt.Errorf("StepExecution 最終状態の永続化に失敗しました: %w", updateStepExecErr)
        } else {
          // 既存のステップエラーと永続化エラーをラップ
          elementErr = fmt.Errorf("ステップ実行エラー (%w), StepExecution 最終状態永時化エラー (%w)", elementErr, updateStepExecErr)
        }
      } else {
        logger.Debugf("StepExecution (ID: %s) を JobRepository で最終状態 (%s) に更新しました。", stepExecution.ID, stepExecution.Status)
      }

      // 実行したステップの ExitStatus を取得
      elementExitStatus = stepExecution.ExitStatus

      // ステップ実行エラーが発生した場合のログ出力
      if elementErr != nil {
        logger.Errorf("ステップ '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
          stepName, stepExecution.ID, elementErr)
        // elementExitStatus は既に StepExecution から取得済み (エラー発生時は通常 FAILED)
      } else {
        // ステップが正常に完了した場合のログ出力
        logger.Infof("ステップ '%s' (Execution ID: %s) が正常に完了しました。最終状態: %s",
          stepName, stepExecution.ID, stepExecution.Status)
        // elementExitStatus は既に StepExecution から取得済み (成功時は通常 COMPLETED)
      }


    // case core.Decision: // core.Decision はまだ定義されていないためコメントアウト
    //   // TODO: Handle Decision execution (Phase 3)
    //   // Call Decision.Decide(ctx, jobExecution)
    //   // Get result string
    //   // elementExitStatus = core.ExitStatus(resultString) // Map decision result to ExitStatus concept for transition
    //   // elementErr = nil // Decision execution itself might not return error, but result indicates flow direction
    //   err := fmt.Errorf("Decision 要素の実行はまだ実装されていません (要素名: '%s')", currentElementName)
    //   logger.Errorf("%v", err)
    //   // JobExecution を FAILED としてマークし、defer で永続化
    //   jobExecution.MarkAsFailed(err)
    //   return err // エラーを返してジョブを失敗させる

    default:
      // 未知の要素タイプの場合
      err := fmt.Errorf("フロー定義に未知の要素タイプ '%T' が含まれています (要素名: '%s')", currentElement, currentElementName)
      logger.Errorf("%v", err)
      // JobExecution を FAILED としてマークし、defer で永時化
      jobExecution.MarkAsFailed(err)
      return err // エラーを返してジョブを失敗させる
    }

    // --- 遷移評価 ---
    // 実行した要素の名前と ExitStatus に基づいて遷移ルールを検索
    transition := j.flow.FindTransition(currentElementName, elementExitStatus)

    if transition == nil {
      // 一致する遷移ルールが見つからない場合はエラー
      err := fmt.Errorf("要素 '%s' (ExitStatus: %s) に対する遷移ルールが見つかりません。", currentElementName, elementExitStatus)
      logger.Errorf("%v", err)
      // JobExecution を FAILED としてマークし、defer で永続化
      jobExecution.MarkAsFailed(err)
      return err // エラーを返してジョブを失敗させる
    }

    // 見つかった遷移ルールを評価
    if transition.End {
      // End 遷移: ジョブを完了としてマークし、ループを終了
      logger.Infof("遷移ルールにより Job '%s' (Execution ID: %s) を終了します。最終 ExitStatus: %s",
        j.JobName(), jobExecution.ID, transition.EndStatus)
      jobExecution.Status = core.JobStatusCompleted // End 遷移の場合、Status は通常 COMPLETED
      jobExecution.ExitStatus = transition.EndStatus
      currentElementName = "" // ループを終了するための条件を設定
    } else if transition.Fail {
      // Fail 遷移: ジョブを失敗としてマークし、ループを終了
      logger.Errorf("遷移ルールにより Job '%s' (Execution ID: %s) を失敗とします。最終 ExitStatus: %s",
        j.JobName(), jobExecution.ID, transition.FailStatus)
      jobExecution.Status = core.JobStatusFailed // Fail 遷移の場合、Status は FAILED
      jobExecution.ExitStatus = transition.FailStatus
      // 遷移ルールによる失敗であることを示す例外を追加
      jobExecution.AddFailureException(fmt.Errorf("遷移ルールによるジョブ失敗 (ExitStatus: %s)", transition.FailStatus))
      currentElementName = "" // ループを終了するための条件を設定
    } else if transition.Stop {
      // Stop 遷移: ジョブを停止としてマークし、ループを終了
      logger.Warnf("遷移ルールにより Job '%s' (Execution ID: %s) を停止します。Restartable: %t",
        j.JobName(), jobExecution.ID, transition.Restartable)
      jobExecution.Status = core.JobStatusStopped // Stop 遷移の場合、Status は STOPPED
      jobExecution.ExitStatus = core.ExitStatusStopped // ExitStatus は通常 STOPPED
      // TODO: Restartable フラグを JobExecution に保存するか検討 (JobOperator で利用するため)
      currentElementName = "" // ループを終了するための条件を設定
    } else if transition.To != "" {
      // Next 遷移: 次の要素に進む
      logger.Debugf("遷移ルールにより次の要素 '%s' へ進みます。", transition.To)
      currentElementName = transition.To // 次のループで実行する要素名を設定
    } else {
      // 遷移ルールが無効 (To, End, Fail, Stop のいずれも指定されていない)
      err := fmt.Errorf("要素 '%s' (ExitStatus: %s) に対する遷移ルールが無効です (To, End, Fail, Stop のいずれも指定されていません)。",
        currentElementName, elementExitStatus)
      logger.Errorf("%v", err)
      // JobExecution を FAILED としてマークし、defer で永時化
      jobExecution.MarkAsFailed(err)
      return err // エラーを返してジョブを失敗させる
    }

    // 要素の実行でエラーが発生した場合 (elementErr != nil) でも、遷移ルールが見つかればそのルールに従います。
    // 例えば、ステップが失敗 (elementErr != nil, ExitStatus=FAILED) しても、
    // フロー定義に 'from: stepName, on: FAILED, to: cleanupStep' のような遷移があれば、
    // ジョブは即座に失敗せず cleanupStep へ進みます。
    // ただし、elementErr は JobExecution の Failureliye に追加されているべきです。
    // StepExecution の実行結果を JobExecution の Failureliye に追加する処理は、
    // ChunkOrientedStep.Execute 内や、その呼び出し側 (ここ) で行う必要があります。
    // 現在の ChunkOrientedStep.Execute では、ステップ内部のエラーを StepExecution に追加しています。
    // ここでは、ステップ実行がエラーを返した場合 (elementErr != nil) に、そのエラーを JobExecution にも追加します。
    if elementErr != nil {
      jobExecution.AddFailureException(elementErr)
      // JobExecution の Status/ExitStatus は遷移ルールによって決定されるため、ここでは変更しません。
    }

    // 各要素の処理と遷移評価の後、JobExecution の状態を JobRepository に更新
    // これにより、JobExecution の CurrentStepName や Status が永続化され、リスタート時に利用できます。
    // updateJobExecErr 変数を使用
    updateJobExecErr = j.jobRepository.UpdateJobExecution(ctx, jobExecution)
    if updateJobExecErr != nil {
      logger.Errorf("JobExecution (ID: %s) の状態更新に失敗しました (要素 '%s' 処理後): %v",
        jobExecution.ID, currentElementName, updateJobExecErr)
      jobExecution.AddFailureException(fmt.Errorf("JobExecution 状態更新エラー (要素 '%s' 処理後): %w", currentElementName, updateJobExecErr))
      // この永続化エラーが発生した場合、ジョブを即座に失敗させるか検討が必要です。
      // フローが終了遷移でない場合、ログ出力して続行する選択肢もあります。
      // ここでは、フローが終了遷移でない限りログ出力に留めます。
      if currentElementName == "" { // フローが終了遷移でループを抜ける場合
        // 最終状態の永続化に失敗した場合はエラーを返す
        return fmt.Errorf("JobExecution (ID: %s) の最終状態の永続化に失敗しました: %w", jobExecution.ID, updateJobExecErr)
      }
    }

    // currentElementName が空文字列になった場合はループを終了
    if currentElementName == "" {
      break
    }

  } // フロー実行ループ終了

  // ループが終了した場合、defer 関数が JobExecution の最終状態を JobRepository に永続化します。
  // ループの終了は、End, Fail, Stop 遷移によるものか、または currentElementName が空になった場合です。
  // End, Fail, Stop 遷移の場合は、ループ内で jobExecution.Status/ExitStatus が既に設定されています。
  // currentElementName が空になった場合は、defer で COMPLETED としてマークされます。

  // ループ内でエラーが発生して return された場合は、defer が実行され、そのエラーが JobLauncher に返されます。
  // ループが正常に終了した場合は、nil が返されます。

  // ここに到達するのは、ループが break で終了した場合（End, Fail, Stop 遷移）
  // この場合、defer 関数が JobExecution の最終状態を永続化します。
  // ループ内でエラーが発生して return された場合は、ここまで到達しません。

  // 正常終了の場合、nil を返す
  return nil
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
// func (j *WeatherItemProcessor) processSingleItem(...) ([]*entity.WeatherDataToStore, bool, error) { ... } // Processor は ItemProcessor にリネームされている可能性
// func (j *WeatherItemWriter) writeChunk(...) error { ... } // Writer は ItemWriter にリネームされている可能性
