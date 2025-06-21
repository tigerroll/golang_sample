package impl

import (
	"context"
	"fmt"
	"time" // time パッケージをインポート

	config "sample/src/main/go/batch/config"
	core "sample/src/main/go/batch/job/core"
	jsl "sample/src/main/go/batch/job/jsl" // jsl パッケージをインポート (JSL Decision の一時的な処理のため)
	jobListener "sample/src/main/go/batch/job/listener"
	repository "sample/src/main/go/batch/repository"
	exception "sample/src/main/go/batch/util/exception" // exception パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
	"gopkg.in/yaml.v3" // yaml パッケージをインポート (JSL Decision の一時的な処理のため)
)

// WeatherJob は天気予報データを取得・処理・保存するバッチジョブです。
// core.Job インターフェースを実装します。
type WeatherJob struct {
	jobRepository repository.JobRepository
	config        *config.Config
	jobListeners  []jobListener.JobExecutionListener
	flow          *core.FlowDefinition // ★ フロー定義を追加
}

// WeatherJob が core.Job インターフェースを満たすことを確認します。
var _ core.Job = (*WeatherJob)(nil)

// NewWeatherJob は新しい WeatherJob のインスタンスを作成します。
// flow パラメータは JSL からロードされたフロー定義を受け取ります。
func NewWeatherJob(
	jobRepository repository.JobRepository,
	cfg *config.Config,
	listeners []jobListener.JobExecutionListener, // リスナーリストを受け取るように変更
	flow *core.FlowDefinition, // ★ FlowDefinition を追加
) *WeatherJob {
	return &WeatherJob{
		jobRepository: jobRepository,
		config:        cfg,
		jobListeners:  listeners, // 受け取ったリスナーを設定
		flow:          flow,
	}
}

// RegisterListener は JobExecutionListener を登録します。
// NewWeatherJob でリスナーを受け取るように変更したため、このメソッドは不要になる可能性がありますが、
// 実行時に動的にリスナーを追加するユースケースのために残しておくこともできます。
func (j *WeatherJob) RegisterListener(l jobListener.JobExecutionListener) {
	j.jobListeners = append(j.jobListeners, l)
}

// notifyBeforeJob は登録されている JobExecutionListener の BeforeJob メソッドを呼び出します。
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
// ジョブ全体の実行フローを制御します。
// Context の完了チェックとリスタートロジックを追加します。
func (j *WeatherJob) Run(ctx context.Context, jobExecution *core.JobExecution) error {
	logger.Infof("Job '%s' (Execution ID: %s) の実行を開始します。", j.JobName(), jobExecution.ID)

	// Job 実行前処理の通知
	j.notifyBeforeJob(ctx, jobExecution)

	// Job 実行後処理 (defer で必ず実行)
	defer func() {
		// ジョブがまだ終了状態としてマークされていなければ完了としてマーク
		if !jobExecution.Status.IsFinished() {
			jobExecution.MarkAsCompleted()
		}

		// Job 実行後処理の通知
		j.notifyAfterJob(ctx, jobExecution)

		// WeatherRepository の Close は JobFactory または main 関数で行うべき。
	}()

	// フロー定義が存在しない場合はエラー
	if j.flow == nil {
		err := fmt.Errorf("ジョブ '%s' のフロー定義がありません", j.JobName())
		logger.Errorf("%v", err)
		jobExecution.MarkAsFailed(err)
		// defer で最終状態が永続化されるため、ここではエラーを返してループを抜ける
		return err
	}

	// フロー定義に基づいてステップを実行するロジック
	// リスタート時には jobExecution.CurrentStepName から開始
	currentElementName := j.flow.StartElement // フローの開始要素名を取得

	// リスタートの場合、前回の停止ステップから開始
	if jobExecution.CurrentStepName != "" &&
		(jobExecution.Status == core.JobStatusStarted || // 実行中に停止した場合
			jobExecution.Status == core.JobStatusFailed || // 実行中に失敗した場合
			jobExecution.Status == core.JobStatusStopped) { // 停止要求により停止した場合
		currentElementName = jobExecution.CurrentStepName
		logger.Infof("Job '%s' (Execution ID: %s) をステップ '%s' からリスタートします。",
			j.JobName(), jobExecution.ID, currentElementName)
		// TODO: リスタート時に ExecutionContext の状態を復元するロジックが必要
		//       これは JobOperator.Restart または Job.Run の開始部分で行うべき
	}


	// JobExecution の状態更新エラー変数をループ内で宣言 (スコープをループ全体にする)
	var updateJobExecErr error

	// フローの要素が存在する限りループ
	for {
		// Context の完了をチェック
		select {
		case <-ctx.Done():
			logger.Warnf("Context がキャンセルされたため、Job '%s' の実行を中断します: %v", j.JobName(), ctx.Err())
			// Context キャンセル時は JobExecution を STOPPED としてマークし、ExitStatus も設定
			jobExecution.Status = core.JobStatusStopped
			jobExecution.ExitStatus = core.ExitStatusStopped
			jobExecution.EndTime = time.Now()
			jobExecution.LastUpdated = time.Now()
			jobExecution.AddFailureException(ctx.Err()) // エラー情報を追加
			// defer で最終状態が永続化されるため、ここではエラーを返してループを抜ける
			return ctx.Err() // Context エラーを返す
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
			// JobExecution を FAILED としてマークし、defer で永続化
			jobExecution.MarkAsFailed(err)
			return err // エラーを返してジョブを失敗させる
		}

		var elementExitStatus core.ExitStatus // 実行した要素の ExitStatus
		var elementErr error                // 実行した要素が返したエラー


		// 要素のタイプに応じて処理を分岐
		switch element := currentElement.(type) {
		case core.Step:
			// ステップの場合の実行ロジック
			stepName := element.StepName()
			logger.Infof("ステップ '%s' の実行を開始します。", stepName)

			// StepExecution の作成と初期永続化
			// リスタートの場合、既存の StepExecution を再利用するのではなく、新しい StepExecution を作成します。
			// ただし、リスタート可能なステップの場合、前回の StepExecution の情報を引き継ぐ必要があります。
			// これは Step.Execute メソッド内で行うべき責務です（例: ChunkOrientedStep の CheckpointData）。
			stepExecution := core.NewStepExecution(stepName, jobExecution)
			// NewStepExecution 内で jobExecution.StepExecutions に追加済み

			// JobExecution の CurrentStepName を更新し永続化 (リスタート対応のため)
			jobExecution.CurrentStepName = stepName
			// updateJobExecErr 変数を使用 (ループ全体スコープで宣言済み)
			updateJobExecErr = j.jobRepository.UpdateJobExecution(ctx, jobExecution)
			if updateJobExecErr != nil {
				logger.Errorf("JobExecution (ID: %s) の CurrentStepName 更新に失敗しました: %v", jobExecution.ID, updateJobExecErr)
				jobExecution.AddFailureException(fmt.Errorf("JobExecution CurrentStepName 更新エラー: %w", updateJobExecErr))
				// 永続化エラーは記録するが、ステップ実行自体は進める
			}


			// StepExecution の初期状態を永続化
			saveErr := j.jobRepository.SaveStepExecution(ctx, stepExecution)
			if saveErr != nil {
				logger.Errorf("StepExecution (ID: %s) の初期永続化に失敗しました: %v", stepExecution.ID, saveErr)
				stepExecution.MarkAsFailed(fmt.Errorf("StepExecution の初期永続化に失敗しました: %w", saveErr))
				j.jobRepository.UpdateStepExecution(ctx, stepExecution) // 失敗状態を永続化
				jobExecution.MarkAsFailed(fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, saveErr))
				return fmt.Errorf("ステップ '%s' の実行に失敗しました: %w", stepName, saveErr)
			}
			logger.Debugf("StepExecution (ID: %s) を JobRepository に初期保存しました。", stepExecution.ID)

			// StepExecution の状態を Started に更新し、永続化
			stepExecution.MarkAsStarted()
			// updateStepExecErr 変数を宣言
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
			elementErr = element.Execute(ctx, jobExecution, stepExecution) // Context を渡す

			// ステップ実行完了後の StepExecution の最終状態を永続化
			// Execute メソッド内で StepExecution の最終状態 (Completed or Failed) は既に設定されています。
			// ここではその最終状態を JobRepository に保存します。
			// updateStepExecErr 変数を再利用
			updateStepExecErr = j.jobRepository.UpdateStepExecution(ctx, stepExecution)
			if updateStepExecErr != nil {
				logger.Errorf("StepExecution (ID: %s) の最終状態の更新に失敗しました: %v", stepExecution.ID, updateStepExecErr)
				stepExecution.AddFailureException(fmt.Errorf("StepExecution 最終状態更新エラー: %w", updateStepExecErr))
				if stepExecution.ExitStatus != core.ExitStatusFailed {
					stepExecution.ExitStatus = core.ExitStatusFailed
					stepExecution.Status = core.JobStatusFailed
				}
				if elementErr == nil {
					elementErr = fmt.Errorf("StepExecution 最終状態の永続化に失敗しました: %w", updateStepExecErr)
				} else {
					elementErr = fmt.Errorf("ステップ実行エラー (%w), StepExecution 最終状態永続化エラー (%w)", elementErr, updateStepExecErr)
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


		case core.Decision:
			// Decision の実行ロジック
			decisionName := element.DecisionName()
			logger.Infof("Decision '%s' の実行を開始します。", decisionName)

			// DecisionExecution の作成 (StepExecution と同様のライフサイクル管理が必要であれば)
			// 現状は StepExecution を流用するか、専用の DecisionExecution を定義する
			// ここでは簡易的に StepExecution を使用
			decisionExecution := core.NewStepExecution(decisionName, jobExecution) // 仮にStepExecutionを使用
			if saveErr := j.jobRepository.SaveStepExecution(ctx, decisionExecution); saveErr != nil {
				logger.Errorf("DecisionExecution (ID: %s) の初期永続化に失敗しました: %v", decisionExecution.ID, saveErr)
				jobExecution.MarkAsFailed(fmt.Errorf("Decision '%s' の実行に失敗しました: %w", decisionName, saveErr))
				return fmt.Errorf("Decision '%s' の実行に失敗しました: %w", decisionName, saveErr)
			}
			decisionExecution.MarkAsStarted()
			j.jobRepository.UpdateStepExecution(ctx, decisionExecution)

			// Decide メソッドを実行
			exitStatus, err := element.Decide(ctx, jobExecution, decisionExecution)
			if err != nil {
				logger.Errorf("Decision '%s' の実行中にエラーが発生しました: %v", decisionName, err)
				decisionExecution.MarkAsFailed(err)
				jobExecution.AddFailureException(err)
			} else {
				decisionExecution.MarkAsCompleted()
			}

			if updateErr := j.jobRepository.UpdateStepExecution(ctx, decisionExecution); updateErr != nil {
				logger.Errorf("DecisionExecution (ID: %s) の最終状態の更新に失敗しました: %v", decisionExecution.ID, updateErr)
				jobExecution.AddFailureException(updateErr)
				if err == nil { // Decide 自体はエラーでなくても、永続化でエラーならジョブ失敗
					err = fmt.Errorf("DecisionExecution 最終状態の永続化に失敗しました: %w", updateErr)
				}
			}

			elementExitStatus = exitStatus
			elementErr = err

			if elementErr != nil {
				logger.Errorf("Decision '%s' (Execution ID: %s) がエラーで完了しました: %v",
					decisionName, decisionExecution.ID, elementErr)
			} else {
				logger.Infof("Decision '%s' (Execution ID: %s) が正常に完了しました。結果: %s",
					decisionName, decisionExecution.ID, elementExitStatus)
			}

		case jsl.Decision: // JSL Decision が直接格納されている場合 (一時的な措置)
			// JSL の Decision は core.Decision インターフェースを実装していないため、
			// ここで直接処理することはできません。
			// jsl.ConvertJSLToCoreFlow で core.Decision の具体的な実装に変換されるべきです。
			// 現状は、JSL の Decision が直接 Elements に入っている場合、エラーとするか、
			// 暫定的に COMPLETED として次の遷移に進むようにします。
			logger.Warnf("JSL Decision '%s' が直接フローに格納されています。適切な core.Decision への変換が必要です。暫定的に COMPLETED とします。", element.ID)
			elementExitStatus = core.ExitStatusCompleted // 仮の処理
			elementErr = nil
		default:
			// 未知の要素タイプの場合
			err := fmt.Errorf("フロー定義に未知の要素タイプ '%T' が含まれています (要素名: '%s')", currentElement, currentElementName)
			logger.Errorf("%v", err)
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
			jobExecution.MarkAsFailed(err)
			return err // エラーを返してジョブを失敗させる
		}

		// 見つかった遷移ルールを評価
		if transition.End {
			// End 遷移: ジョブを完了としてマークし、ループを終了
			logger.Infof("遷移ルールにより Job '%s' (Execution ID: %s) を終了します。最終 ExitStatus: %s",
				j.JobName(), jobExecution.ID, core.ExitStatusCompleted) // End 遷移は通常 COMPLETED
			jobExecution.Status = core.JobStatusCompleted
			jobExecution.ExitStatus = core.ExitStatusCompleted
			currentElementName = "" // ループを終了するための条件を設定
		} else if transition.Fail {
			// Fail 遷移: ジョブを失敗としてマークし、ループを終了
			logger.Errorf("遷移ルールにより Job '%s' (Execution ID: %s) を失敗とします。最終 ExitStatus: %s",
				j.JobName(), jobExecution.ID, core.ExitStatusFailed) // Fail 遷移は通常 FAILED
			jobExecution.Status = core.JobStatusFailed
			jobExecution.ExitStatus = core.ExitStatusFailed
			jobExecution.AddFailureException(fmt.Errorf("遷移ルールによるジョブ失敗 (ExitStatus: %s)", core.ExitStatusFailed))
			currentElementName = "" // ループを終了するための条件を設定
		} else if transition.Stop {
			// Stop 遷移: ジョブを停止としてマークし、ループを終了
			logger.Warnf("遷移ルールにより Job '%s' (Execution ID: %s) を停止します。",
				j.JobName(), jobExecution.ID)
			jobExecution.Status = core.JobStatusStopped // Stop 遷移の場合、Status は STOPPED
			jobExecution.ExitStatus = core.ExitStatusStopped // ExitStatus は通常 STOPPED
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
			jobExecution.MarkAsFailed(err)
			return err // エラーを返してジョブを失敗させる
		}

		// 要素の実行でエラーが発生した場合 (elementErr != nil) でも、遷移ルールが見つかればそのルールに従います。
		// ただし、elementErr は JobExecution の Failureliye に追加されています。
		if elementErr != nil {
			// elementErr は StepExecution.Failureliye に追加済みだが、JobExecution にも追加
			jobExecution.AddFailureException(elementErr)
		}

		// 各要素の処理と遷移評価の後、JobExecution の状態を JobRepository に更新
		// これにより、JobExecution の CurrentStepName や Status が永続化され、リスタート時に利用できます。
		// updateJobExecErr 変数を使用 (ループ全体スコープで宣言済み)
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
func (j *WeatherJob) GetFlow() *core.FlowDefinition {
	return j.flow
}
