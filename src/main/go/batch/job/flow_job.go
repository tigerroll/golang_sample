package job

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid" // UUID生成のためにインポート

	core "sample/src/main/go/batch/job/core"
	jobListener "sample/src/main/go/batch/job/listener" // jobListener パッケージをインポート
	repository "sample/src/main/go/batch/repository"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
)

// FlowJob は JSL で定義されたフローに基づいてジョブを実行する core.Job の実装です。
type FlowJob struct {
	id            string
	name          string
	flow          *core.FlowDefinition
	jobRepository repository.JobRepository
	jobListeners  []jobListener.JobExecutionListener
}

// FlowJob が core.Job インターフェースを満たすことを確認します。
var _ core.Job = (*FlowJob)(nil)

// NewFlowJob は新しい FlowJob のインスタンスを作成します。
func NewFlowJob(
	id string,
	name string,
	flow *core.FlowDefinition,
	jobRepository repository.JobRepository,
	jobListeners []jobListener.JobExecutionListener,
) *FlowJob {
	return &FlowJob{
		id:            id,
		name:          name,
		flow:          flow,
		jobRepository: jobRepository,
		jobListeners:  jobListeners,
	}
}

// JobID はジョブのIDを返します。
func (j *FlowJob) JobID() string {
	return j.id
}

// JobName はジョブ名を返します。
func (j *FlowJob) JobName() string {
	return j.name
}

// notifyBeforeJob は登録されている JobExecutionListener の BeforeJob メソッドを呼び出します。
func (j *FlowJob) notifyBeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
	for _, l := range j.jobListeners {
		l.BeforeJob(ctx, jobExecution)
	}
}

// notifyAfterJob は登録されている JobExecutionListener の AfterJob メソッドを呼び出します。
func (j *FlowJob) notifyAfterJob(ctx context.Context, jobExecution *core.JobExecution) {
	for _, l := range j.jobListeners {
		l.AfterJob(ctx, jobExecution)
	}
}

// Run はジョブの実行ロジックを定義します。
// ジョブのフロー定義に基づいてステップやデシジョンを順次実行します。
func (j *FlowJob) Run(ctx context.Context, jobExecution *core.JobExecution, jobParameters core.JobParameters) error {
	logger.Infof("ジョブ '%s' (Execution ID: %s) を開始します。", j.name, jobExecution.ID)

	// JobExecution の開始時刻を設定し、状態をマーク
	jobExecution.StartTime = time.Now()
	jobExecution.MarkAsStarted() // Status = Started

	// JobExecution の初期状態を保存
	// JobOperator.Start/Restart で既に保存されているはずだが、念のため
	if err := j.jobRepository.UpdateJobExecution(ctx, jobExecution); err != nil {
		logger.Errorf("ジョブ '%s': JobExecution の初期状態の更新に失敗しました: %v", j.name, err)
		jobExecution.AddFailureException(err)
		jobExecution.MarkAsFailed(err)
		return exception.NewBatchError(j.name, "JobExecution の初期状態更新エラー", err, false, false)
	}

	// ジョブ実行前処理の通知
	j.notifyBeforeJob(ctx, jobExecution)

	// ジョブ実行後処理 (defer で必ず実行)
	defer func() {
		// ジョブの終了時刻を設定
		jobExecution.EndTime = time.Now()

		// ジョブ実行後処理の通知
		j.notifyAfterJob(ctx, jobExecution)

		// 最終的な JobExecution の状態を保存
		if err := j.jobRepository.UpdateJobExecution(ctx, jobExecution); err != nil {
			logger.Errorf("ジョブ '%s': JobExecution の最終状態の更新に失敗しました: %v", j.name, err)
			// ここでエラーが発生しても、ジョブの実行自体は完了しているため、ログに留める
			// ただし、リスタート情報が正しく保存されない可能性がある
		}
		logger.Infof("ジョブ '%s' (Execution ID: %s) が終了しました。最終ステータス: %s, 終了ステータス: %s",
			j.name, jobExecution.ID, jobExecution.Status, jobExecution.ExitStatus)
	}()

	// フローの開始要素から実行を開始
	currentElementID := j.flow.StartElement
	if jobExecution.CurrentStepName != "" && jobExecution.Status == core.BatchStatusFailed {
		// リスタートの場合、中断したステップから再開
		logger.Infof("ジョブ '%s' はステップ '%s' からリスタートします。", j.name, jobExecution.CurrentStepName)
		currentElementID = jobExecution.CurrentStepName
	}

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context がキャンセルされたため、ジョブ '%s' の実行を中断します: %v", j.name, ctx.Err())
			jobExecution.AddFailureException(ctx.Err())
			jobExecution.MarkAsFailed(ctx.Err())
			return ctx.Err()
		default:
		}

		if currentElementID == "" {
			// フローの終端に達したか、不正な遷移
			if jobExecution.Status == core.BatchStatusStarted {
				jobExecution.MarkAsCompleted()
				logger.Infof("ジョブ '%s' のフローが正常に完了しました。", j.name)
			}
			break
		}

		element, ok := j.flow.Elements[currentElementID]
		if !ok {
			err := exception.NewBatchErrorf(j.name, "フロー要素 '%s' が見つかりません", currentElementID)
			logger.Errorf("ジョブ '%s': %v", j.name, err)
			jobExecution.AddFailureException(err)
			jobExecution.MarkAsFailed(err)
			return err
		}

		logger.Debugf("ジョブ '%s': フロー要素 '%s' を実行します。", j.name, currentElementID)

		var nextElementID string
		var elementExitStatus core.ExitStatus
		var elementErr error

		switch elem := element.(type) {
		case core.Step:
			// ステップの実行
			stepName := elem.StepName()
			jobExecution.CurrentStepName = stepName // 現在実行中のステップ名を JobExecution に設定

			// リスタートの場合、既存の StepExecution を取得
			var stepExecution *core.StepExecution
			if jobExecution.Status == core.BatchStatusFailed && jobExecution.CurrentStepName == stepName {
				// 失敗したジョブがこのステップで中断した場合、その StepExecution を再利用
				// 最新の JobExecution に紐づく StepExecution を取得
				// TODO: 厳密には、JobExecution.StepExecutions から該当するものを探す
				// 現状は FindStepExecutionsByJobExecutionID で全て取得し、その中から最新のものを探す
				// または、JobExecution.CurrentStepName が設定されている場合は、そのステップの最新の StepExecution を取得する
				// ここでは、JobExecution に既にロードされている StepExecutions から探す
				for _, se := range jobExecution.StepExecutions {
					if se.StepName == stepName {
						stepExecution = se
						logger.Infof("ジョブ '%s': ステップ '%s' の既存の StepExecution (ID: %s) を再利用します。", j.name, stepName, stepExecution.ID)
						break
					}
				}
			}

			if stepExecution == nil {
				// 新しい StepExecution を作成
				stepExecutionID := uuid.New().String()
				stepExecution = core.NewStepExecution(stepExecutionID, jobExecution, stepName)
				jobExecution.AddStepExecution(stepExecution) // JobExecution に StepExecution を追加
				// 新しい StepExecution を保存
				if err := j.jobRepository.SaveStepExecution(ctx, stepExecution); err != nil {
					logger.Errorf("ジョブ '%s': StepExecution (ID: %s) の保存に失敗しました: %v", j.name, stepExecution.ID, err)
					jobExecution.AddFailureException(err)
					jobExecution.MarkAsFailed(err)
					return exception.NewBatchError(j.name, "StepExecution の保存エラー", err, false, false)
				}
				logger.Infof("ジョブ '%s': 新しい StepExecution (ID: %s) を作成しました。", j.name, stepExecution.ID)
			}

			elementErr = elem.Execute(ctx, jobExecution, stepExecution) // StepExecution を渡す
			elementExitStatus = stepExecution.ExitStatus                // ステップの終了ステータスを取得

			if elementErr != nil {
				logger.Errorf("ジョブ '%s': ステップ '%s' の実行中にエラーが発生しました: %v", j.name, stepName, elementErr)
				// StepExecution は既に失敗状態になっているはず
				jobExecution.AddFailureException(elementErr)
				jobExecution.MarkAsFailed(elementErr)
				// エラーが発生した場合でも、遷移ルールを評価して Fail 遷移を探す
				nextElementID, _ = j.flow.GetNextElementID(currentElementID, elementExitStatus, true) // isError = true
				if nextElementID == "" {
					// Fail 遷移が見つからない場合、ジョブ全体を失敗として終了
					return elementErr
				}
			} else {
				// ステップが正常終了した場合
				logger.Infof("ジョブ '%s': ステップ '%s' が正常に完了しました。ExitStatus: %s", j.name, stepName, elementExitStatus)
				nextElementID, _ = j.flow.GetNextElementID(currentElementID, elementExitStatus, false) // isError = false
			}

		case core.Decision:
			// デシジョンの実行
			decisionName := elem.ID()
			decisionResult, err := elem.Decide(ctx, jobExecution, jobParameters)
			elementExitStatus = decisionResult
			elementErr = err

			if elementErr != nil {
				logger.Errorf("ジョブ '%s': デシジョン '%s' の実行中にエラーが発生しました: %v", j.name, decisionName, elementErr)
				jobExecution.AddFailureException(elementErr)
				jobExecution.MarkAsFailed(elementErr)
				// エラーが発生した場合でも、遷移ルールを評価して Fail 遷移を探す
				nextElementID, _ = j.flow.GetNextElementID(currentElementID, elementExitStatus, true) // isError = true
				if nextElementID == "" {
					// Fail 遷移が見つからない場合、ジョブ全体を失敗として終了
					return elementErr
				}
			} else {
				logger.Infof("ジョブ '%s': デシジョン '%s' が完了しました。結果: %s", j.name, decisionName, decisionResult)
				nextElementID, _ = j.flow.GetNextElementID(currentElementID, elementExitStatus, false) // isError = false
			}

		default:
			err := exception.NewBatchErrorf(j.name, "不明なフロー要素の型: %T (ID: %s)", elem, currentElementID)
			logger.Errorf("ジョブ '%s': %v", j.name, err)
			jobExecution.AddFailureException(err)
			jobExecution.MarkAsFailed(err)
			return err
		}

		// 遷移先の要素IDを更新
		currentElementID = nextElementID

		// 遷移ルールが End, Fail, Stop を指示した場合の処理
		transitionRule, found := j.flow.GetTransitionRule(element.ID(), elementExitStatus, elementErr != nil)
		if found {
			if transitionRule.End {
				logger.Infof("ジョブ '%s': フロー要素 '%s' から 'End' 遷移が指示されました。ジョブを完了します。", j.name, element.ID())
				jobExecution.MarkAsCompleted()
				break
			}
			if transitionRule.Fail {
				logger.Errorf("ジョブ '%s': フロー要素 '%s' から 'Fail' 遷移が指示されました。ジョブを失敗として終了します。", j.name, element.ID())
				jobExecution.MarkAsFailed(fmt.Errorf("explicit fail transition from %s", element.ID()))
				break
			}
			if transitionRule.Stop {
				logger.Infof("ジョブ '%s': フロー要素 '%s' から 'Stop' 遷移が指示されました。ジョブを停止します。", j.name, element.ID())
				jobExecution.MarkAsStopped()
				break
			}
		}

		// 次の要素が見つからない場合 (End, Fail, Stop 遷移がない場合)
		if currentElementID == "" {
			if elementErr == nil {
				// 正常にフローの終端に達した
				logger.Infof("ジョブ '%s': フローの終端に達しました。ジョブを完了します。", j.name)
				jobExecution.MarkAsCompleted()
			} else {
				// エラーが発生したが、次の遷移先が見つからなかった
				logger.Errorf("ジョブ '%s': エラー発生後、次の遷移先が見つかりませんでした。ジョブを失敗として終了します。", j.name)
				jobExecution.MarkAsFailed(elementErr)
			}
			break
		}
	}

	// ジョブの最終的な ExitStatus を設定
	if jobExecution.Status == core.BatchStatusCompleted {
		jobExecution.ExitStatus = core.ExitStatusCompleted
	} else if jobExecution.Status == core.BatchStatusFailed {
		jobExecution.ExitStatus = core.ExitStatusFailed
	} else if jobExecution.Status == core.BatchStatusStopped {
		jobExecution.ExitStatus = core.ExitStatusStopped
	} else {
		// 未知の状態の場合、失敗とみなす
		jobExecution.ExitStatus = core.ExitStatusUnknown
	}

	logger.Infof("ジョブ '%s' (Execution ID: %s) の実行が完了しました。", j.name, jobExecution.ID)
	return nil
}
