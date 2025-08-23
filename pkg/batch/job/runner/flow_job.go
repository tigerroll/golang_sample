package runner

import (
	"context"
	"errors"
	"fmt"
	"sync" // ★ 追加: Split の並列実行のため
	"time"

	"github.com/google/uuid"

	core "sample/pkg/batch/job/core"
	// jobListener "sample/pkg/batch/job/listener" // Moved to core
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// FlowJob は JSL で定義されたフローに基づいてジョブを実行する core.Job の実装です。
type FlowJob struct {
	id            string
	name          string
	flow          *core.FlowDefinition // core.FlowDefinition を使用
	jobRepository job.JobRepository // job.JobRepository に変更
	jobListeners  []core.JobExecutionListener // ★ 変更: core.JobExecutionListener を使用
	// JobParametersIncrementer は JobLauncher が使用するため、FlowJob には直接持たせない
}

// FlowJob が core.Job インターフェースを満たすことを確認します。
var _ core.Job = (*FlowJob)(nil)

// NewFlowJob は新しい FlowJob のインスタンスを作成します。
func NewFlowJob(
	id string,
	name string,
	flow *core.FlowDefinition, // core.FlowDefinition を使用
	jobRepository job.JobRepository, // job.JobRepository を受け取る
	jobListeners []core.JobExecutionListener, // ★ 変更: core.JobExecutionListener を使用
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

// GetFlow はジョブのフロー定義を返します。
func (j *FlowJob) GetFlow() *core.FlowDefinition {
	return j.flow
}

// ValidateParameters はジョブパラメータのバリデーションを行います。
// 現時点では常に nil を返しますが、ジョブ固有のバリデーションロジックをここに追加できます。
func (j *FlowJob) ValidateParameters(params core.JobParameters) error {
	logger.Debugf("ジョブ '%s': JobParameters のバリデーションを実行するよ。Parameters: %+v", j.name, params.Params)
	// TODO: ここにジョブ固有のバリデーションロジックを実装
	// 例:
	// if _, ok := params.GetString("required_param"); !ok {
	// 	return exception.NewBatchErrorf(j.name, "必須パラメータ 'required_param' が見つかりません")
	// }
	return nil
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
	logger.Infof("ジョブ '%s' (Execution ID: %s) を始めるよ。", j.name, jobExecution.ID)

	// ジョブ実行前処理の通知
	j.notifyBeforeJob(ctx, jobExecution)

	// ジョブ実行後処理 (defer で必ず実行)
	defer func() {
		// ジョブの終了時刻を設定
		jobExecution.EndTime = time.Now()

		// ジョブ実行後処理の通知
		j.notifyAfterJob(ctx, jobExecution)

		logger.Infof("ジョブ '%s' (Execution ID: %s) が終了したよ。最終ステータス: %s, 終了ステータス: %s",
			j.name, jobExecution.ID, jobExecution.Status, jobExecution.ExitStatus)
	}()

	// フローの開始要素から実行を開始
	currentElementID := j.flow.StartElement
	if jobExecution.CurrentStepName != "" && jobExecution.Status == core.BatchStatusFailed {
		// リスタートの場合、中断したステップから再開
		logger.Infof("ジョブ '%s' はステップ '%s' からリスタートするよ。", j.name, jobExecution.CurrentStepName)
		currentElementID = jobExecution.CurrentStepName
	}

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context がキャンセルされたため、ジョブ '%s' の実行を中断するよ: %v", j.name, ctx.Err())
			jobExecution.AddFailureException(ctx.Err())
			jobExecution.MarkAsStopped()
			return ctx.Err()
		default:
		}

		if currentElementID == "" {
			// フローの終端に達したか、不正な遷移
			if jobExecution.Status == core.BatchStatusStarted {
				jobExecution.MarkAsCompleted()
				logger.Infof("ジョブ '%s' のフローが正常に完了したよ。", j.name)
			}
			break
		}

		element, ok := j.flow.Elements[currentElementID]
		if !ok {
			err := exception.NewBatchErrorf(j.name, "フロー要素 '%s' が見つからないよ", currentElementID)
			logger.Errorf("ジョブ '%s': %v", j.name, err)
			jobExecution.AddFailureException(err)
			jobExecution.MarkAsFailed(err)
			return err
		}

		logger.Debugf("ジョブ '%s': フロー要素 '%s' を実行するよ。", j.name, currentElementID)

		var elementExitStatus core.ExitStatus
		var elementErr error

		switch elem := element.(type) {
		case core.Step:
			// ステップの実行
			stepName := elem.StepName()
			jobExecution.CurrentStepName = stepName // 現在実行中のステップ名を JobExecution に設定

			// リスタートの場合、既存の StepExecution を取得
			var stepExecution *core.StepExecution
			for _, se := range jobExecution.StepExecutions {
				if se.StepName == stepName && se.Status == core.BatchStatusFailed {
					stepExecution = se
					logger.Infof("ジョブ '%s': ステップ '%s' の既存の StepExecution (ID: %s) を再利用するよ。", j.name, stepName, stepExecution.ID)
					break
				}
			}

			if stepExecution == nil {
				// 新しい StepExecution を作成
				stepExecutionID := uuid.New().String()
				stepExecution = core.NewStepExecution(stepExecutionID, jobExecution, stepName)
				jobExecution.AddStepExecution(stepExecution)
				// 新しい StepExecution を保存
				if err := j.jobRepository.SaveStepExecution(ctx, stepExecution); err != nil {
					logger.Errorf("ジョブ '%s': StepExecution (ID: %s) の保存に失敗しました: %v", j.name, stepExecution.ID, err)
					jobExecution.AddFailureException(err)
					jobExecution.MarkAsFailed(err)
					return exception.NewBatchError(j.name, "StepExecution の保存エラー", err, false, false)
				}
				logger.Infof("ジョブ '%s': 新しい StepExecution (ID: %s) を作成したよ。", j.name, stepExecution.ID)
			}

			elementErr = elem.Execute(ctx, jobExecution, stepExecution)
			elementExitStatus = stepExecution.ExitStatus

			if elementErr != nil {
				logger.Errorf("ジョブ '%s': ステップ '%s' の実行中にエラーが発生したよ: %v", j.name, stepName, elementErr)
				jobExecution.AddFailureException(elementErr)
				if errors.Is(elementErr, context.Canceled) {
					jobExecution.MarkAsStopped()
				} else {
					jobExecution.MarkAsFailed(elementErr)
				}
			} else {
				logger.Infof("ジョブ '%s': ステップ '%s' が正常に完了したよ。ExitStatus: %s", j.name, stepName, elementExitStatus)
			}

		case core.Decision:
			// デシジョンの実行
			decisionName := elem.ID()
			decisionResult, err := elem.Decide(ctx, jobExecution, jobParameters)
			elementExitStatus = decisionResult
			elementErr = err

			if elementErr != nil {
				logger.Errorf("ジョブ '%s': デシジョン '%s' の実行中にエラーが発生したよ: %v", j.name, decisionName, elementErr)
				jobExecution.AddFailureException(elementErr)
				if errors.Is(elementErr, context.Canceled) {
					jobExecution.MarkAsStopped()
				} else {
					jobExecution.MarkAsFailed(elementErr)
				}
			} else {
				logger.Infof("ジョブ '%s': デシジョン '%s' が完了したよ。結果: %s", j.name, decisionName, decisionResult)
			}

		case core.Split: // ★ 追加: Split の処理
			splitName := elem.ID()
			logger.Infof("ジョブ '%s': Split '%s' を実行するよ。並列ステップ数: %d", j.name, splitName, len(elem.Steps()))

			var wg sync.WaitGroup
			splitErrors := make(chan error, len(elem.Steps()))
			splitExitStatuses := make(chan core.ExitStatus, len(elem.Steps()))

			for _, s := range elem.Steps() {
				wg.Add(1)
				go func(splitStep core.Step) {
					defer wg.Done()
					stepName := splitStep.StepName()
					logger.Infof("ジョブ '%s': Split 内のステップ '%s' を開始するよ。", j.name, stepName)

					// Split内のステップもStepExecutionを管理
					stepExecutionID := uuid.New().String()
					splitStepExecution := core.NewStepExecution(stepExecutionID, jobExecution, stepName)
					jobExecution.AddStepExecution(splitStepExecution) // JobExecutionにStepExecutionを追加

					if err := j.jobRepository.SaveStepExecution(ctx, splitStepExecution); err != nil {
						logger.Errorf("ジョブ '%s': Split 内の StepExecution (ID: %s) の保存に失敗しました: %v", j.name, splitStepExecution.ID, err)
						splitErrors <- exception.NewBatchError(j.name, fmt.Sprintf("Split 内の StepExecution '%s' の保存エラー", stepName), err, false, false)
						splitExitStatuses <- core.ExitStatusFailed
						return
					}

					err := splitStep.Execute(ctx, jobExecution, splitStepExecution)
					if err != nil {
						logger.Errorf("ジョブ '%s': Split 内のステップ '%s' の実行中にエラーが発生したよ: %v", j.name, stepName, err)
						splitErrors <- err
					} else {
						logger.Infof("ジョブ '%s': Split 内のステップ '%s' が正常に完了したよ。ExitStatus: %s", j.name, stepName, splitStepExecution.ExitStatus)
					}
					splitExitStatuses <- splitStepExecution.ExitStatus
				}(s)
			}
			wg.Wait()
			close(splitErrors)
			close(splitExitStatuses)

			// Split の結果を集約
			allStepsCompleted := true
			for err := range splitErrors {
				if err != nil {
					elementErr = errors.Join(elementErr, err) // 全てのエラーを結合
					allStepsCompleted = false
				}
			}

			// Split の ExitStatus を決定 (一つでも失敗があれば FAILED)
			if elementErr != nil {
				elementExitStatus = core.ExitStatusFailed
			} else {
				elementExitStatus = core.ExitStatusCompleted
			}
			logger.Infof("ジョブ '%s': Split '%s' の実行が完了したよ。結果: %s", j.name, splitName, elementExitStatus)

		default:
			err := exception.NewBatchErrorf(j.name, "不明なフロー要素の型だよ: %T (ID: %s)", elem, currentElementID)
			logger.Errorf("ジョブ '%s': %v", j.name, err)
			jobExecution.AddFailureException(err)
			jobExecution.MarkAsFailed(err)
			return err
		}

		// 遷移ルールを評価して次の要素を決定
		transitionRule, found := j.flow.GetTransitionRule(element.ID(), elementExitStatus, elementErr != nil)
		if !found {
			if elementErr == nil {
				logger.Infof("ジョブ '%s': フロー要素 '%s' からの遷移ルールが見つからないよ。ジョブを完了するよ。", j.name, element.ID())
				jobExecution.MarkAsCompleted()
			} else {
				logger.Errorf("ジョブ '%s': フロー要素 '%s' でエラーが発生したけど、適切な遷移ルールが見つからないよ。ジョブを失敗として終了するよ。", j.JobName(), element.ID())
				jobExecution.MarkAsFailed(elementErr)
			}
			break
		}

		// 遷移ルールが End, Fail, Stop を指示した場合の処理
		if transitionRule.End {
			logger.Infof("ジョブ '%s': フロー要素 '%s' から 'End' 遷移が指示されたよ。ジョブを完了するよ。", j.JobName(), element.ID())
			jobExecution.MarkAsCompleted()
			break
		}
		if transitionRule.Fail {
			logger.Errorf("ジョブ '%s': フロー要素 '%s' から 'Fail' 遷移が指示されたよ。ジョブを失敗として終了するよ。", j.JobName(), element.ID())
			jobExecution.MarkAsFailed(fmt.Errorf("explicit fail transition from %s", element.ID()))
			break
		}
		if transitionRule.Stop {
			logger.Infof("ジョブ '%s': フロー要素 '%s' から 'Stop' 遷移が指示されたよ。ジョブを停止するよ。", j.JobName(), element.ID())
			jobExecution.MarkAsStopped()
			break
		}

		// 次の要素IDを更新
		currentElementID = transitionRule.To
	}

	logger.Infof("ジョブ '%s' (Execution ID: %s) の実行が完了したよ。", j.JobName(), jobExecution.ID)
	return nil
}
