package impl

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"sample/src/main/go/batch/config"
	core "sample/src/main/go/batch/job/core"
	jobListener "sample/src/main/go/batch/job/listener"
	repository "sample/src/main/go/batch/repository"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
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

// JobName はジョブ名を返します。
func (j *WeatherJob) JobName() string {
	return j.config.Batch.JobName
}

// GetFlow はジョブのフロー定義を返します。
func (j *WeatherJob) GetFlow() *core.FlowDefinition {
	return j.flow
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

// Run はジョブの実行ロジックを定義します。
// ジョブのフロー定義に基づいてステップやデシジョンを順次実行します。
func (j *WeatherJob) Run(ctx context.Context, jobExecution *core.JobExecution, jobParameters core.JobParameters) error { // ★ 修正: jobParameters を追加
	logger.Infof("ジョブ '%s' (Execution ID: %s) 実行開始！", j.JobName(), jobExecution.ID)

	// JobExecution の開始時刻を設定し、状態をマーク
	jobExecution.StartTime = time.Now()
	jobExecution.MarkAsStarted()

	// JobExecution の初期状態を保存
	if err := j.jobRepository.UpdateJobExecution(ctx, jobExecution); err != nil {
		logger.Errorf("ジョブ '%s': JobExecution の初期状態の更新に失敗しました: %v", j.JobName(), err)
		jobExecution.AddFailureException(err)
		jobExecution.MarkAsFailed(err)
		return exception.NewBatchError(j.JobName(), "JobExecution の初期状態更新エラー", err, false, false)
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
			logger.Errorf("ジョブ '%s': JobExecution の最終状態の更新に失敗しました: %v", j.JobName(), err)
		}
		logger.Infof("ジョブ '%s' (Execution ID: %s) が終了しました。最終ステータス: %s, 終了ステータス: %s",
			j.JobName(), jobExecution.ID, jobExecution.Status, jobExecution.ExitStatus)
	}()

	// フローの開始要素から実行を開始
	currentElementID := j.flow.StartElement
	if jobExecution.CurrentStepName != "" && jobExecution.Status == core.BatchStatusFailed { // ★ 修正: core.BatchStatusFailed を使用
		// リスタートの場合、中断したステップから再開
		logger.Infof("ジョブ '%s' はステップ '%s' からリスタートします。", j.JobName(), jobExecution.CurrentStepName)
		currentElementID = jobExecution.CurrentStepName
	}

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context がキャンセルされたため、ジョブ '%s' の実行を中断します: %v", j.JobName(), ctx.Err())
			jobExecution.AddFailureException(ctx.Err())
			jobExecution.MarkAsFailed(ctx.Err())
			return ctx.Err()
		default:
		}

		if currentElementID == "" {
			// フローの終端に達したか、不正な遷移
			if jobExecution.Status == core.BatchStatusStarted { // ★ 修正: core.BatchStatusStarted を使用
				jobExecution.MarkAsCompleted()
				logger.Infof("ジョブ '%s' のフローが正常に完了しました。", j.JobName())
			}
			break
		}

		element, ok := j.flow.Elements[currentElementID]
		if !ok {
			err := exception.NewBatchErrorf(j.JobName(), "フロー要素 '%s' が見つかりません", currentElementID)
			logger.Errorf("ジョブ '%s': %v", j.JobName(), err)
			jobExecution.AddFailureException(err)
			jobExecution.MarkAsFailed(err)
			return err
		}

		logger.Debugf("ジョブ '%s': フロー要素 '%s' を実行します。", j.JobName(), currentElementID)

		var elementExitStatus core.ExitStatus
		var elementErr error

		switch elem := element.(type) {
		case core.Step:
			// ステップの実行
			stepName := elem.StepName()
			jobExecution.CurrentStepName = stepName // 現在実行中のステップ名を JobExecution に設定

			// リスタートの場合、既存の StepExecution を取得
			var stepExecution *core.StepExecution
			// 失敗したジョブがこのステップで中断した場合、その StepExecution を再利用
			for _, se := range jobExecution.StepExecutions {
				if se.StepName == stepName && se.Status == core.BatchStatusFailed { // ★ 修正: core.BatchStatusFailed を使用
					stepExecution = se
					logger.Infof("ジョブ '%s': ステップ '%s' の既存の StepExecution (ID: %s) を再利用します。", j.JobName(), stepName, stepExecution.ID)
					break
				}
			}

			if stepExecution == nil {
				// 新しい StepExecution を作成
				stepExecutionID := uuid.New().String()
				stepExecution = core.NewStepExecution(stepExecutionID, jobExecution, stepName) // ★ 修正: stepExecutionID と stepName を渡す
				jobExecution.AddStepExecution(stepExecution)                                 // ★ 修正: AddStepExecution を呼び出す
				// 新しい StepExecution を保存
				if err := j.jobRepository.SaveStepExecution(ctx, stepExecution); err != nil {
					logger.Errorf("ジョブ '%s': StepExecution (ID: %s) の保存に失敗しました: %v", j.JobName(), stepExecution.ID, err)
					jobExecution.AddFailureException(err)
					jobExecution.MarkAsFailed(err)
					return exception.NewBatchError(j.JobName(), "StepExecution の保存エラー", err, false, false)
				}
				logger.Infof("ジョブ '%s': 新しい StepExecution (ID: %s) を作成しました。", j.JobName(), stepExecution.ID)
			}

			elementErr = elem.Execute(ctx, jobExecution, stepExecution) // StepExecution を渡す
			elementExitStatus = stepExecution.ExitStatus                // ステップの終了ステータスを取得

			if elementErr != nil {
				logger.Errorf("ジョブ '%s': ステップ '%s' の実行中にエラーが発生しました: %v", j.JobName(), stepName, elementErr)
				jobExecution.AddFailureException(elementErr)
				jobExecution.MarkAsFailed(elementErr)
			} else {
				logger.Infof("ジョブ '%s': ステップ '%s' が正常に完了しました。ExitStatus: %s", j.JobName(), stepName, elementExitStatus)
			}

		case core.Decision:
			// デシジョンの実行
			decisionName := elem.ID()
			decisionResult, err := elem.Decide(ctx, jobExecution, jobParameters) // ★ 修正: jobParameters を渡す
			elementExitStatus = decisionResult
			elementErr = err

			if elementErr != nil {
				logger.Errorf("ジョブ '%s': デシジョン '%s' の実行中にエラーが発生しました: %v", j.JobName(), decisionName, elementErr)
				jobExecution.AddFailureException(elementErr)
				jobExecution.MarkAsFailed(elementErr)
			} else {
				logger.Infof("ジョブ '%s': デシジョン '%s' が完了しました。結果: %s", j.JobName(), decisionName, decisionResult)
			}

		default:
			err := exception.NewBatchErrorf(j.JobName(), "不明なフロー要素の型: %T (ID: %s)", elem, currentElementID)
			logger.Errorf("ジョブ '%s': %v", j.JobName(), err)
			jobExecution.AddFailureException(err)
			jobExecution.MarkAsFailed(err)
			return err
		}

		// 遷移ルールを評価して次の要素を決定
		transitionRule, found := j.flow.GetTransitionRule(element.ID(), elementExitStatus, elementErr != nil) // ★ 修正: GetTransitionRule を使用
		if !found {
			// 遷移ルールが見つからない場合
			if elementErr == nil {
				// 正常終了だが次の遷移がない場合はジョブ完了
				logger.Infof("ジョブ '%s': フロー要素 '%s' からの遷移ルールが見つかりません。ジョブを完了します。", j.JobName(), element.ID())
				jobExecution.MarkAsCompleted()
			} else {
				// エラー発生で次の遷移がない場合はジョブ失敗
				logger.Errorf("ジョブ '%s': フロー要素 '%s' でエラーが発生しましたが、適切な遷移ルールが見つかりません。ジョブを失敗として終了します。", j.JobName(), element.ID())
				jobExecution.MarkAsFailed(elementErr)
			}
			break
		}

		// 遷移ルールが End, Fail, Stop を指示した場合の処理
		if transitionRule.End {
			logger.Infof("ジョブ '%s': フロー要素 '%s' から 'End' 遷移が指示されました。ジョブを完了します。", j.JobName(), element.ID())
			jobExecution.MarkAsCompleted()
			break
		}
		if transitionRule.Fail {
			logger.Errorf("ジョブ '%s': フロー要素 '%s' から 'Fail' 遷移が指示されました。ジョブを失敗として終了します。", j.JobName(), element.ID())
			jobExecution.MarkAsFailed(fmt.Errorf("explicit fail transition from %s", element.ID()))
			break
		}
		if transitionRule.Stop {
			logger.Infof("ジョブ '%s': フロー要素 '%s' から 'Stop' 遷移が指示されました。ジョブを停止します。", j.JobName(), element.ID())
			jobExecution.MarkAsStopped()
			break
		}

		// 次の要素IDを更新
		currentElementID = transitionRule.To
	}

	logger.Infof("ジョブ '%s' (Execution ID: %s) の実行が完了しました。", j.JobName(), jobExecution.ID)
	return nil
}
