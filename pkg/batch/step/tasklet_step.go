package step

import (
	"context"
	"errors"
	"time"

	core "sample/pkg/batch/job/core"
	job "sample/pkg/batch/repository/job"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// TaskletStep は Tasklet を実行するステップの実装です。
// core.Step インターフェースを実装します。
type TaskletStep struct {
	name                      string
	tasklet                   core.Tasklet
	jobRepository             job.JobRepository
	stepListeners             []core.StepExecutionListener
	executionContextPromotion *core.ExecutionContextPromotion
}

// TaskletStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*TaskletStep)(nil)

// NewTaskletStep は新しい TaskletStep のインスタンスを作成します。
func NewTaskletStep(
	name string,
	tasklet core.Tasklet,
	jobRepository job.JobRepository,
	stepListeners []core.StepExecutionListener,
	executionContextPromotion *core.ExecutionContextPromotion,
) *TaskletStep {
	return &TaskletStep{
		name:                      name,
		tasklet:                   tasklet,
		jobRepository:             jobRepository,
		stepListeners:             stepListeners,
		executionContextPromotion: executionContextPromotion,
	}
}

// StepName はステップ名を返します。core.Step インターフェースの実装です。
func (s *TaskletStep) StepName() string {
	return s.name
}

// ID はステップのIDを返します。core.FlowElement インターフェースの実装です。
func (s *TaskletStep) ID() string {
	return s.name
}

// notifyBeforeStep は登録されている StepExecutionListener の BeforeStep メソッドを呼び出します。
func (s *TaskletStep) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.stepListeners {
		l.BeforeStep(ctx, stepExecution)
	}
}

// notifyAfterStep は登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
func (s *TaskletStep) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) {
	for _, l := range s.stepListeners {
		l.AfterStep(ctx, stepExecution)
	}
}

// promoteExecutionContext は StepExecutionContext の指定されたキーを JobExecutionContext にプロモートします。
func (s *TaskletStep) promoteExecutionContext(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) {
	if s.executionContextPromotion == nil || len(s.executionContextPromotion.Keys) == 0 {
		logger.Debugf("ステップ '%s': ExecutionContextPromotion の設定がないか、プロモートするキーが指定されていません。", s.name)
		return
	}

	logger.Debugf("ステップ '%s': ExecutionContext のプロモーションを開始します。", s.name)
	for _, key := range s.executionContextPromotion.Keys {
		if val, ok := stepExecution.ExecutionContext.GetNested(key); ok {
			jobLevelKey := key
			if mappedKey, found := s.executionContextPromotion.JobLevelKeys[key]; found {
				jobLevelKey = mappedKey
			}
			jobExecution.ExecutionContext.PutNested(jobLevelKey, val)
			logger.Debugf("ステップ '%s': StepExecutionContext のキー '%s' を JobExecutionContext のキー '%s' にプロモートしました。", s.name, key, jobLevelKey)
		} else {
			logger.Warnf("ステップ '%s': StepExecutionContext にプロモート対象のキー '%s' が見つかりませんでした。", s.name, key)
		}
	}
	logger.Debugf("ステップ '%s': ExecutionContext のプロモーションが完了しました。", s.name)
}

// Execute は Tasklet の実行ロジックを定義します。core.Step インターフェースの実装です。
func (s *TaskletStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	logger.Infof("ステップ '%s' (Execution ID: %s) を始めるよ。", s.name, stepExecution.ID)

	// StepExecution の開始時刻を設定し、状態をマーク
	if err := stepExecution.TransitionTo(core.BatchStatusStarted); err != nil {
		logger.Errorf("ステップ '%s': StepExecution の状態を STARTED に更新できませんでした: %v", s.name, err)
		stepExecution.AddFailureException(err)
		stepExecution.MarkAsFailed(err)
		return exception.NewBatchError(s.name, "StepExecution の状態更新エラー", err, false, false)
	}
	stepExecution.StartTime = time.Now()

	// StepExecution の ExecutionContext から Tasklet の状態を復元 (リスタート時)
	if len(stepExecution.ExecutionContext) > 0 {
		logger.Debugf("ステップ '%s': ExecutionContext から Tasklet の状態を復元するよ。", s.name)
		if taskletECVal, ok := stepExecution.ExecutionContext.Get("tasklet_context"); ok {
			if taskletEC, isEC := taskletECVal.(core.ExecutionContext); isEC {
				if err := s.tasklet.SetExecutionContext(ctx, taskletEC); err != nil {
					logger.Errorf("ステップ '%s': Tasklet の ExecutionContext 復元に失敗したよ: %v", s.name, err)
					stepExecution.AddFailureException(err)
					return exception.NewBatchError(s.name, "Tasklet の ExecutionContext 復元エラー", err, false, false)
				}
			} else {
				logger.Warnf("ステップ '%s': Tasklet の ExecutionContext が予期しない型だよ: %T", s.name, taskletECVal)
			}
		}
	}

	// ステップ実行前処理の通知 (ExecutionContext 復元後)
	s.notifyBeforeStep(ctx, stepExecution)

	// ステップ実行後処理 (defer で必ず実行)
	defer func() {
		// ステップの終了時刻を設定
		stepExecution.EndTime = time.Now()

		// Tasklet の ExecutionContext をステップの ExecutionContext に保存
		if ec, err := s.tasklet.GetExecutionContext(ctx); err == nil {
			stepExecution.ExecutionContext.Put("tasklet_context", ec)
		} else {
			logger.Errorf("ステップ '%s': Tasklet の ExecutionContext 取得に失敗したよ: %v", s.name, err)
		}

		// Tasklet の Close を呼び出す
		if err := s.tasklet.Close(ctx); err != nil {
			logger.Errorf("ステップ '%s': Tasklet のクローズに失敗したよ: %v", s.name, err)
			stepExecution.AddFailureException(err)
		}

		// ステップ実行後処理の通知
		s.notifyAfterStep(ctx, stepExecution)

		// ExecutionContext のプロモーション
		s.promoteExecutionContext(ctx, jobExecution, stepExecution)
	}()

	// Tasklet の Execute メソッドを呼び出す
	exitStatus, err := s.tasklet.Execute(ctx, stepExecution)

	if err != nil {
		logger.Errorf("ステップ '%s': Tasklet の実行中にエラーが発生したよ: %v", s.name, err)
		stepExecution.AddFailureException(err)
		// Context キャンセルによるエラーの場合、ステップを STOPPED 状態に
		if errors.Is(err, context.Canceled) {
			stepExecution.MarkAsStopped() // ★ 修正
		} else {
			stepExecution.MarkAsFailed(err)
		}
		return exception.NewBatchError(s.name, "Tasklet 実行エラー", err, false, false)
	}

	// Tasklet が正常に完了した場合
	stepExecution.ExitStatus = exitStatus
	stepExecution.MarkAsCompleted()
	logger.Infof("ステップ '%s' が正常に完了したよ。ExitStatus: %s", s.name, exitStatus)

	// StepExecution を更新して永続化
	if err := s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
		logger.Errorf("ステップ '%s': StepExecution の更新に失敗したよ: %v", s.name, err)
		stepExecution.AddFailureException(err)
		return exception.NewBatchError(s.name, "StepExecution の永続化エラー", err, false, false)
	}

	return nil
}
