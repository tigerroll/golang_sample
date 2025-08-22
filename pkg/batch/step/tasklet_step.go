package step

import (
	"context"
	"fmt"
	"time"
	
	core "sample/pkg/batch/job/core"
	"sample/pkg/batch/repository/job"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// TaskletStep は Tasklet インターフェースをラップし、core.Step インターフェースを実装します。
// JSR352のTaskletステップに相当します。
type TaskletStep struct {
	name                      string                         // ステップ名
	tasklet                   core.Tasklet                   // core.Tasklet インターフェースを使用
	stepListeners             []core.StepExecutionListener   // core.StepExecutionListener を使用
	jobRepository             job.JobRepository              // job.JobRepository に変更
	executionContextPromotion *core.ExecutionContextPromotion // ★ 追加: ExecutionContextPromotion の設定
}

// TaskletStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*TaskletStep)(nil)

// NewTaskletStep は新しい TaskletStep のインスタンスを作成します。
func NewTaskletStep(
	name string,
	tasklet core.Tasklet, // core.Tasklet を使用
	jobRepository job.JobRepository,
	stepListeners []core.StepExecutionListener, // core.StepExecutionListener を使用
	executionContextPromotion *core.ExecutionContextPromotion, // ★ 追加
) *TaskletStep { // TaskletStep を返す
	return &TaskletStep{
		name:                      name,
		tasklet:                   tasklet,
		jobRepository:             jobRepository,
		stepListeners:             stepListeners,
		executionContextPromotion: executionContextPromotion, // ★ 追加
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
func (s *TaskletStep) notifyBeforeStep(ctx context.Context, stepExecution *core.StepExecution) { // core.StepExecutionListener を使用
	for _, l := range s.stepListeners {
		l.BeforeStep(ctx, stepExecution)
	}
}

// notifyAfterStep は登録されている StepExecutionListener の AfterStep メソッドを呼び出します。
func (s *TaskletStep) notifyAfterStep(ctx context.Context, stepExecution *core.StepExecution) { // core.StepExecutionListener を使用
	for _, l := range s.stepListeners {
		l.AfterStep(ctx, stepExecution)
	}
}

// promoteExecutionContext は StepExecutionContext の指定されたキーを JobExecutionContext にプロモートします。
func (s *TaskletStep) promoteExecutionContext(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) {
	if s.executionContextPromotion == nil || len(s.executionContextPromotion.Keys) == 0 {
		logger.Debugf("Taskletステップ '%s': ExecutionContextPromotion の設定がないか、プロモートするキーが指定されていません。", s.name)
		return
	}

	logger.Debugf("Taskletステップ '%s': ExecutionContext のプロモーションを開始します。", s.name)
	for _, key := range s.executionContextPromotion.Keys {
		if val, ok := stepExecution.ExecutionContext.GetNested(key); ok {
			jobLevelKey := key
			if mappedKey, found := s.executionContextPromotion.JobLevelKeys[key]; found {
				jobLevelKey = mappedKey
			}
			jobExecution.ExecutionContext.PutNested(jobLevelKey, val)
			logger.Debugf("Taskletステップ '%s': StepExecutionContext のキー '%s' を JobExecutionContext のキー '%s' にプロモートしました。", s.name, key, jobLevelKey)
		} else {
			logger.Warnf("Taskletステップ '%s': StepExecutionContext にプロモート対象のキー '%s' が見つかりませんでした。", s.name, key)
		}
	}
	logger.Debugf("Taskletステップ '%s': ExecutionContext のプロモーションが完了しました。", s.name)
}

// Execute は TaskletStep の処理を実行します。core.Step インターフェースの実装です。
func (s *TaskletStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	logger.Infof("Taskletステップ '%s' (Execution ID: %s) を始めるよ。", s.name, stepExecution.ID)

	// StepExecution は Job.Run メソッドで既に初期化され、JobExecution に追加されていることを想定
	// ここでは、その StepExecution の状態を更新し、永続化する
	stepExecution.StartTime = time.Now()
	stepExecution.Status = core.BatchStatusStarting
	stepExecution.LastUpdated = time.Now()
	// ExecutionContext は NewStepExecution で初期化済み、またはリスタート時にロード済み

	// StepExecution の ExecutionContext から Tasklet の状態を復元 (リスタート時)
	if len(stepExecution.ExecutionContext) > 0 {
		logger.Debugf("Taskletステップ '%s': ExecutionContext から Tasklet の状態を復元するよ。", s.name)
		if taskletECVal, ok := stepExecution.ExecutionContext.Get("tasklet_context"); ok {
			if taskletEC, isEC := taskletECVal.(core.ExecutionContext); isEC {
				if err := s.tasklet.SetExecutionContext(ctx, taskletEC); err != nil {
					logger.Errorf("Taskletステップ '%s': Tasklet の ExecutionContext 復元に失敗したよ: %v", s.name, err)
					stepExecution.AddFailureException(err)
					return exception.NewBatchError(s.name, "Tasklet の ExecutionContext 復元エラー", err, false, false)
				}
			} else {
				logger.Warnf("Taskletステップ '%s': Tasklet の ExecutionContext が予期しない型だよ: %T", s.name, taskletECVal)
			}
		}
	}

	// ステップ実行前処理の通知 (ExecutionContext 復元後)
	s.notifyBeforeStep(ctx, stepExecution)


	// ステップ実行後処理 (defer で必ず実行)
	defer func() {
		// ステップの終了時刻を設定
		stepExecution.EndTime = time.Now()

		// Tasklet の Close を呼び出す
		if err := s.tasklet.Close(ctx); err != nil {
			logger.Errorf("Taskletステップ '%s': Tasklet のクローズに失敗したよ: %v", s.name, err)
			stepExecution.AddFailureException(err)
		}

		// ステップ実行後処理の通知
		s.notifyAfterStep(ctx, stepExecution)

		// ★ 追加: ExecutionContext のプロモーション
		s.promoteExecutionContext(ctx, jobExecution, stepExecution)
	}()

	// Tasklet の Execute メソッドを呼び出す
	exitStatus, err := s.tasklet.Execute(ctx, stepExecution)
	if err != nil {
		logger.Errorf("Taskletステップ '%s' の実行中にエラーが発生したよ: %v", s.name, err)
		stepExecution.MarkAsFailed(err)
		jobExecution.AddFailureException(err)
		return exception.NewBatchError(s.name, "Tasklet 実行エラー", err, false, false)
	}

	// Tasklet の ExecutionContext を取得し、StepExecution に保存
	taskletEC, err := s.tasklet.GetExecutionContext(ctx)
	if err != nil {
		logger.Errorf("Taskletステップ '%s': Tasklet の ExecutionContext 取得に失敗したよ: %v", s.name, err)
		stepExecution.AddFailureException(err)
		return exception.NewBatchError(s.name, "Tasklet の ExecutionContext 取得エラー", err, false, false)
	}
	stepExecution.ExecutionContext.Put("tasklet_context", taskletEC)

	// StepExecution を更新してチェックポイントを永続化
	if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
		logger.Errorf("Taskletステップ '%s': StepExecution の更新 (チェックポイント) に失敗したよ: %v", s.name, err)
		stepExecution.AddFailureException(exception.NewBatchError(s.name, "StepExecution 更新エラー", err, false, false))
		return exception.NewBatchError(s.name, "StepExecution の更新 (チェックポイント) エラー", err, false, false)
	}

	// Tasklet の ExitStatus に基づいて StepExecution の状態を更新
	stepExecution.ExitStatus = exitStatus
	if exitStatus == core.ExitStatusCompleted {
		stepExecution.MarkAsCompleted()
	} else {
		stepExecution.MarkAsFailed(fmt.Errorf("Tasklet returned non-completed exit status: %s", exitStatus))
	}

	logger.Infof("Taskletステップ '%s' が正常に完了したよ。ExitStatus: %s", s.name, exitStatus)
	return nil
}
