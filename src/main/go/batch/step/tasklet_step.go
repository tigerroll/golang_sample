// src/main/go/batch/step/tasklet_step.go
package step

import (
	"context"
	"fmt"
	"time"

	core "sample/src/main/go/batch/job/core"
	repository "sample/src/main/go/batch/repository"
	stepListener "sample/src/main/go/batch/step/listener"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
)

// TaskletStep は Tasklet インターフェースをラップし、core.Step インターフェースを実装します。
// JSR352のTaskletステップに相当します。
type TaskletStep struct {
	name          string
	tasklet       Tasklet // Tasklet インターフェース
	stepListeners []stepListener.StepExecutionListener
	jobRepository repository.JobRepository // JobRepository を追加
}

// TaskletStep が core.Step インターフェースを満たすことを確認します。
var _ core.Step = (*TaskletStep)(nil)

// NewTaskletStep は新しい TaskletStep のインスタンスを作成します。
func NewTaskletStep(
	name string,
	tasklet Tasklet,
	jobRepository repository.JobRepository,
	stepListeners []stepListener.StepExecutionListener,
) *TaskletStep {
	return &TaskletStep{
		name:          name,
		tasklet:       tasklet,
		jobRepository: jobRepository,
		stepListeners: stepListeners,
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

// Execute は TaskletStep の処理を実行します。core.Step インターフェースの実装です。
func (s *TaskletStep) Execute(ctx context.Context, jobExecution *core.JobExecution, stepExecution *core.StepExecution) error {
	logger.Infof("Taskletステップ '%s' (Execution ID: %s) を開始します。", s.name, stepExecution.ID)

	// StepExecution の開始時刻を設定し、状態をマーク
	stepExecution.StartTime = time.Now()
	stepExecution.MarkAsStarted() // Status = Started

	// ステップ実行前処理の通知
	s.notifyBeforeStep(ctx, stepExecution)

	// StepExecution の ExecutionContext から Tasklet の状態を復元 (リスタート時)
	if len(stepExecution.ExecutionContext) > 0 {
		logger.Debugf("Taskletステップ '%s': ExecutionContext から Tasklet の状態を復元します。", s.name)
		// taskletECVal は interface{} 型、ok は bool 型
		if taskletECVal, ok := stepExecution.ExecutionContext.Get("tasklet_context"); ok { // ★ 修正: 2つの戻り値を受け取る
			if taskletEC, isEC := taskletECVal.(core.ExecutionContext); isEC { // ★ 修正: 型アサーションを安全に行う
				if err := s.tasklet.SetExecutionContext(ctx, taskletEC); err != nil {
					logger.Errorf("Taskletステップ '%s': Tasklet の ExecutionContext 復元に失敗しました: %v", s.name, err)
					stepExecution.AddFailureException(err)
					return exception.NewBatchError(s.name, "Tasklet の ExecutionContext 復元エラー", err, false, false)
				}
			} else {
				logger.Warnf("Taskletステップ '%s': Tasklet の ExecutionContext が予期しない型です: %T", s.name, taskletECVal)
			}
		}
	}

	// ステップ実行後処理 (defer で必ず実行)
	defer func() {
		// ステップの終了時刻を設定
		stepExecution.EndTime = time.Now()

		// Tasklet の Close を呼び出す
		if err := s.tasklet.Close(ctx); err != nil {
			logger.Errorf("Taskletステップ '%s': Tasklet のクローズに失敗しました: %v", s.name, err)
			stepExecution.AddFailureException(err)
		}

		// ステップ実行後処理の通知
		s.notifyAfterStep(ctx, stepExecution)
	}()

	// Tasklet の Execute メソッドを呼び出す
	exitStatus, err := s.tasklet.Execute(ctx, stepExecution)
	if err != nil {
		logger.Errorf("Taskletステップ '%s' の実行中にエラーが発生しました: %v", s.name, err)
		stepExecution.MarkAsFailed(err)
		jobExecution.AddFailureException(err) // JobExecution にもエラーを追加
		return exception.NewBatchError(s.name, "Tasklet 実行エラー", err, false, false)
	}

	// Tasklet の ExecutionContext を取得し、StepExecution に保存
	taskletEC, err := s.tasklet.GetExecutionContext(ctx)
	if err != nil {
		logger.Errorf("Taskletステップ '%s': Tasklet の ExecutionContext 取得に失敗しました: %v", s.name, err)
		stepExecution.AddFailureException(err)
		return exception.NewBatchError(s.name, "Tasklet の ExecutionContext 取得エラー", err, false, false)
	}
	stepExecution.ExecutionContext.Put("tasklet_context", taskletEC)

	// StepExecution を更新してチェックポイントを永続化
	if err = s.jobRepository.UpdateStepExecution(ctx, stepExecution); err != nil {
		logger.Errorf("Taskletステップ '%s': StepExecution の更新 (チェックポイント) に失敗しました: %v", s.name, err)
		stepExecution.AddFailureException(exception.NewBatchError(s.name, "StepExecution 更新エラー", err, false, false))
		return exception.NewBatchError(s.name, "StepExecution の更新 (チェックポイント) エラー", err, false, false)
	}

	// Tasklet の ExitStatus に基づいて StepExecution の状態を更新
	stepExecution.ExitStatus = exitStatus
	if exitStatus == core.ExitStatusCompleted {
		stepExecution.MarkAsCompleted()
	} else {
		// Tasklet が COMPLETED 以外を返した場合、ステップは失敗とみなす
		stepExecution.MarkAsFailed(fmt.Errorf("Tasklet returned non-completed exit status: %s", exitStatus))
	}

	logger.Infof("Taskletステップ '%s' が正常に完了しました。ExitStatus: %s", s.name, exitStatus)
	return nil
}
