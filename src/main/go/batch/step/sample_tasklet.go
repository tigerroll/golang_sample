// src/main/go/batch/step/sample_tasklet.go
package step

import (
	"context"
	"fmt"
	"time"

	core "sample/src/main/go/batch/job/core"
	logger "sample/src/main/go/batch/util/logger"
)

// SampleTasklet は Tasklet インターフェースのシンプルな実装です。
// 特定のビジネスロジックを実行し、ExitStatus を返します。
type SampleTasklet struct {
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
	executionCount   int // 実行回数を保持するダミーの状態
}

// NewSampleTasklet は新しい SampleTasklet のインスタンスを作成します。
func NewSampleTasklet() *SampleTasklet {
	return &SampleTasklet{
		executionContext: core.NewExecutionContext(),
		executionCount:   0,
	}
}

// Execute は Tasklet のビジネスロジックを実行します。
func (t *SampleTasklet) Execute(ctx context.Context, stepExecution *core.StepExecution) (core.ExitStatus, error) {
	select {
	case <-ctx.Done():
		logger.Warnf("SampleTasklet '%s': Context がキャンセルされたため中断します: %v", stepExecution.StepName, ctx.Err())
		return core.ExitStatusStopped, ctx.Err()
	default:
	}

	t.executionCount++
	logger.Infof("SampleTasklet '%s' が実行されました。実行回数: %d", stepExecution.StepName, t.executionCount)

	// ダミーのビジネスロジック: 3回実行されたら失敗する
	if t.executionCount >= 3 {
		logger.Errorf("SampleTasklet '%s' が意図的に失敗しました (実行回数: %d)", stepExecution.StepName, t.executionCount)
		return core.ExitStatusFailed, fmt.Errorf("SampleTasklet '%s' 意図的な失敗", stepExecution.StepName)
	}

	// ExecutionContext に状態を保存
	t.executionContext.Put("tasklet_execution_count", t.executionCount)
	logger.Debugf("SampleTasklet '%s': ExecutionContext に実行回数を保存しました: %d", stepExecution.StepName, t.executionCount)

	// 処理に時間がかかることをシミュレート
	time.Sleep(500 * time.Millisecond)

	logger.Infof("SampleTasklet '%s' が正常に完了しました。", stepExecution.StepName)
	return core.ExitStatusCompleted, nil
}

// Close は Tasklet が使用するリソースを解放します。
func (t *SampleTasklet) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("SampleTasklet.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は Tasklet の ExecutionContext を設定します。
func (t *SampleTasklet) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	t.executionContext = ec
	if count, ok := ec.GetInt("tasklet_execution_count"); ok {
		t.executionCount = count
		logger.Debugf("SampleTasklet: ExecutionContext から実行回数を復元しました: %d", t.executionCount)
	}
	logger.Debugf("SampleTasklet.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は Tasklet の現在の ExecutionContext を返します。
func (t *SampleTasklet) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	// 現在の実行回数を ExecutionContext に反映
	t.executionContext.Put("tasklet_execution_count", t.executionCount)
	logger.Debugf("SampleTasklet.GetExecutionContext が呼び出されました。現在の実行回数: %d", t.executionCount)
	return t.executionContext, nil
}

// SampleTasklet が Tasklet インターフェースを満たすことを確認
var _ Tasklet = (*SampleTasklet)(nil)
