package tasklet

import (
	"context"
	"database/sql" // Add sql import for *sql.DB
	"fmt"
	"math/rand" // rand パッケージをインポート
	"time"

	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core"
	step "sample/pkg/batch/step" // pkg/batch/step を参照
	logger "sample/pkg/batch/util/logger"
	"sample/pkg/batch/util/exception" // exception パッケージをインポート
)

// DummyTasklet は Tasklet インターフェースのシンプルな実装です。
// 特定のビジネスロジックを実行し、ExitStatus を返します。
type DummyTasklet struct {
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
	executionCount   int // 実行回数を保持するダミーの状態
}

// NewDummyTasklet は新しい DummyTasklet のインスタンスを作成します。
// ComponentBuilder のシグネチャに合わせ、cfg, db, properties を受け取りますが、現時点では利用しません。
func NewDummyTasklet(cfg *config.Config, db *sql.DB, properties map[string]string) (*DummyTasklet, error) { // ★ 変更: シグネチャを factory.ComponentBuilder に合わせる
	_ = cfg        // 未使用の引数を無視
	_ = db
	_ = properties
	return &DummyTasklet{
		executionContext: core.NewExecutionContext(),
		executionCount:   0,
	}, nil
}

// Execute は Tasklet のビジネスロジックを実行します。
func (t *DummyTasklet) Execute(ctx context.Context, stepExecution *core.StepExecution) (core.ExitStatus, error) {
	select {
	case <-ctx.Done():
		logger.Warnf("DummyTasklet '%s': Context がキャンセルされたため中断します: %v", stepExecution.StepName, ctx.Err())
		return core.ExitStatusStopped, ctx.Err()
	default:
	}

	t.executionCount++
	logger.Infof("DummyTasklet '%s' が実行されました。実行回数: %d", stepExecution.StepName, t.executionCount)

	// ダミーのエラー発生ロジック: 3回に1回エラーを発生させる (ランダム)
	// または、特定の回数で失敗させるロジックを維持
	if rand.Intn(3) == 0 { // 3分の1の確率でエラーを発生させる例
	// if t.executionCount == 1 { // 最初の実行でエラーを発生させる例 (テスト用)
		logger.Errorf("DummyTasklet '%s' が意図的に失敗しました (実行回数: %d)", stepExecution.StepName, t.executionCount)
		return core.ExitStatusFailed, exception.NewBatchError("dummy_tasklet", fmt.Sprintf("DummyTasklet '%s' 意図的な失敗", stepExecution.StepName), nil, false, false)
	}

	// ExecutionContext に状態を保存
	t.executionContext.Put("tasklet_execution_count", t.executionCount)
	logger.Debugf("DummyTasklet '%s': ExecutionContext に実行回数を保存しました: %d", stepExecution.StepName, t.executionCount)

	// 処理に時間がかかることをシミュレート
	time.Sleep(500 * time.Millisecond)

	logger.Infof("DummyTasklet '%s' が正常に完了しました。", stepExecution.StepName)
	return core.ExitStatusCompleted, nil
}

// Close は Tasklet が使用するリソースを解放します。
func (t *DummyTasklet) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("DummyTasklet.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は Tasklet の ExecutionContext を設定します。
func (t *DummyTasklet) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	t.executionContext = ec
	if count, ok := ec.GetInt("tasklet_execution_count"); ok {
		t.executionCount = count
		logger.Debugf("DummyTasklet: ExecutionContext から実行回数を復元しました: %d", t.executionCount)
	}
	logger.Debugf("DummyTasklet.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は Tasklet の現在の ExecutionContext を返します。
func (t *DummyTasklet) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	// 現在の実行回数を ExecutionContext に反映
	t.executionContext.Put("tasklet_execution_count", t.executionCount)
	logger.Debugf("DummyTasklet.GetExecutionContext が呼び出されました。現在の実行回数: %d", t.executionCount)
	return t.executionContext, nil
}

// DummyTasklet が Tasklet インターフェースを満たすことを確認
var _ step.Tasklet = (*DummyTasklet)(nil)
