package tasklet

import (
	"context"

	config "sample/pkg/batch/config"
	core "sample/pkg/batch/job/core"
	job "sample/pkg/batch/repository/job"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// ExecutionContextWriterTasklet は指定されたキーと値で ExecutionContext にデータを書き込む Tasklet です。
type ExecutionContextWriterTasklet struct {
	name       string
	properties map[string]string
}

// NewExecutionContextWriterTasklet は新しい ExecutionContextWriterTasklet のインスタンスを作成します。
func NewExecutionContextWriterTasklet(cfg *config.Config, repo job.JobRepository, properties map[string]string) (core.Tasklet, error) { // ★ 変更: 戻り値に error を追加
	return &ExecutionContextWriterTasklet{
		name:       "ExecutionContextWriterTasklet",
		properties: properties,
	}, nil // ★ 変更: nil を追加して2つの戻り値を返す
}

// Execute は Tasklet のビジネスロジックを実行します。
func (t *ExecutionContextWriterTasklet) Execute(ctx context.Context, stepExecution *core.StepExecution) (core.ExitStatus, error) {
	logger.Infof("Tasklet '%s' を実行するよ。", t.name)

	key, ok := t.properties["key"]
	if !ok || key == "" {
		return core.ExitStatusFailed, exception.NewBatchErrorf(t.name, "プロパティ 'key' が指定されていません")
	}
	value, ok := t.properties["value"]
	if !ok {
		// 値が指定されていない場合は空文字列として扱う
		value = ""
	}

	logger.Debugf("Tasklet '%s': ExecutionContext にキー '%s' で値 '%s' を書き込むよ。", t.name, key, value)
	stepExecution.ExecutionContext.PutNested(key, value) // ★ 変更: PutNested を使用

	logger.Infof("Tasklet '%s' が正常に完了したよ。", t.name)
	return core.ExitStatusCompleted, nil
}

// Close はリソースを解放するためのメソッドです。
func (t *ExecutionContextWriterTasklet) Close(ctx context.Context) error {
	logger.Debugf("Tasklet '%s' をクローズするよ。", t.name)
	return nil
}

// SetExecutionContext は ExecutionContext を設定します。
func (t *ExecutionContextWriterTasklet) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	logger.Debugf("Tasklet '%s': ExecutionContext を設定するよ: %+v", t.name, ec)
	// このTaskletは自身の状態をECに保存しないため、特に何もしない
	return nil
}

// GetExecutionContext は ExecutionContext を取得します。
func (t *ExecutionContextWriterTasklet) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	logger.Debugf("Tasklet '%s': ExecutionContext を取得するよ。", t.name)
	// このTaskletは自身の状態をECに保存しないため、空のECを返す
	return core.NewExecutionContext(), nil
}

// ExecutionContextWriterTasklet が core.Tasklet インターフェースを満たすことを確認
var _ core.Tasklet = (*ExecutionContextWriterTasklet)(nil)
