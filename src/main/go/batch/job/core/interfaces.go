package core

import "context"

// FlowElement はフロー内の要素（StepまたはDecision）の共通インターフェースです。
type FlowElement interface {
	ID() string // 要素のIDを返すメソッド
}

// Job は実行可能なバッチジョブのインターフェースです。
type Job interface {
	Run(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) error
	JobName() string
	GetFlow() *FlowDefinition
}

// Step はジョブ内で実行される単一のステップのインターフェースです。
type Step interface {
	Execute(ctx context.Context, jobExecution *JobExecution, stepExecution *StepExecution) error
	StepName() string
	ID() string // FlowElement インターフェースの実装
}

// ItemReadListener はアイテム読み込みイベントを処理するためのインターフェースです。
type ItemReadListener interface {
	OnReadError(ctx context.Context, err error) // 読み込みエラー時に呼び出されます
}

// ItemProcessListener はアイテム処理イベントを処理するためのインターフェースです。
type ItemProcessListener interface {
	OnProcessError(ctx context.Context, item interface{}, err error) // 処理エラー時に呼び出されます
	OnSkipInProcess(ctx context.Context, item interface{}, err error) // 処理中にスキップされたアイテムに対して呼び出されます
}

// ItemWriteListener はアイテム書き込みイベントを処理するためのインターフェースです。
type ItemWriteListener interface {
	OnWriteError(ctx context.Context, items []interface{}, err error) // 書き込みエラー時に呼び出されます
	OnSkipInWrite(ctx context.Context, item interface{}, err error) // 書き込み中にスキップされたアイテムに対して呼び出されます
}

// ItemListener は全てのアイテムレベルリスナーインターフェースをまとめたものです。
type ItemListener interface{}

// Decision はフロー内の条件分岐ポイントのインターフェースを定義します。
type Decision interface {
	// Decide メソッドは、ExecutionContext やその他のパラメータに基づいて次の遷移を決定します。
	Decide(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) (ExitStatus, error)
	DecisionName() string
	ID() string // FlowElement インターフェースの実装
}
