package core

import (
	"context"
	"sample/pkg/batch/database"
)

// FlowElement はフロー内の要素（StepまたはDecision）の共通インターフェースです。
type FlowElement interface {
	ID() string // 要素のIDを返すメソッド
}

// Job は実行可能なバッチジョブのインターフェースです。
type Job interface {
	Run(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) error
	JobName() string
	GetFlow() *FlowDefinition
	ValidateParameters(params JobParameters) error
}

// Step はジョブ内で実行される単一のステップのインターフェースです。
type Step interface {
	Execute(ctx context.Context, jobExecution *JobExecution, stepExecution *StepExecution) error
	StepName() string
	ID() string // FlowElement インターフェースの実装
}

// --- ここから追加/移動するインターフェース ---

// ItemReader はデータを読み込むステップのインターフェースです。
// O は読み込まれるアイテムの型です。
type ItemReader[O any] interface {
	Open(ctx context.Context, ec ExecutionContext) error // リソースを開き、ExecutionContextから状態を復元
	Read(ctx context.Context) (O, error)                 // 読み込んだデータを O 型で返す
	Close(ctx context.Context) error                     // リソースを解放するためのメソッド
	SetExecutionContext(ctx context.Context, ec ExecutionContext) error // ExecutionContext を設定
	GetExecutionContext(ctx context.Context) (ExecutionContext, error) // ExecutionContext を取得
}

// ItemProcessor はアイテムを処理するステップのインターフェースです。
// I は入力アイテムの型、O は出力アイテムの型です。
type ItemProcessor[I, O any] interface {
	Process(ctx context.Context, item I) (O, error) // 処理対象のアイテムと結果を I, O 型で扱う
	SetExecutionContext(ctx context.Context, ec ExecutionContext) error // ExecutionContext を設定
	GetExecutionContext(ctx context.Context) (ExecutionContext, error) // ExecutionContext を取得
}

// ItemWriter はデータを書き込むステップのインターフェースです。
// I は書き込まれるアイテムの型です。
type ItemWriter[I any] interface {
	Open(ctx context.Context, ec ExecutionContext) error // リソースを開き、ExecutionContextから状態を復元
	Write(ctx context.Context, tx database.Tx, items []I) error // 書き込むデータを I 型のスライスで扱い、トランザクションを受け取る
	Close(ctx context.Context) error                     // リソースを解放するためのメソッド
	SetExecutionContext(ctx context.Context, ec ExecutionContext) error // ExecutionContext を設定
	GetExecutionContext(ctx context.Context) (ExecutionContext, error) // ExecutionContext を取得
}

// Tasklet は単一の操作を実行するステップのインターフェースです。
// JSR352のTaskletに相当します。
type Tasklet interface {
	// Execute はTaskletのビジネスロジックを実行します。
	// 処理が成功した場合は COMPLETED などの ExitStatus を返し、エラーが発生した場合はエラーを返します。
	Execute(ctx context.Context, stepExecution *StepExecution) (ExitStatus, error)
	// Close はリソースを解放するためのメソッドです。
	Close(ctx context.Context) error
	// SetExecutionContext は ExecutionContext を設定します。
	SetExecutionContext(ctx context.Context, ec ExecutionContext) error
	// GetExecutionContext は ExecutionContext を取得します。
	GetExecutionContext(ctx context.Context) (ExecutionContext, error)
}

// RetryItemListener はアイテムレベルのリトライイベントを処理するためのインターフェースです。
type RetryItemListener interface {
	OnRetryRead(ctx context.Context, err error)
	OnRetryProcess(ctx context.Context, item interface{}, err error)
	OnRetryWrite(ctx context.Context, items []interface{}, err error)
}

// SkipListener はアイテムスキップイベントを処理するためのインターフェースです。
type SkipListener interface {
	OnSkipRead(ctx context.Context, err error)
	OnSkipProcess(ctx context.Context, item interface{}, err error)
	OnSkipWrite(ctx context.Context, item interface{}, err error)
}

// StepExecutionListener はステップ実行イベントを処理するためのインターフェースです。
type StepExecutionListener interface {
	// BeforeStep メソッドシグネチャを変更し、StepExecution を追加
	BeforeStep(ctx context.Context, stepExecution *StepExecution)
	// AfterStep メソッドシグネチャを変更し、StepExecution を追加
	AfterStep(ctx context.Context, stepExecution *StepExecution)
}

// ChunkListener はチャンク処理イベントを処理するためのインターフェースです。
type ChunkListener interface {
	BeforeChunk(ctx context.Context, stepExecution *StepExecution)
	AfterChunk(ctx context.Context, stepExecution *StepExecution)
}

// JobExecutionListener はジョブ実行イベントを処理するためのインターフェースです。
type JobExecutionListener interface {
	// BeforeJob はジョブの実行が開始される直前に呼び出されます。
	BeforeJob(ctx context.Context, jobExecution *JobExecution)
	// AfterJob はジョブの実行が完了した後に呼び出されます。成功・失敗に関わらず呼び出されます。
	AfterJob(ctx context.Context, jobExecution *JobExecution)
}

// --- ここまで追加/移動するインターフェース ---

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
	OnSkipInWrite(ctx context.Context, items []interface{}, err error) // 書き込み中にスキップされたアイテムに対して呼び出されます (変更: items []interface{} を受け取る)
}

// ItemListener は全てのアイテムレベルリスナーインターフェースをまとめたものです。
type ItemListener interface{}

// Decision はフロー内の条件分岐ポイントのインターフェースを定義します。
type Decision interface {
	// Decide メソッドは、ExecutionContext やその他のパラメータに基づいて次の遷移を決定します。
	Decide(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) (ExitStatus, error)
	DecisionName() string
	ID() string // FlowElement インターフェースの実装
	// SetProperties は JSL から注入されるプロパティを設定します。
	SetProperties(properties map[string]string)
}

// Split は複数のステップを並列実行するためのフロー要素のインターフェースです。
type Split interface {
	Steps() []Step
	ID() string // FlowElement インターフェースの実装
}

// JobParametersIncrementer は JobParameters を自動的にインクリメントするためのインターフェースです。
type JobParametersIncrementer interface {
	GetNext(params JobParameters) JobParameters
}
