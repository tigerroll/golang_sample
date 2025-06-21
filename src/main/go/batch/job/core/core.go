package core

import (
	"context"
	"fmt"
	"time"
	"github.com/google/uuid" // ID生成のためにuuidパッケージをインポート
	"sample/src/main/go/batch/util/exception" // exception パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

// Job は実行可能なバッチジョブのインターフェースです。
// Run メソッドが JobExecution を受け取るように変更
type Job interface {
	Run(ctx context.Context, jobExecution *JobExecution) error
	// SimpleJobLauncherでジョブ名を取得するために追加
	JobName() string
	// TODO: ジョブが持つステップやフロー定義を取得するメソッドを追加する必要がある
	// GetSteps() []Step // 例: シンプルにステップのリストを返す場合 (現状のWeatherJobはこちらに近い)
	GetFlow() *FlowDefinition // 例: フロー定義構造を返す場合 (フェーズ2以降で導入)
}

// Step はジョブ内で実行される単一のステップのインターフェースです。
// Item-Oriented Step や Tasklet Step の基盤となります。
// JSR352 の Step に相当します。
type Step interface {
	// Execute はステップの処理を実行します。
	// StepExecution のライフサイクル管理（開始/終了マーク、リスナー通知など）はこのメソッドの実装内で行われます。
	// エラーが発生した場合、そのエラーを返します。
	Execute(ctx context.Context, jobExecution *JobExecution, stepExecution *StepExecution) error
	// ステップ名を取得するために追加
	StepName() string
	// TODO: ステップが持つリスナーを取得するメソッドを追加する必要がある (Job が一元管理する場合は不要)
	// GetListeners() []StepExecutionListener
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
// これを実装するリスナーは、必要に応じて各インターフェースに型アサートされます。
type ItemListener interface{}

// Decision はフロー内の条件分岐ポイントのインターフェースを定義します。
type Decision interface {
	// Decide メソッドは、ExecutionContext やその他のパラメータに基づいて次の遷移を決定します。
	Decide(ctx context.Context, jobExecution *JobExecution, stepExecution *StepExecution) (ExitStatus, error)
	DecisionName() string
}


// JobStatus はジョブ実行の状態を表します。
type JobStatus string

const (
	JobStatusUnknown        JobStatus = "UNKNOWN"
	JobStatusStarting       JobStatus = "STARTING"
	JobStatusStarted        JobStatus = "STARTED"
	JobStatusStopping       JobStatus = "STOPPING"
	JobStatusStopped        JobStatus = "STOPPED"
	JobStatusCompleted      JobStatus = "COMPLETED"
	JobStatusFailed         JobStatus = "FAILED"
	JobStatusAbandoned      JobStatus = "ABANDONED"
	JobStatusCompleting     JobStatus = "COMPLETING"      // 完了処理中
	JobStatusStoppingFailed JobStatus = "STOPPING_FAILED" // 停止処理中に失敗
)

// IsFinished は JobStatus が終了状態かどうかを判定するヘルパーメソッドです。
// このメソッドを core パッケージ内で定義します。
func (s JobStatus) IsFinished() bool {
	switch s {
	case JobStatusCompleted, JobStatusFailed, JobStatusStopped, JobStatusAbandoned:
		return true
	default:
		return false
	}
}


// ExitStatus はジョブ/ステップの終了時の詳細なステータスを表します。
type ExitStatus string

const (
	ExitStatusUnknown   ExitStatus = "UNKNOWN"
	ExitStatusCompleted ExitStatus = "COMPLETED"
	ExitStatusFailed    ExitStatus = "FAILED"
	ExitStatusStopped   ExitStatus = "STOPPED"
	ExitStatusNoOp      ExitStatus = "NO_OP" // 処理なし
)

// ExecutionContext はジョブやステップの状態を共有するためのキー-値ストアです。
// 任意の値 (interface{}) を格納できるように map[string]interface{} とします。
// JSR352 の ExecutionContext に相当します。
type ExecutionContext map[string]interface{}

// NewExecutionContext は新しい空の ExecutionContext を作成します。
func NewExecutionContext() ExecutionContext {
	return make(ExecutionContext)
}

// Put は指定されたキーと値で ExecutionContext に値を設定します。
func (ec ExecutionContext) Put(key string, value interface{}) {
	ec[key] = value
}

// Get は指定されたキーの値を取得します。値が存在しない場合は nil を返します。
func (ec ExecutionContext) Get(key string) interface{} {
	val, ok := ec[key]
	if !ok {
		return nil // キーが存在しない場合は nil を返す
	}
	return val
}

// GetString は指定されたキーの値を文字列として取得します。
// 存在しない場合や型が異なる場合は空文字列と false を返します。
func (ec ExecutionContext) GetString(key string) (string, bool) {
	val, ok := ec[key]
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

// GetInt は指定されたキーの値をintとして取得します。
// 存在しない場合や型が異なる場合は0と false を返します。
func (ec ExecutionContext) GetInt(key string) (int, bool) {
	val, ok := ec[key]
	if !ok {
		return 0, false
	}
	i, ok := val.(int)
	return i, ok
}

// GetBool は指定されたキーの値をboolとして取得します。
// 存在しない場合や型が異なる場合は false と false を返します。
func (ec ExecutionContext) GetBool(key string) (bool, bool) {
	val, ok := ec[key]
	if !ok {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}

// GetFloat64 は指定されたキーの値をfloat64として取得します。
// 存在しない場合や型が異なる場合は 0.0 と false を返します。
func (ec ExecutionContext) GetFloat64(key string) (float64, bool) {
	val, ok := ec[key]
	if !ok {
		return 0.0, false
	}
	f, ok := val.(float64)
	return f, ok
}

// Copy は ExecutionContext のシャローコピーを作成します。
// マップのキーと値のペアを新しいマップにコピーします。
// 値が参照型の場合、参照先は元のマップと共有されます。
func (ec ExecutionContext) Copy() ExecutionContext {
	newEC := make(ExecutionContext, len(ec))
	for k, v := range ec {
		newEC[k] = v
	}
	return newEC
}


// JobParameters はジョブ実行時のパラメータを保持する構造体です。
// JSR352 に近づけるため map[string]interface{} とし、型安全な取得メソッドを追加します。
type JobParameters struct {
	Params map[string]interface{} // JSR352に近づけるためマップ形式に
}

// NewJobParameters は新しい JobParameters のインスタンスを作成します。
func NewJobParameters() JobParameters {
	return JobParameters{
		Params: make(map[string]interface{}),
	}
}

// Put は指定されたキーと値で JobParameters に値を設定します。
func (jp JobParameters) Put(key string, value interface{}) {
	jp.Params[key] = value
}

// Get は指定されたキーの値を取得します。値が存在しない場合は nil を返します。
func (jp JobParameters) Get(key string) interface{} {
	val, ok := jp.Params[key]
	if !ok {
		return nil // キーが存在しない場合は nil を返す
	}
	return val
}

// GetString は指定されたキーの値を文字列として取得します。
// 存在しない場合や型が異なる場合は空文字列と false を返します。
func (jp JobParameters) GetString(key string) (string, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

// GetInt は指定されたキーの値をintとして取得します。
// 存在しない場合や型が異なる場合は0と false を返します。
func (jp JobParameters) GetInt(key string) (int, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return 0, false
	}
	i, ok := val.(int)
	return i, ok
}

// GetBool は指定されたキーの値をboolとして取得します。
// 存在しない場合や型が異なる場合は false と false を返します。
func (jp JobParameters) GetBool(key string) (bool, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}

// GetFloat64 は指定されたキーの値をfloat64として取得します。
// 存在しない場合や型が異なる場合は 0.0 と false を返します。
func (jp JobParameters) GetFloat64(key string) (float64, bool) {
	val, ok := jp.Params[key] // ここを修正: jp.Params[key]
	if !ok {
		return 0.0, false
	}
	f, ok := val.(float64)
	return f, ok
}

// Equal は2つの JobParameters が等しいかどうかを比較します。
// JSR352 の JobParameters の同一性判定に相当します。
// マップのキーと値が全て一致する場合に true を返します。
// 値の比較は型に応じて適切に行う必要があります（ここでは簡易的な比較）。
func (jp JobParameters) Equal(other JobParameters) bool {
	if len(jp.Params) != len(other.Params) {
		return false
	}
	for key, val1 := range jp.Params {
		val2, ok := other.Params[key]
		if !ok {
			return false // キーが存在しない
		}
		// 値の比較 (簡易的な比較)
		if val1 != val2 {
			// TODO: より厳密な値の比較（型に応じた比較）を実装
			return false
		}
	}
	return true
}


// JobInstance はジョブの論理的な実行単位を表す構造体です。
// 同じ JobParameters で複数回実行された JobExecution は、同じ JobInstance に属します。
// JSR352 の JobInstance に相当します。
type JobInstance struct {
	ID           string        // JobInstance を一意に識別するID
	JobName      string        // この JobInstance に関連付けられているジョブの名前
	Parameters   JobParameters // この JobInstance を識別するための JobParameters
	CreateTime   time.Time     // JobInstance が作成された時刻
	Version      int           // バージョン (楽観的ロックなどに使用)
	// TODO: JobInstance の状態（完了したか、失敗したかなど）を表すフィールドを追加するか検討
	//       JSR352 では JobInstance 自体は状態を持ちませんが、関連する JobExecution の状態から判断します。
}

// NewJobInstance は新しい JobInstance のインスタンスを作成します。
func NewJobInstance(jobName string, params JobParameters) *JobInstance {
	now := time.Now()
	return &JobInstance{
		ID:         uuid.New().String(), // 例: uuid パッケージを使用
		JobName:    jobName,
		Parameters: params,
		CreateTime: now,
		Version:    0, // 初期バージョン
	}
}


// JobExecution はジョブの単一の実行インスタンスを表す構造体です。
// JobExecution は BatchStatus と ExitStatus を持ちます。
type JobExecution struct {
	ID             string         // 実行を一意に識別するID (通常、永続化層で生成)
	JobInstanceID  string         // ★ 所属する JobInstance の ID を追加
	JobName        string
	Parameters     JobParameters // JobParameters の型は変更なし (内部構造が変更)
	StartTime      time.Time
	EndTime        time.Time
	Status         JobStatus      // BatchStatus に相当
	ExitStatus     ExitStatus     // 終了時の詳細ステータス
	ExitCode       int            // 終了コード (ここでは単純化のため未使用)
	Failures       []error        // 発生したエラー (複数保持できるようにスライスとする)
	Version        int            // バージョン (ここでは単純化のため未使用)
	CreateTime     time.Time
	LastUpdated    time.Time
	StepExecutions []*StepExecution // このジョブ実行に関連するステップ実行
	ExecutionContext ExecutionContext // ジョブレベルのコンテキスト
	CurrentStepName string // 現在実行中のステップ名 (リスタート時に使用)
	// TODO: JobInstance への参照を追加 (JobInstance オブジェクト自体を持つか、ID だけ持つか検討)
	//       ID だけ持つ設計（今回採用）が循環参照を防ぎやすい。必要に応じて JobRepository で JobInstance を取得する。
}

// NewJobExecution は新しい JobExecution のインスタンスを作成します。
// JobInstanceID を引数に追加
func NewJobExecution(jobInstanceID string, jobName string, params JobParameters) *JobExecution {
	now := time.Now()
	return &JobExecution{
		ID:             uuid.New().String(), // 例: uuid パッケージを使用
		JobInstanceID:  jobInstanceID, // ★ JobInstanceID を設定
		JobName:        jobName,
		Parameters:     params, // JobParameters はそのまま代入
		StartTime:      now,
		Status:         JobStatusStarting, // 開始時は Starting または Started
		ExitStatus:     ExitStatusUnknown,
		CreateTime:     now,
		LastUpdated:    now,
		Failures:       make([]error, 0),
		StepExecutions: make([]*StepExecution, 0),
		ExecutionContext: NewExecutionContext(), // ここで初期化
		CurrentStepName: "", // 初期状態では空
	}
}

// MarkAsStarted は JobExecution の状態を実行中に更新します。
func (je *JobExecution) MarkAsStarted() {
	je.Status = JobStatusStarted
	je.LastUpdated = time.Now()
}

// MarkAsCompleted は JobExecution の状態を完了に更新します。
func (je *JobExecution) MarkAsCompleted() {
	je.Status = JobStatusCompleted
	je.ExitStatus = ExitStatusCompleted // ★ ExitStatus も COMPLETED に設定
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
}

// MarkAsFailed は JobExecution の状態を失敗に更新し、エラー情報を追加します。
func (je *JobExecution) MarkAsFailed(err error) {
	je.Status = JobStatusFailed
	je.ExitStatus = ExitStatusFailed // 失敗時は ExitStatusFailed に設定することが多い
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
	if err != nil {
		je.Failures = append(je.Failures, err)
	}
}

// AddFailureException は JobExecution にエラー情報を追加します。
func (je *JobExecution) AddFailureException(err error) {
	if err != nil {
		je.Failures = append(je.Failures, err)
		je.LastUpdated = time.Now()
	}
}

// StepExecution はステップの単一の実行インスタンスを表す構造体です。
// StepExecution は BatchStatus と ExitStatus を持ちます。
type StepExecution struct {
	ID             string         // 実行を一意に識別するID (ここでは単純化のため未使用)
	StepName       string
	JobExecution   *JobExecution  // 所属するジョブ実行への参照
	StartTime      time.Time
	EndTime        time.Time
	Status         JobStatus      // BatchStatus に相当 (JobStatus を流用)
	ExitStatus     ExitStatus     // 終了時の詳細ステータス
	Failures       []error        // 発生したエラー (複数保持できるようにスライスとする)
	ReadCount      int
	WriteCount     int
	CommitCount    int
	RollbackCount  int
	FilterCount    int // 追加
	SkipReadCount  int // 追加
	SkipProcessCount int // 追加
	SkipWriteCount int // 追加
	ExecutionContext ExecutionContext // ステップレベルのコンテキスト
	LastUpdated    time.Time // 追加
	Version        int       // 追加
	// TODO: CheckpointData を追加 (リスタート時に使用)
}

// NewStepExecution は新しい StepExecution のインスタンスを作成します。
// JobExecution への参照を受け取るように変更
func NewStepExecution(stepName string, jobExecution *JobExecution) *StepExecution {
	now := time.Now()
	se := &StepExecution{
		ID:             uuid.New().String(), // 例: uuid パッケージを使用
		StepName:       stepName,
		JobExecution:   jobExecution, // JobExecution への参照を設定
		StartTime:      now,
		Status:         JobStatusStarting, // 開始時は Starting または Started
		ExitStatus:     ExitStatusUnknown,
		Failures:       make([]error, 0),
		ExecutionContext: NewExecutionContext(), // ここで初期化
		LastUpdated:    now, // 追加
		Version:        0,   // 追加
	}
	// JobExecution にこの StepExecution を追加 (JobExecution が nil でない場合)
	if jobExecution != nil {
		jobExecution.StepExecutions = append(jobExecution.StepExecutions, se)
	}
	return se
}

// MarkAsStarted は StepExecution の状態を実行中に更新します。
func (se *StepExecution) MarkAsStarted() {
	se.Status = JobStatusStarted
	//se.StartTime = time.Now() // StartTime は NewStepExecution で設定済み
	se.LastUpdated = time.Now() // 追加
}

// MarkAsCompleted は StepExecution の状態を完了に更新します。
func (se *StepExecution) MarkAsCompleted() {
	se.Status = JobStatusCompleted
	se.ExitStatus = ExitStatusCompleted // ★ ExitStatus も COMPLETED に設定
	se.EndTime = time.Now()
	se.LastUpdated = time.Now() // 追加
}

// MarkAsFailed は StepExecution の状態を失敗に更新し、エラー情報を追加します。
func (se *StepExecution) MarkAsFailed(err error) {
	se.Status = JobStatusFailed
	se.ExitStatus = ExitStatusFailed // 失敗時は ExitStatusFailed に設定することが多い
	se.EndTime = time.Now()
	se.LastUpdated = time.Now() // 追加
	if err != nil {
		se.Failures = append(se.Failures, err)
	}
}

// AddFailureException は StepExecution にエラー情報を追加します。
func (se *StepExecution) AddFailureException(err error) {
	if err != nil {
		se.Failures = append(se.Failures, err)
		se.LastUpdated = time.Now() // 追加
	}
}

// --- Step 実行の共通ロジックのためのヘルパー関数や構造体 ---
// これらは core パッケージに置くか、新しいユーティリティパッケージに置くか検討可能
// ここではシンプルに core パッケージ内にヘルパー関数として追加する例を示します。

// ExecuteStep は与えられた Step の Execute メソッドを呼び出し、
// StepExecution のライフサイクル（開始/終了マーク、リスナー通知）を管理します。
// この関数は Job.Run メソッドから呼び出されることを想定しています。
// TODO: リスナー通知ロジックは Job または Step に持たせるべきか検討
// TODO: JobRepository への StepExecution の保存/更新ロジックもここに含まれるべきか検討
// 現状は Job.Run 内で StepExecution を作成し、JobRepository に保存/更新するが、
// Step の Execute メソッド内で自身（StepExecution）を更新し、JobRepository を使用する設計も考えられる。
// シンプル化のため、ここでは Job.Run が StepExecution のライフサイクルと永続化を管理し、
// Step.Execute は純粋にビジネスロジック（Reader/Processor/Writer）の実行とエラー返却に集中する設計とします。
// そのため、この ExecuteStep ヘルパー関数は今回のステップでは不要になります。
// Step.Execute メソッド内で直接 StepExecution の状態を更新し、Job.Run が永続化を管理します。

// したがって、core.go の修正は Step インターフェースの追加のみとなります。

// --- フェーズ2: 条件付き遷移のための構造体定義 ---

// Transition はステップまたは Decision から次の要素への遷移ルールを定義します。
// JSR352 の <next> 要素や <end>, <fail>, <stop> 要素に相当します。
type Transition struct {
	// On はこの遷移が有効になる遷移元要素の ExitStatus です。
	// ワイルドカード (*) もサポートすることを想定します。
	On string `yaml:"on"`

	// To は遷移先のステップ名または Decision 名です。
	// End, Fail, Stop のいずれかが true の場合は無視されます。
	To string `yaml:"to,omitempty"`

	// End はこの遷移でジョブを完了させるかどうかを示します。
	End bool `yaml:"end,omitempty"`
	// Fail はこの遷移でジョブを失敗させるかどうかを示します。
	Fail bool `yaml:"fail,omitempty"`
	// Stop はこの遷移でジョブを停止させるかどうかを示します。
	Stop bool `yaml:"stop,omitempty"`
}

// FlowDefinition はジョブの実行フロー全体を定義します。
// ステップ、Decision、およびそれらの間の遷移ルールを含みます。
type FlowDefinition struct {
	// StartElement はフローの開始点となるステップまたは Decision の名前です。
	StartElement string

	// Elements はこのフローに含まれる全ての要素（ステップ、Decisionなど）を名前でマッピングしたものです。
	// 実行時に名前解決するために使用します。
	// map[string]interface{} とすることで、Step や Decision など異なる型の要素を保持できます。
	// ただし、型安全性のために map[string]FlowElement のようなインターフェースを定義することも検討できます。
	// ここではシンプルに interface{} とします。
	Elements map[string]interface{} // map[要素名]要素オブジェクト (Step, Decisionなど)

	// TransitionRules はフロー内の全ての遷移ルールのリストです。
	// 各遷移ルールは、遷移元要素の名前と、その要素の ExitStatus に基づく遷移先を指定します。
	TransitionRules []TransitionRule // 遷移ルールリスト

	// TODO: Split 要素（並列実行）の定義を追加 (フェーズ4以降)
}

// TransitionRule は特定の遷移元要素からの単一の遷移ルールを定義します。
type TransitionRule struct {
	// From はこのルールの遷移元となるステップ名または Decision 名です。
	From string

	// Transition はこのルールの詳細な遷移定義です (On, To, End, Fail, Stop など)。
	Transition Transition
}


// NewFlowDefinition は新しい FlowDefinition のインスタンスを作成します。
func NewFlowDefinition(startElement string) *FlowDefinition {
	return &FlowDefinition{
		StartElement:    startElement,
		Elements:        make(map[string]interface{}),
		TransitionRules: make([]TransitionRule, 0),
	}
}

// AddElement はフローにステップまたは Decision を追加します。
func (fd *FlowDefinition) AddElement(name string, element interface{}) error {
	module := "core" // このモジュールの名前を定義

	// TODO: element が Step または Decision インターフェースを満たすかチェックする
	// 現状は interface{} なので任意の型を追加可能だが、フロー実行エンジンで処理できる型に限定する必要がある
	if _, exists := fd.Elements[name]; exists {
		err := fmt.Errorf("フロー要素名 '%s' は既に存在します", name)
		logger.Errorf("%v", err) // エラーログを追加
		return exception.NewBatchError(module, fmt.Sprintf("フロー要素名 '%s' は既に存在します", name), err, false, false) // BatchError でラップ
	}
	fd.Elements[name] = element
	logger.Debugf("フローに要素 '%s' を追加しました。", name) // 成功ログを追加
	return nil
}

// AddTransitionRule はフローに遷移ルールを追加します。
// ExitStatus は Transition 構造体に含まれないため、引数から削除
func (fd *FlowDefinition) AddTransitionRule(from string, on string, to string, end bool, fail bool, stop bool) {
	rule := TransitionRule{
		From: from,
		Transition: Transition{
			On:   on,
			To:   to,
			End:  end,
			Fail: fail,
			Stop: stop,
		},
	}
	fd.TransitionRules = append(fd.TransitionRules, rule)
	logger.Debugf("フローに遷移ルールを追加しました: From='%s', On='%s', To='%s', End=%t, Fail=%t, Stop=%t", from, on, to, end, fail, stop) // 成功ログを追加
}

// FindTransition は指定された遷移元要素名と ExitStatus に一致する遷移ルールを検索します。
// 複数のルールが一致する場合（例: ワイルドカードと具体的なステータス）、より具体的なルールを優先するなどの
// 解決ロジックが必要になりますが、ここではシンプルに最初に見つかった一致ルールを返します。
// 一致するルールが見つからない場合は nil を返します。
func (fd *FlowDefinition) FindTransition(from string, exitStatus ExitStatus) *Transition {
	// JSR352では、on="COMPLETED" > on="FAILED" > on="*" の順で評価されるのが一般的です。
	// ここでは、まず具体的な ExitStatus に一致するルールを探し、次にワイルドカードを探します。
	var wildcardMatch *Transition

	for _, rule := range fd.TransitionRules {
		if rule.From == from {
			if rule.Transition.On == string(exitStatus) {
				logger.Debugf("遷移ルールが見つかりました: From='%s', On='%s' (Exact Match)", from, rule.Transition.On)
				return &rule.Transition // 厳密な一致を優先
			}
			if rule.Transition.On == "*" {
				wildcardMatch = &rule.Transition // ワイルドカードの一致を保持
			}
		}
	}

	if wildcardMatch != nil {
		logger.Debugf("遷移ルールが見つかりました: From='%s', On='*' (Wildcard Match)", from)
		return wildcardMatch // ワイルドカードの一致を返す
	}

	logger.Debugf("一致する遷移ルールが見つかりませんでした: From='%s', ExitStatus='%s'", from, exitStatus) // ログを追加
	return nil // 一致する遷移が見つからない
}

// GetElement は指定された名前のフロー要素を取得します。
func (fd *FlowDefinition) GetElement(name string) (interface{}, bool) {
	element, ok := fd.Elements[name]
	if ok {
		logger.Debugf("フロー要素 '%s' を取得しました。", name) // ログを追加
	} else {
		logger.Debugf("フロー要素 '%s' は見つかりませんでした。", name) // ログを追加
	}
	return element, ok
}
