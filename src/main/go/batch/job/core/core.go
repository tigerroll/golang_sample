package core

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
)

// FlowElement はフロー内の要素（StepまたはDecision）の共通インターフェースです。
type FlowElement interface {
	ID() string // 要素のIDを返すメソッド
}

// Job は実行可能なバッチジョブのインターフェースです。
type Job interface {
	Run(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) error // ★ 修正: jobParameters を追加
	JobName() string
	GetFlow() *FlowDefinition // ★ 修正: コメントアウトを解除
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
	Decide(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) (ExitStatus, error) // ★ 修正: jobParameters を受け取る
	DecisionName() string
	ID() string // FlowElement インターフェースの実装
}

// SimpleDecision は Decision インターフェースのシンプルな実装です。
type SimpleDecision struct {
	id string
}

// NewSimpleDecision は新しい SimpleDecision のインスタンスを作成します。
func NewSimpleDecision(id string) *SimpleDecision {
	return &SimpleDecision{id: id}
}

// Decide は常に COMPLETED を返します。
func (d *SimpleDecision) Decide(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) (ExitStatus, error) { // ★ 修正: jobParameters を受け取る
	logger.Debugf("SimpleDecision '%s' が呼び出されました。常に COMPLETED を返します。", d.id)
	return ExitStatusCompleted, nil
}

// DecisionName は Decision の名前を返します。
func (d *SimpleDecision) DecisionName() string {
	return d.id
}

// ID は Decision のIDを返します。
func (d *SimpleDecision) ID() string {
	return d.id
}

// JobStatus はジョブ実行の状態を表します。
type JobStatus string

const (
	BatchStatusStarting       JobStatus = "STARTING"
	BatchStatusStarted        JobStatus = "STARTED"
	BatchStatusStopping       JobStatus = "STOPPING"
	BatchStatusStopped        JobStatus = "STOPPED"
	BatchStatusCompleted      JobStatus = "COMPLETED"
	BatchStatusFailed         JobStatus = "FAILED"
	BatchStatusAbandoned      JobStatus = "ABANDONED"
	BatchStatusCompleting     JobStatus = "COMPLETING"
	BatchStatusStoppingFailed JobStatus = "STOPPING_FAILED"
	BatchStatusUnknown        JobStatus = "UNKNOWN" // ★ 修正: UNKNOWN を追加
)

// IsFinished は JobStatus が終了状態かどうかを判定するヘルパーメソッドです。
func (s JobStatus) IsFinished() bool {
	switch s {
	case BatchStatusCompleted, BatchStatusFailed, BatchStatusStopped, BatchStatusAbandoned:
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
	ExitStatusNoOp      ExitStatus = "NO_OP"
)

// ExecutionContext はジョブやステップの状態を共有するためのキー-値ストアです。
type ExecutionContext map[string]interface{}

// NewExecutionContext は新しい空の ExecutionContext を作成します。
func NewExecutionContext() ExecutionContext {
	return make(ExecutionContext)
}

// Put は指定されたキーと値で ExecutionContext に値を設定します。
func (ec ExecutionContext) Put(key string, value interface{}) {
	ec[key] = value
}

// Get は指定されたキーの値を取得します。値が存在しない場合は nil と false を返します。
func (ec ExecutionContext) Get(key string) (interface{}, bool) {
	val, ok := ec[key]
	return val, ok
}

// GetString は指定されたキーの値を文字列として取得します。
func (ec ExecutionContext) GetString(key string) (string, bool) {
	val, ok := ec[key]
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

// GetInt は指定されたキーの値をintとして取得します。
func (ec ExecutionContext) GetInt(key string) (int, bool) {
	val, ok := ec[key]
	if !ok {
		return 0, false
	}
	i, ok := val.(int)
	return i, ok
}

// GetBool は指定されたキーの値をboolとして取得します。
func (ec ExecutionContext) GetBool(key string) (bool, bool) {
	val, ok := ec[key]
	if !ok {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}

// GetFloat64 は指定されたキーの値をfloat64として取得します。
func (ec ExecutionContext) GetFloat64(key string) (float64, bool) {
	val, ok := ec[key]
	if !ok {
		return 0.0, false
	}
	f, ok := val.(float64)
	return f, ok
}

// Copy は ExecutionContext のシャローコピーを作成します。
func (ec ExecutionContext) Copy() ExecutionContext {
	newEC := make(ExecutionContext, len(ec))
	for k, v := range ec {
		newEC[k] = v
	}
	return newEC
}

// JobParameters はジョブ実行時のパラメータを保持する構造体です。
type JobParameters struct {
	Params map[string]interface{}
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
		return nil
	}
	return val
}

// GetString は指定されたキーの値を文字列として取得します。
func (jp JobParameters) GetString(key string) (string, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	return str, ok
}

// GetInt は指定されたキーの値をintとして取得します。
func (jp JobParameters) GetInt(key string) (int, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return 0, false
	}
	i, ok := val.(int)
	return i, ok
}

// GetBool は指定されたキーの値をboolとして取得します。
func (jp JobParameters) GetBool(key string) (bool, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}

// GetFloat64 は指定されたキーの値をfloat64として取得します。
func (jp JobParameters) GetFloat64(key string) (float64, bool) {
	val, ok := jp.Params[key]
	if !ok {
		return 0.0, false
	}
	f, ok := val.(float64)
	return f, ok
}

// Equal は2つの JobParameters が等しいかどうかを比較します。
func (jp JobParameters) Equal(other JobParameters) bool {
	return reflect.DeepEqual(jp.Params, other.Params)
}

// JobInstance はジョブの論理的な実行単位を表す構造体です。
type JobInstance struct {
	ID         string
	JobName    string
	Parameters JobParameters
	CreateTime time.Time
	Version    int
}

// NewJobInstance は新しい JobInstance のインスタンスを作成します。
func NewJobInstance(jobName string, params JobParameters) *JobInstance {
	now := time.Now()
	return &JobInstance{
		ID:         uuid.New().String(),
		JobName:    jobName,
		Parameters: params,
		CreateTime: now,
		Version:    0,
	}
}

// JobExecution はジョブの単一の実行インスタンスを表す構造体です。
type JobExecution struct {
	ID               string
	JobInstanceID    string
	JobName          string
	Parameters       JobParameters
	StartTime        time.Time
	EndTime          time.Time
	Status           JobStatus
	ExitStatus       ExitStatus
	ExitCode         int
	Failures         []error
	Version          int
	CreateTime       time.Time
	LastUpdated      time.Time
	StepExecutions   []*StepExecution // ★ 修正: StepExecutions フィールドを追加
	ExecutionContext ExecutionContext
	CurrentStepName  string
}

// NewJobExecution は新しい JobExecution のインスタンスを作成します。
func NewJobExecution(jobInstanceID string, jobName string, params JobParameters) *JobExecution {
	now := time.Now()
	return &JobExecution{
		ID:               uuid.New().String(),
		JobInstanceID:    jobInstanceID,
		JobName:          jobName,
		Parameters:       params,
		StartTime:        now,
		Status:           BatchStatusStarting,
		ExitStatus:       ExitStatusUnknown,
		CreateTime:       now,
		LastUpdated:      now,
		Failures:         make([]error, 0),
		StepExecutions:   make([]*StepExecution, 0), // ★ 修正: スライスを初期化
		ExecutionContext: NewExecutionContext(),
		CurrentStepName:  "",
	}
}

// MarkAsStarted は JobExecution の状態を実行中に更新します。
func (je *JobExecution) MarkAsStarted() {
	je.Status = BatchStatusStarted
	je.LastUpdated = time.Now()
}

// MarkAsCompleted は JobExecution の状態を完了に更新します。
func (je *JobExecution) MarkAsCompleted() {
	je.Status = BatchStatusCompleted
	je.ExitStatus = ExitStatusCompleted
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
}

// MarkAsFailed は JobExecution の状態を失敗に更新し、エラー情報を追加します。
func (je *JobExecution) MarkAsFailed(err error) {
	je.Status = BatchStatusFailed
	je.ExitStatus = ExitStatusFailed
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
	if err != nil {
		je.Failures = append(je.Failures, err)
	}
}

// MarkAsStopped は JobExecution の状態を停止に更新します。
func (je *JobExecution) MarkAsStopped() { // ★ 追加
	je.Status = BatchStatusStopped
	je.ExitStatus = ExitStatusStopped
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
}

// AddFailureException は JobExecution にエラー情報を追加します。
func (je *JobExecution) AddFailureException(err error) {
	if err == nil {
		return
	}

	newBatchErr, isNewBatchErr := err.(*exception.BatchError)

	for _, existingErr := range je.Failures {
		existingBatchErr, isExistingBatchErr := existingErr.(*exception.BatchError)

		if isNewBatchErr && isExistingBatchErr {
			if newBatchErr.Module == existingBatchErr.Module && newBatchErr.Message == existingBatchErr.Message {
				logger.Debugf("JobExecution (ID: %s) に重複する BatchError (Module: %s, Message: %s) の追加をスキップしました。", je.ID, newBatchErr.Module, newBatchErr.Message)
				return
			}
		} else if existingErr.Error() == err.Error() {
			logger.Debugf("JobExecution (ID: %s) に重複するエラー '%s' の追加をスキップしました。", je.ID, err.Error())
			return
		}
	}

	je.Failures = append(je.Failures, err)
	je.LastUpdated = time.Now()
}

// AddStepExecution は JobExecution に StepExecution を追加します。
func (je *JobExecution) AddStepExecution(se *StepExecution) { // ★ 追加
	je.StepExecutions = append(je.StepExecutions, se)
}

// StepExecution はステップの単一の実行インスタンスを表す構造体です。
type StepExecution struct {
	ID               string
	StepName         string
	JobExecution     *JobExecution // 所属するジョブ実行への参照
	StartTime        time.Time
	EndTime          time.Time
	Status           JobStatus
	ExitStatus       ExitStatus
	Failures         []error
	ReadCount        int
	WriteCount       int
	CommitCount      int
	RollbackCount    int
	FilterCount      int
	SkipReadCount    int
	SkipProcessCount int
	SkipWriteCount   int
	ExecutionContext ExecutionContext
	LastUpdated      time.Time
	Version          int
}

// NewStepExecution は新しい StepExecution のインスタンスを作成します。
func NewStepExecution(id string, jobExecution *JobExecution, stepName string) *StepExecution { // ★ 修正: id を引数に追加
	now := time.Now()
	se := &StepExecution{
		ID:               id, // ★ 修正: 引数から ID を設定
		StepName:         stepName,
		JobExecution:     jobExecution,
		StartTime:        now,
		Status:           BatchStatusStarting,
		ExitStatus:       ExitStatusUnknown,
		Failures:         make([]error, 0),
		ExecutionContext: NewExecutionContext(),
		LastUpdated:      now,
		Version:          0,
	}
	return se
}

// MarkAsStarted は StepExecution の状態を実行中に更新します。
func (se *StepExecution) MarkAsStarted() {
	se.Status = BatchStatusStarted
	se.LastUpdated = time.Now()
}

// MarkAsCompleted は StepExecution の状態を完了に更新します。
func (se *StepExecution) MarkAsCompleted() {
	se.Status = BatchStatusCompleted
	se.ExitStatus = ExitStatusCompleted
	se.EndTime = time.Now()
	se.LastUpdated = time.Now()
}

// MarkAsFailed は StepExecution の状態を失敗に更新し、エラー情報を追加します。
func (se *StepExecution) MarkAsFailed(err error) {
	se.Status = BatchStatusFailed
	se.ExitStatus = ExitStatusFailed
	se.EndTime = time.Now()
	se.LastUpdated = time.Now()
	if err != nil {
		se.Failures = append(se.Failures, err)
	}
}

// AddFailureException は StepExecution にエラー情報を追加します。
func (se *StepExecution) AddFailureException(err error) {
	if err == nil {
		return
	}

	newBatchErr, isNewBatchErr := err.(*exception.BatchError)

	for _, existingErr := range se.Failures {
		existingBatchErr, isExistingBatchErr := existingErr.(*exception.BatchError)

		if isNewBatchErr && isExistingBatchErr {
			if newBatchErr.Module == existingBatchErr.Module && newBatchErr.Message == existingBatchErr.Message {
				logger.Debugf("StepExecution (ID: %s) に重複する BatchError (Module: %s, Message: %s) の追加をスキップしました。", se.ID, newBatchErr.Module, newBatchErr.Message)
				return
			}
		} else if existingErr.Error() == err.Error() {
			logger.Debugf("StepExecution (ID: %s) に重複するエラー '%s' の追加をスキップしました。", se.ID, err.Error())
			return
		}
	}

	se.Failures = append(se.Failures, err)
	se.LastUpdated = time.Now()
}

// Transition はステップまたは Decision から次の要素への遷移ルールを定義します。
type Transition struct {
	On   string `yaml:"on"`
	To   string `yaml:"to,omitempty"`
	End  bool   `yaml:"end,omitempty"`
	Fail bool   `yaml:"fail,omitempty"`
	Stop bool   `yaml:"stop,omitempty"`
}

// FlowDefinition はジョブの実行フロー全体を定義します。
type FlowDefinition struct {
	StartElement    string
	Elements        map[string]FlowElement
	TransitionRules []TransitionRule
}

// TransitionRule は特定の遷移元要素からの単一の遷移ルールを定義します。
type TransitionRule struct {
	From       string
	Transition Transition
}

// NewFlowDefinition は新しい FlowDefinition のインスタンスを作成します。
func NewFlowDefinition(startElement string) *FlowDefinition {
	return &FlowDefinition{
		StartElement:    startElement,
		Elements:        make(map[string]FlowElement),
		TransitionRules: make([]TransitionRule, 0),
	}
}

// AddElement はフローにステップまたは Decision を追加します。
func (fd *FlowDefinition) AddElement(name string, element FlowElement) error {
	module := "core"

	if _, exists := fd.Elements[name]; exists {
		err := fmt.Errorf("フロー要素名 '%s' は既に存在します", name)
		logger.Errorf("%v", err)
		return exception.NewBatchError(module, fmt.Sprintf("フロー要素名 '%s' は既に存在します", name), err, false, false)
	}
	fd.Elements[name] = element
	logger.Debugf("フローに要素 '%s' を追加しました。", name)
	return nil
}

// AddTransitionRule はフローに遷移ルールを追加します。
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
	logger.Debugf("フローに遷移ルールを追加しました: From='%s', On='%s', To='%s', End=%t, Fail=%t, Stop=%t", from, on, to, end, fail, stop)
}

// GetTransitionRule は指定された要素と終了ステータスに合致する遷移ルールを検索します。
// isError が true の場合、エラー遷移を優先して検索します。
func (fd *FlowDefinition) GetTransitionRule(fromElementID string, exitStatus ExitStatus, isError bool) (Transition, bool) { // ★ 修正: GetTransitionRule を追加
	// エラー遷移を優先して検索
	if isError {
		for _, rule := range fd.TransitionRules {
			if rule.From == fromElementID && (rule.Transition.On == string(exitStatus) || rule.Transition.On == "*") && rule.Transition.Fail {
				return rule.Transition, true
			}
		}
	}

	// 通常の遷移を検索 (ワイルドカードを含む)
	for _, rule := range fd.TransitionRules {
		if rule.From == fromElementID && (rule.Transition.On == string(exitStatus) || rule.Transition.On == "*") {
			return rule.Transition, true
		}
	}
	return Transition{}, false // 見つからない場合
}

// GetElement は指定された名前のフロー要素を取得します。
func (fd *FlowDefinition) GetElement(name string) (FlowElement, bool) {
	element, ok := fd.Elements[name]
	if ok {
		logger.Debugf("フロー要素 '%s' を取得しました。", name)
	} else {
		logger.Debugf("フロー要素 '%s' は見つかりませんでした。", name)
	}
	return element, ok
}
