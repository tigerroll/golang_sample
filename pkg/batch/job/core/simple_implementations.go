package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// SimpleDecision は Decision インターフェースのシンプルな実装です。
type SimpleDecision struct {
	id string
}

// NewSimpleDecision は新しい SimpleDecision のインスタンスを作成します。
func NewSimpleDecision(id string) *SimpleDecision {
	return &SimpleDecision{id: id}
}

// Decide は常に COMPLETED を返します。
func (d *SimpleDecision) Decide(ctx context.Context, jobExecution *JobExecution, jobParameters JobParameters) (ExitStatus, error) {
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
	// JSON unmarshal された数値は float64 になることがあるため、両方考慮
	if i, ok := val.(int); ok {
		return i, true
	}
	if f, ok := val.(float64); ok {
		return int(f), true
	}
	return 0, false
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

// GetNested はドット区切りのキーを使用してネストされた値を取得します。
// 例: "reader_context.currentIndex"
func (ec ExecutionContext) GetNested(key string) (interface{}, bool) {
	parts := strings.Split(key, ".")
	var currentVal interface{} = ec
	var ok bool = true

	for i, part := range parts {
		if !ok {
			return nil, false
		}

		// 現在の値がマップ型であることを確認
		var currentMap map[string]interface{}
		if m, isEC := currentVal.(ExecutionContext); isEC {
			currentMap = m
		} else if m, isMap := currentVal.(map[string]interface{}); isMap {
			currentMap = m
		} else {
			return nil, false // ネストの途中でマップではない
		}

		currentVal, ok = currentMap[part]
		if !ok {
			return nil, false
		}

		if i < len(parts)-1 { // 最後のパートでない場合、次の値がマップであることを期待
			if _, isMap := currentVal.(ExecutionContext); !isMap {
				if _, isMap := currentVal.(map[string]interface{}); !isMap {
					return nil, false // ネストの途中でマップではない
				}
			}
		}
	}
	return currentVal, ok
}

// PutNested はドット区切りのキーを使用してネストされた値を設定します。
// 中間パスが存在しない場合は自動的に作成します。
func (ec ExecutionContext) PutNested(key string, value interface{}) {
	parts := strings.Split(key, ".")
	currentMap := ec

	for i, part := range parts {
		if i == len(parts)-1 { // 最後のパート
			currentMap.Put(part, value)
		} else { // 中間パート
			nextMapVal, ok := currentMap.Get(part)
			if !ok {
				// 中間マップが存在しない場合、新しいマップを作成
				newMap := NewExecutionContext()
				currentMap.Put(part, newMap)
				currentMap = newMap
			} else {
				if nextMap, isMap := nextMapVal.(ExecutionContext); isMap {
					currentMap = nextMap
				} else if nextMap, isMap := nextMapVal.(map[string]interface{}); isMap {
					currentMap = ExecutionContext(nextMap)
				} else {
					// 既存の値がマップではない場合、上書きして新しいマップを作成
					newMap := NewExecutionContext()
					currentMap.Put(part, newMap)
					currentMap = newMap
					logger.Warnf("ExecutionContext.PutNested: キー '%s' のパスに既存の非マップ値が存在したため上書きしました。", strings.Join(parts[:i+1], "."))
				}
			}
		}
	}
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
	// JSON unmarshal された数値は float64 になることがあるため、両方考慮
	if i, ok := val.(int); ok {
		return i, true
	}
	if f, ok := val.(float64); ok {
		return int(f), true
	}
	return 0, false
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

// Hash は JobParameters のハッシュ値を計算します。
// パラメータの順序に依存しないように、JSON文字列に変換してからハッシュ化します。
func (jp JobParameters) Hash() (string, error) {
	// map[string]interface{} を JSON にマーシャルする際、キーの順序は保証されないため、
	// 安定したハッシュを得るためには、ソートされたキーを持つ構造体などに変換してからマーシャルするか、
	// 独自の正規化ロジックを実装する必要があります。
	// ここではシンプルに、json.Marshal がデフォルトで提供する順序に依存します。
	// 厳密な順序保証が必要な場合は、別途ソートロジックを追加してください。
	jsonBytes, err := json.Marshal(jp.Params)
	if err != nil {
		return "", exception.NewBatchError("job_parameters", "JobParameters のハッシュ計算のためのJSONマーシャルに失敗しました", err, false, false)
	}

	hasher := sha256.New()
	hasher.Write(jsonBytes)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// NewJobInstance は新しい JobInstance のインスタンスを作成します。
func NewJobInstance(jobName string, params JobParameters) *JobInstance {
	now := time.Now()
	hash, err := params.Hash()
	if err != nil {
		logger.Errorf("JobParameters のハッシュ計算に失敗しました: %v", err)
		hash = "" // エラー時は空文字列
	}
	return &JobInstance{
		ID:             uuid.New().String(),
		JobName:        jobName,
		Parameters:     params,
		CreateTime:     now,
		Version:        0,
		ParametersHash: hash,
	}
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
		StepExecutions:   make([]*StepExecution, 0),
		ExecutionContext: NewExecutionContext(),
		CurrentStepName:  "",
		CancelFunc:       nil,
	}
}

// isValidJobTransition は JobExecution の状態遷移が有効かどうかをチェックします。
func isValidJobTransition(current, next JobStatus) bool {
	switch current {
	case BatchStatusStarting:
		return next == BatchStatusStarted || next == BatchStatusFailed || next == BatchStatusStopped || next == BatchStatusAbandoned
	case BatchStatusStarted:
		return next == BatchStatusStopping || next == BatchStatusCompleted || next == BatchStatusFailed || next == BatchStatusAbandoned
	case BatchStatusStopping:
		return next == BatchStatusStopped || next == BatchStatusStoppingFailed || next == BatchStatusFailed || next == BatchStatusAbandoned
	case BatchStatusStopped:
		return next == BatchStatusStarting // Restart のみ
	case BatchStatusCompleted, BatchStatusFailed, BatchStatusAbandoned, BatchStatusStoppingFailed:
		return false // 終了状態からは直接遷移しない (Restart は新しい JobExecution を作成するため)
	default:
		return false
	}
}

// TransitionTo は JobExecution の状態を安全に遷移させます。
func (je *JobExecution) TransitionTo(newStatus JobStatus) error {
	if !isValidJobTransition(je.Status, newStatus) {
		return fmt.Errorf("JobExecution (ID: %s): 不正な状態遷移: %s -> %s", je.ID, je.Status, newStatus)
	}
	je.Status = newStatus
	je.LastUpdated = time.Now()
	return nil
}

// MarkAsStarted は JobExecution の状態を実行中に更新します。
func (je *JobExecution) MarkAsStarted() {
	if err := je.TransitionTo(BatchStatusStarted); err != nil {
		logger.Warnf("JobExecution (ID: %s) の状態を STARTED に更新できませんでした: %v", je.ID, err)
		// エラーを無視して強制的に設定するか、エラーを返すか選択
		je.Status = BatchStatusStarted // 強制的に設定
	}
	je.LastUpdated = time.Now()
}

// MarkAsCompleted は JobExecution の状態を完了に更新します。
func (je *JobExecution) MarkAsCompleted() {
	if err := je.TransitionTo(BatchStatusCompleted); err != nil {
		logger.Warnf("JobExecution (ID: %s) の状態を COMPLETED に更新できませんでした: %v", je.ID, err)
		je.Status = BatchStatusCompleted // 強制的に設定
	}
	je.ExitStatus = ExitStatusCompleted
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
}

// MarkAsFailed は JobExecution の状態を失敗に更新し、エラー情報を追加します。
func (je *JobExecution) MarkAsFailed(err error) {
	if err := je.TransitionTo(BatchStatusFailed); err != nil {
		logger.Warnf("JobExecution (ID: %s) の状態を FAILED に更新できませんでした: %v", je.ID, err)
		je.Status = BatchStatusFailed // 強制的に設定
	}
	je.ExitStatus = ExitStatusFailed
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
	if err != nil {
		je.AddFailureException(err) // AddFailureException を使用
	}
}

// MarkAsStopped は JobExecution の状態を停止に更新します。
func (je *JobExecution) MarkAsStopped() {
	if err := je.TransitionTo(BatchStatusStopped); err != nil {
		logger.Warnf("JobExecution (ID: %s) の状態を STOPPED に更新できませんでした: %v", je.ID, err)
		je.Status = BatchStatusStopped // 強制的に設定
	}
	je.ExitStatus = ExitStatusStopped
	je.EndTime = time.Now()
	je.LastUpdated = time.Now()
}

// AddFailureException は JobExecution にエラー情報を追加します。
// 重複するエラーの追加を避けるように改善します。
func (je *JobExecution) AddFailureException(err error) {
	if err == nil {
		return
	}

	// errors.Is を使用して、既存のエラーと新しいエラーが同じ根本原因を持つかチェック
	for _, existingErr := range je.Failures {
		if errors.Is(existingErr, err) {
			logger.Debugf("JobExecution (ID: %s) に重複するエラー '%v' の追加をスキップしました。", je.ID, err)
			return
		}
	}

	je.Failures = append(je.Failures, err)
	je.LastUpdated = time.Now()
}

// AddStepExecution は JobExecution に StepExecution を追加します。
func (je *JobExecution) AddStepExecution(se *StepExecution) {
	je.StepExecutions = append(je.StepExecutions, se)
}

// NewStepExecution は新しい StepExecution のインスタンスを作成します。
func NewStepExecution(id string, jobExecution *JobExecution, stepName string) *StepExecution {
	now := time.Now()
	se := &StepExecution{
		ID:               id,
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

// isValidStepTransition は StepExecution の状態遷移が有効かどうかをチェックします。
func isValidStepTransition(current, next JobStatus) bool {
	switch current {
	case BatchStatusStarting:
		return next == BatchStatusStarted || next == BatchStatusFailed || next == BatchStatusStopped || next == BatchStatusAbandoned
	case BatchStatusStarted:
		return next == BatchStatusCompleted || next == BatchStatusFailed || next == BatchStatusStopped || next == BatchStatusAbandoned
	case BatchStatusCompleted, BatchStatusFailed, BatchStatusStopped, BatchStatusAbandoned:
		return false // 終了状態からは直接遷移しない
	default:
		return false
	}
}

// TransitionTo は StepExecution の状態を安全に遷移させます。
func (se *StepExecution) TransitionTo(newStatus JobStatus) error {
	if !isValidStepTransition(se.Status, newStatus) {
		return fmt.Errorf("StepExecution (ID: %s): 不正な状態遷移: %s -> %s", se.ID, se.Status, newStatus)
	}
	se.Status = newStatus
	se.LastUpdated = time.Now()
	return nil
}

// MarkAsStarted は StepExecution の状態を実行中に更新します。
func (se *StepExecution) MarkAsStarted() {
	if err := se.TransitionTo(BatchStatusStarted); err != nil {
		logger.Warnf("StepExecution (ID: %s) の状態を STARTED に更新できませんでした: %v", se.ID, err)
		se.Status = BatchStatusStarted // 強制的に設定
	}
	se.LastUpdated = time.Now()
}

// MarkAsCompleted は StepExecution の状態を完了に更新します。
func (se *StepExecution) MarkAsCompleted() {
	if err := se.TransitionTo(BatchStatusCompleted); err != nil {
		logger.Warnf("StepExecution (ID: %s) の状態を COMPLETED に更新できませんでした: %v", se.ID, err)
		se.Status = BatchStatusCompleted // 強制的に設定
	}
	se.ExitStatus = ExitStatusCompleted
	se.EndTime = time.Now()
	se.LastUpdated = time.Now()
}

// MarkAsFailed は StepExecution の状態を失敗に更新し、エラー情報を追加します。
func (se *StepExecution) MarkAsFailed(err error) {
	if err := se.TransitionTo(BatchStatusFailed); err != nil {
		logger.Warnf("StepExecution (ID: %s) の状態を FAILED に更新できませんでした: %v", se.ID, err)
		se.Status = BatchStatusFailed // 強制的に設定
	}
	se.ExitStatus = ExitStatusFailed
	se.EndTime = time.Now()
	se.LastUpdated = time.Now()
	if err != nil {
		se.AddFailureException(err) // AddFailureException を使用
	}
}

// MarkAsStopped は StepExecution の状態を停止に更新します。
func (se *StepExecution) MarkAsStopped() { // ★ 追加
	if err := se.TransitionTo(BatchStatusStopped); err != nil {
		logger.Warnf("StepExecution (ID: %s) の状態を STOPPED に更新できませんでした: %v", se.ID, err)
		se.Status = BatchStatusStopped // 強制的に設定
	}
	se.ExitStatus = ExitStatusStopped
	se.EndTime = time.Now()
	se.LastUpdated = time.Now()
}

// AddFailureException は StepExecution にエラー情報を追加します。
// 重複するエラーの追加を避けるように改善します。
func (se *StepExecution) AddFailureException(err error) {
	if err == nil {
		return
	}

	// errors.Is を使用して、既存のエラーと新しいエラーが同じ根本原因を持つかチェック
	for _, existingErr := range se.Failures {
		if errors.Is(existingErr, err) {
			logger.Debugf("StepExecution (ID: %s) に重複するエラー '%v' の追加をスキップしました。", se.ID, err)
			return
		}
	}

	se.Failures = append(se.Failures, err)
	se.LastUpdated = time.Now()
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
func (fd *FlowDefinition) GetTransitionRule(fromElementID string, exitStatus ExitStatus, isError bool) (Transition, bool) {
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
