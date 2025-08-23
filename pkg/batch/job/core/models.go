package core

import (
	"context"
	"time"
)

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
	BatchStatusUnknown        JobStatus = "UNKNOWN"
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

// ToExitStatus は JobStatus を対応する ExitStatus に変換します。
func (s JobStatus) ToExitStatus() ExitStatus { // ★ 追加
	switch s {
	case BatchStatusCompleted:
		return ExitStatusCompleted
	case BatchStatusFailed:
		return ExitStatusFailed
	case BatchStatusStopped:
		return ExitStatusStopped
	case BatchStatusAbandoned:
		return ExitStatusAbandoned
	default:
		return ExitStatusUnknown
	}
}

// ExitStatus はジョブ/ステップの終了時の詳細なステータスを表します。
type ExitStatus string

const (
	ExitStatusUnknown   ExitStatus = "UNKNOWN"
	ExitStatusCompleted ExitStatus = "COMPLETED"
	ExitStatusFailed    ExitStatus = "FAILED"
	ExitStatusStopped   ExitStatus = "STOPPED"
	ExitStatusAbandoned ExitStatus = "ABANDONED"
	ExitStatusNoOp      ExitStatus = "NO_OP"
)

// ExecutionContext はジョブやステップの状態を共有するためのキー-値ストアです。
type ExecutionContext map[string]interface{}

// JobParameters はジョブ実行時のパラメータを保持する構造体です。
type JobParameters struct {
	Params map[string]interface{}
}

// JobInstance はジョブの論理的な実行単位を表す構造体です。
type JobInstance struct {
	ID         string
	JobName    string
	Parameters JobParameters
	CreateTime time.Time
	Version    int
	ParametersHash string
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
	StepExecutions   []*StepExecution
	ExecutionContext ExecutionContext
	CurrentStepName  string
	CancelFunc       context.CancelFunc
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

// ExecutionContextPromotion は StepExecutionContext から JobExecutionContext へのプロモーション設定を定義します。
type ExecutionContextPromotion struct {
	Keys         []string          `yaml:"keys,omitempty"`
	JobLevelKeys map[string]string `yaml:"job-level-keys,omitempty"`
}

// concreteSplit は Split インターフェースの具体的な実装です。
type concreteSplit struct {
	id    string
	steps []Step
}

// NewConcreteSplit は新しい concreteSplit インスタンスを作成し、Split インターフェースとして返します。
// この関数は Split インターフェースを返すため、呼び出し側は具体的な実装に依存しません。
func NewConcreteSplit(id string, steps []Step) Split {
	return &concreteSplit{
		id:    id,
		steps: steps,
	}
}

// ID は concreteSplit のIDを返します。Split インターフェースを実装します。
func (s *concreteSplit) ID() string {
	return s.id
}

// Steps は concreteSplit に含まれるステップのリストを返します。Split インターフェースを実装します。
func (s *concreteSplit) Steps() []Step {
	return s.steps
}
