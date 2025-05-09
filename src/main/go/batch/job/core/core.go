package core

import (
  "context"
  "time"
  // uuidパッケージが必要になる可能性がありますが、ここでは単純化のためコメントアウト
  //"github.com/google/uuid"
)

// Job は実行可能なバッチジョブのインターフェースです。
// Run メソッドが JobExecution を受け取るように変更
type Job interface {
  Run(ctx context.Context, jobExecution *JobExecution) error
  // SimpleJobLauncherでジョブ名を取得するために追加
  JobName() string
}

// JobStatus はジョブ実行の状態を表します。
type JobStatus string

const (
  JobStatusUnknown   JobStatus = "UNKNOWN"
  JobStatusStarting  JobStatus = "STARTING"
  JobStatusStarted   JobStatus = "STARTED"
  JobStatusStopping  JobStatus = "STOPPING"
  JobStatusStopped   JobStatus = "STOPPED"
  JobStatusCompleted JobStatus = "COMPLETED"
  JobStatusFailed    JobStatus = "FAILED"
  JobStatusAbandoned JobStatus = "ABANDONED"
)

// ExitStatus はジョブ/ステップの終了時の詳細なステータスを表します。
type ExitStatus string

const (
  ExitStatusUnknown   ExitStatus = "UNKNOWN"
  ExitStatusCompleted ExitStatus = "COMPLETED"
  ExitStatusFailed    ExitStatus = "FAILED"
  ExitStatusStopped   ExitStatus = "STOPPED"
  ExitStatusNoOp      ExitStatus = "NO_OP" // 処理なし
)


// JobParameters はジョブ実行時のパラメータを保持する構造体です。
type JobParameters struct {
  // 例: StartDate string
  // 例: EndDate string
  // 必要に応じてマップなどに変更可能
}

// NewJobParameters は新しい JobParameters のインスタンスを作成します。
func NewJobParameters() JobParameters {
  return JobParameters{}
}

// JobExecution はジョブの単一の実行インスタンスを表す構造体です。
// JobExecution は BatchStatus と ExitStatus を持ちます。
type JobExecution struct {
  ID           string // 実行を一意に識別するID (通常、永続化層で生成)
  JobName      string
  Parameters   JobParameters
  StartTime    time.Time
  EndTime      time.Time
  Status       JobStatus    // BatchStatus に相当
  ExitStatus   ExitStatus   // 終了時の詳細ステータス
  ExitCode     int          // 終了コード (ここでは単純化のため未使用)
  Failureliye  []error      // 発生したエラー (複数保持できるようにスライスとする)
  Version      int          // バージョン (ここでは単純化のため未使用)
  CreateTime   time.Time
  LastUpdated  time.Time
  StepExecutions []*StepExecution // このジョブ実行に関連するステップ実行
  // ExecutionContext など、他の属性が必要であれば追加
}

// NewJobExecution は新しい JobExecution のインスタンスを作成します。
func NewJobExecution(jobName string, params JobParameters) *JobExecution {
  now := time.Now()
  return &JobExecution{
    // ID は通常、永続化層で生成されますが、ここでは単純化します。
    // ID:          uuid.New().String(), // 例: uuid パッケージを使用
    JobName:      jobName,
    Parameters:   params,
    StartTime:    now,
    Status:       JobStatusStarting, // 開始時は Starting または Started
    ExitStatus:   ExitStatusUnknown,
    CreateTime:   now,
    LastUpdated:  now,
    Failureliye:  make([]error, 0),
    StepExecutions: make([]*StepExecution, 0),
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
  je.ExitStatus = ExitStatusCompleted
  je.EndTime = time.Now()
  je.LastUpdated = time.Now()
}

// MarkAsFailed は JobExecution の状態を失敗に更新し、エラー情報を追加します。
func (je *JobExecution) MarkAsFailed(err error) {
  je.Status = JobStatusFailed
  je.ExitStatus = ExitStatusFailed
  je.EndTime = time.Now()
  je.LastUpdated = time.Now()
  if err != nil {
    je.Failureliye = append(je.Failureliye, err)
  }
}

// AddFailureException は JobExecution にエラー情報を追加します。
func (je *JobExecution) AddFailureException(err error) {
  if err != nil {
    je.Failureliye = append(je.Failureliye, err)
    je.LastUpdated = time.Now()
  }
}

// StepExecution はステップの単一の実行インスタンスを表す構造体です。
// StepExecution は BatchStatus と ExitStatus を持ちます。
type StepExecution struct {
  ID          string // 実行を一意に識別するID (ここでは単純化のため未使用)
  StepName    string
  JobExecution *JobExecution // 所属するジョブ実行への参照
  StartTime   time.Time
  EndTime     time.Time
  Status      JobStatus  // BatchStatus に相当 (JobStatus を流用)
  ExitStatus  ExitStatus // 終了時の詳細ステータス
  Failureliye []error    // 発生したエラー (複数保持できるようにスライスとする)
  ReadCount   int
  WriteCount  int
  CommitCount int
  RollbackCount int
  // ExecutionContext など、他の属性が必要であれば追加
}

// NewStepExecution は新しい StepExecution のインスタンスを作成します。
func NewStepExecution(stepName string, jobExecution *JobExecution) *StepExecution {
  now := time.Now()
  se := &StepExecution{
    // ID は通常、永続化層で生成されますが、ここでは単純化します。
    // ID:          uuid.New().String(), // 例: uuid パッケージを使用
    StepName:    stepName,
    JobExecution: jobExecution, // JobExecution への参照を設定
    StartTime:   now,
    Status:      JobStatusStarting, // 開始時は Starting または Started
    ExitStatus:  ExitStatusUnknown,
    Failureliye: make([]error, 0),
  }
  // JobExecution にこの StepExecution を追加
  if jobExecution != nil {
    jobExecution.StepExecutions = append(jobExecution.StepExecutions, se)
  }
  return se
}

// MarkAsStarted は StepExecution の状態を実行中に更新します。
func (se *StepExecution) MarkAsStarted() {
  se.Status = JobStatusStarted
}

// MarkAsCompleted は StepExecution の状態を完了に更新します。
func (se *StepExecution) MarkAsCompleted() {
  se.Status = JobStatusCompleted
  se.ExitStatus = ExitStatusCompleted
  se.EndTime = time.Now()
}

// MarkAsFailed は StepExecution の状態を失敗に更新し、エラー情報を追加します。
func (se *StepExecution) MarkAsFailed(err error) {
  se.Status = JobStatusFailed
  se.ExitStatus = ExitStatusFailed
  se.EndTime = time.Now()
  if err != nil {
    se.Failureliye = append(se.Failureliye, err)
  }
}

// AddFailureException は StepExecution にエラー情報を追加します。
func (se *StepExecution) AddFailureException(err error) {
  if err != nil {
    se.Failureliye = append(se.Failureliye, err)
  }
}
