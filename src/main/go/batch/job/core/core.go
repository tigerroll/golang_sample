package core

import (
  "context"
  "time"
  "github.com/google/uuid" // ID生成のためにuuidパッケージをインポート
)

// Job は実行可能なバッチジョブのインターフェースです。
// Run メソッドが JobExecution を受け取るように変更
type Job interface {
  Run(ctx context.Context, jobExecution *JobExecution) error
  // SimpleJobLauncherでジョブ名を取得するために追加
  JobName() string
  // TODO: ジョブが持つステップやフロー定義を取得するメソッドを追加する必要がある
  // GetSteps() []Step // 例: シンプルにステップのリストを返す場合
  // GetFlow() FlowDefinition // 例: フロー定義構造を返す場合
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

// ExecutionContext はジョブやステップの状態を共有するためのキー-値ストアです。
// 任意の値 (interface{}) を格納できるように map[string]interface{} とします。
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
  return ec[key]
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

// JobParameters はジョブ実行時のパラメータを保持する構造体です。
type JobParameters struct {
  // 例: StartDate string
  // 例: EndDate string
  // 必要に応じてマップなどに変更可能
  // TODO: JSR352 に近づけるなら map[string]interface{} に変更し、型安全な取得メソッドを追加
}

// NewJobParameters は新しい JobParameters のインスタンスを作成します。
func NewJobParameters() JobParameters {
  return JobParameters{}
}

// JobExecution はジョブの単一の実行インスタンスを表す構造体です。
// JobExecution は BatchStatus と ExitStatus を持ちます。
type JobExecution struct {
  ID             string         // 実行を一意に識別するID (通常、永続化層で生成)
  JobName        string
  Parameters     JobParameters
  StartTime      time.Time
  EndTime        time.Time
  Status         JobStatus      // BatchStatus に相当
  ExitStatus     ExitStatus     // 終了時の詳細ステータス
  ExitCode       int            // 終了コード (ここでは単純化のため未使用)
  Failureliye    []error        // 発生したエラー (複数保持できるようにスライスとする)
  Version        int            // バージョン (ここでは単純化のため未使用)
  CreateTime     time.Time
  LastUpdated    time.Time
  StepExecutions []*StepExecution // このジョブ実行に関連するステップ実行
  ExecutionContext ExecutionContext // ジョブレベルのコンテキスト
}

// NewJobExecution は新しい JobExecution のインスタンスを作成します。
func NewJobExecution(jobName string, params JobParameters) *JobExecution {
  now := time.Now()
  return &JobExecution{
    ID:             uuid.New().String(), // 例: uuid パッケージを使用
    JobName:        jobName,
    Parameters:     params,
    StartTime:      now,
    Status:         JobStatusStarting, // 開始時は Starting または Started
    ExitStatus:     ExitStatusUnknown,
    CreateTime:     now,
    LastUpdated:    now,
    Failureliye:    make([]error, 0),
    StepExecutions: make([]*StepExecution, 0),
    ExecutionContext: NewExecutionContext(), // ここで初期化
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
  ID             string         // 実行を一意に識別するID (ここでは単純化のため未使用)
  StepName       string
  JobExecution   *JobExecution  // 所属するジョブ実行への参照
  StartTime      time.Time
  EndTime        time.Time
  Status         JobStatus      // BatchStatus に相当 (JobStatus を流用)
  ExitStatus     ExitStatus     // 終了時の詳細ステータス
  Failureliye    []error        // 発生したエラー (複数保持できるようにスライスとする)
  ReadCount      int
  WriteCount     int
  CommitCount    int
  RollbackCount  int
  ExecutionContext ExecutionContext // ステップレベルのコンテキスト
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
    Failureliye:    make([]error, 0),
    ExecutionContext: NewExecutionContext(), // ここで初期化
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
