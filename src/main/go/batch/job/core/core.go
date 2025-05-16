package core

import (
  "context"
  "fmt"
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
  val, ok := jp.Params[key]
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
  Failureliye    []error        // 発生したエラー (複数保持できるようにスライスとする)
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
    Failureliye:    make([]error, 0),
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
  //se.StartTime = time.Now() // StartTime は NewStepExecution で設定済み
}

// MarkAsCompleted は StepExecution の状態を完了に更新します。
func (se *StepExecution) MarkAsCompleted() {
  se.Status = JobStatusCompleted
  se.ExitStatus = ExitStatusCompleted // ★ ExitStatus も COMPLETED に設定
  se.EndTime = time.Now()
}

// MarkAsFailed は StepExecution の状態を失敗に更新し、エラー情報を追加します。
func (se *StepExecution) MarkAsFailed(err error) {
  se.Status = JobStatusFailed
  se.ExitStatus = ExitStatusFailed // 失敗時は ExitStatusFailed に設定することが多い
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

// --- フェーズ2: 条件付き遷移のための構造体定義 ---

// Transition はステップまたは Decision から次の要素への遷移ルールを定義します。
// JSR352 の <next> 要素や <end>, <fail>, <stop> 要素に相当します。
type Transition struct {
  // From は遷移元のステップ名または Decision 名です。
  // このフィールドはフロー定義内で使用されるため、Transition 自体には不要かもしれません。
  // 例: <step id="stepA" next="stepB"> または <step id="stepA"> <end on="COMPLETED"/> <next on="FAILED" to="stepC"/> </step>
  // ここではシンプルに、ある要素の終了ステータスに基づいて次の要素を指定する形式とします。

  // On はこの遷移が有効になる遷移元要素の ExitStatus です。
  // ワイルドカード (*) もサポートすることを想定します。
  On string

  // To は遷移先のステップ名または Decision 名です。
  // End, Fail, Stop のいずれかが true の場合は無視されます。
  To string

  // End はこの遷移でジョブを完了させるかどうかを示します。
  End bool
  // EndStatus はジョブを完了させる場合の ExitStatus です (例: COMPLETED, COMPLETED_WITH_WARNINGS)。
  // End が true の場合に有効です。
  EndStatus ExitStatus // JSR352ではExitStatusを文字列で指定することが多い

  // Fail はこの遷移でジョブを失敗させるかどうかを示します。
  Fail bool
  // FailStatus はジョブを失敗させる場合の ExitStatus です (例: FAILED)。
  // Fail が true の場合に有効です。
  FailStatus ExitStatus // JSR352ではExitStatusを文字列で指定することが多い

  // Stop はこの遷移でジョブを停止させるかどうかを示します。
  // Stop が true の場合に有効です。
  Stop bool
  // Restartable は Stop が true の場合に、停止したジョブがリスタート可能かどうかを示します。
  // JSR352のstop要素のrestartable属性に相当します。
  Restartable bool
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

  // Transitions はフロー内の全ての遷移ルールのリストです。
  // 各遷移ルールは、遷移元要素の名前と、その要素の ExitStatus に基づく遷移先を指定します。
  // マップ形式にすることで、遷移元要素ごとにルールを管理しやすくなります。
  // map[string][]TransitionRule のような構造も考えられますが、
  // ここではシンプルに、遷移元要素名と ExitStatus をキーとするマップとします。
  // map[string]map[string]Transition // map[遷移元要素名][ExitStatus]遷移ルール
  // あるいは、遷移元要素の構造体自身が次の遷移ルールを持つ形式も考えられます。
  // 例: Step 構造体に NextTransitions map[ExitStatus]Transition を持たせる
  // JSR352のJob XMLの構造に近づけるため、ここではフロー定義全体で遷移ルールを管理する形式とします。
  // TransitionRule 構造体を別途定義し、From, On, To/End/Fail/Stop を持つ形式がより分かりやすいかもしれません。

  // TransitionRule は特定の遷移元要素からの遷移ルールを定義します。
  // FlowDefinition の Transitions フィールドで使用することを想定します。
  // JSR352の <step> や <decision> 要素内の <next>, <end>, <fail>, <stop> に相当します。
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
  // TODO: element が Step または Decision インターフェースを満たすかチェックする
  // 現状は interface{} なので任意の型を追加可能だが、フロー実行エンジンで処理できる型に限定する必要がある
  if _, exists := fd.Elements[name]; exists {
    return fmt.Errorf("フロー要素名 '%s' は既に存在します", name)
  }
  fd.Elements[name] = element
  return nil
}

// AddTransitionRule はフローに遷移ルールを追加します。
func (fd *FlowDefinition) AddTransitionRule(from string, on string, to string, end bool, endStatus ExitStatus, fail bool, failStatus ExitStatus, stop bool, restartable bool) {
  rule := TransitionRule{
    From: from,
    Transition: Transition{
      On:          on,
      To:          to,
      End:         end,
      EndStatus:   endStatus,
      Fail:        fail,
      FailStatus:  failStatus,
      Stop:        stop,
      Restartable: restartable,
    },
  }
  fd.TransitionRules = append(fd.TransitionRules, rule)
}

// FindTransition は指定された遷移元要素名と ExitStatus に一致する遷移ルールを検索します。
// 複数のルールが一致する場合（例: ワイルドカードと具体的なステータス）、より具体的なルールを優先するなどの
// 解決ロジックが必要になりますが、ここではシンプルに最初に見つかった一致ルールを返します。
// 一致するルールが見つからない場合は nil を返します。
func (fd *FlowDefinition) FindTransition(from string, exitStatus ExitStatus) *Transition {
  // TODO: ワイルドカード (*) の考慮や、より具体的なルールを優先するロジックを追加
  // JSR352では、on="COMPLETED" > on="FAILED" > on="*" の順で評価されるのが一般的です。
  // この検索ロジックはフロー実行エンジン内で実装される可能性が高いですが、
  // ここでは定義構造の一部として FindTransition メソッドのシグネチャのみ示します。

  // シンプルな実装例 (厳密なJSR352のルールではない):
  for _, rule := range fd.TransitionRules {
    if rule.From == from {
      if rule.Transition.On == string(exitStatus) || rule.Transition.On == "*" {
        // ここでさらに具体的なルールを優先するロジックが必要
        // 例: if rule.Transition.On == string(exitStatus) { return &rule.Transition }
        //     if rule.Transition.On == "*" { wildcardMatch = &rule.Transition }
        // ループ後に wildcardMatch を返す
        return &rule.Transition // 一旦、最初に見つかった一致を返す
      }
    }
  }
  return nil // 一致する遷移が見つからない
}

// GetElement は指定された名前のフロー要素を取得します。
func (fd *FlowDefinition) GetElement(name string) (interface{}, bool) {
  element, ok := fd.Elements[name]
  return element, ok
}
