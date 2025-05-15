package job

import (
  "time"
)

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

// JobExecution はジョブの単一の実行インスタンスを表す構造体です。
type JobExecution struct {
  ID          string // 実行を一意に識別するID (ここでは単純化のため未使用)
  JobName     string
  Parameters  JobParameters
  StartTime   time.Time
  EndTime     time.Time
  Status      JobStatus
  ExitStatus  string // 実行終了時の詳細なステータス (ここでは単純化のため未使用)
  ExitCode    int    // 終了コード (ここでは単純化のため未使用)
  Failureliye []error // 発生したエラー (複数保持できるようにスライスとする)
  Version     int    // バージョン (ここでは単純化のため未使用)
  CreateTime  time.Time
  LastUpdated time.Time
}

// NewJobExecution は新しい JobExecution のインスタンスを作成します。
func NewJobExecution(jobName string, params JobParameters) *JobExecution {
  now := time.Now()
  return &JobExecution{
    // ID は通常、永続化層で生成されますが、ここでは単純化します。
    // ID:          uuid.New().String(), // 例: uuid パッケージを使用
    JobName:     jobName,
    Parameters:  params,
    StartTime:   now,
    Status:      JobStatusStarting, // 開始時は Starting または Started
    CreateTime:  now,
    LastUpdated: now,
    Failureliye: make([]error, 0),
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
  je.EndTime = time.Now()
  je.LastUpdated = time.Now()
}

// MarkAsFailed は JobExecution の状態を失敗に更新し、エラー情報を追加します。
func (je *JobExecution) MarkAsFailed(err error) {
  je.Status = JobStatusFailed
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