package repository

import (
  "context"
  "database/sql"
  "encoding/json" // JobParameters, Failureliye, ExecutionContext をJSONとして保存/読み込み
  "fmt"
  "time"

  core "sample/src/main/go/batch/job/core"
  exception "sample/src/main/go/batch/util/exception"
  logger "sample/src/main/go/batch/util/logger"
)

// SQLJobRepository は JobRepository インターフェースの SQL データベース実装です。
type SQLJobRepository struct {
  db *sql.DB
}

// NewSQLJobRepository は新しい SQLJobRepository のインスタンスを作成します。
// 既に確立されたデータベース接続を受け取ります。
func NewSQLJobRepository(db *sql.DB) *SQLJobRepository {
  return &SQLJobRepository{db: db}
}

// SaveJobExecution は新しい JobExecution をデータベースに保存します。
func (r *SQLJobRepository) SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
  // TODO: JobParameters, Failureliye をデータベースの形式に合わせて変換 (例: JSON)
  paramsJSON, err := json.Marshal(jobExecution.Parameters) // 仮の変換
  if err != nil {
    return exception.NewBatchError("job_repository", "JobParameters のエンコードに失敗しました", err)
  }
  failuresJSON, err := json.Marshal(jobExecution.Failureliye) // 仮の変換
  if err != nil {
    return exception.NewBatchError("job_repository", "Failureliye のエンコードに失敗しました", err)
  }

  // ★ 第3段階: ExecutionContext を JSON にシリアライズ
  contextJSONBytes, err := marshalExecutionContext(jobExecution.ExecutionContext)
  if err != nil {
    return exception.NewBatchError("job_repository", "JobExecution ExecutionContext のシリアライズに失敗しました", err)
  }
  contextJSON := string(contextJSONBytes)


  // ★ スキーマ変更の意図を示すコメントを追加
  // job_executions テーブルに execution_context カラム (JSONB or JSON 型) を追加することを想定
  query := `
    INSERT INTO job_executions (id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
  `
  _, err = r.db.ExecContext(
    ctx,
    query,
    jobExecution.ID,
    jobExecution.JobName,
    jobExecution.StartTime,
    sql.NullTime{Time: jobExecution.EndTime, Valid: !jobExecution.EndTime.IsZero()}, // NULLable 対応
    string(jobExecution.Status),
    string(jobExecution.ExitStatus),
    jobExecution.ExitCode, // TODO: exit_code がゼロ値でない場合のみ保存するなど考慮
    jobExecution.CreateTime,
    jobExecution.LastUpdated,
    jobExecution.Version, // TODO: version の管理ロジックが必要
    string(paramsJSON),
    string(failuresJSON),
    contextJSON, // ★ シリアライズした execution_context カラムにバインド
  )
  if err != nil {
    return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の保存に失敗しました", jobExecution.ID), err)
  }

  logger.Debugf("JobExecution (ID: %s) を保存しました。", jobExecution.ID)
  return nil
}

// UpdateJobExecution は既存の JobExecution の状態をデータベースで更新します。
func (r *SQLJobRepository) UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
  // TODO: JobParameters, Failureliye をデータベースの形式に合わせて変換 (例: JSON)
  paramsJSON, err := json.Marshal(jobExecution.Parameters) // 仮の変換
  if err != nil {
    return exception.NewBatchError("job_repository", "JobParameters のエンコードに失敗しました", err)
  }
  failuresJSON, err := json.Marshal(jobExecution.Failureliye) // 仮の変換
  if err != nil {
    return exception.NewBatchError("job_repository", "Failureliye のエンコードに失敗しました", err)
  }

  // ★ 第3段階: ExecutionContext を JSON にシリアライズ
  contextJSONBytes, err := marshalExecutionContext(jobExecution.ExecutionContext)
  if err != nil {
    return exception.NewBatchError("job_repository", "JobExecution ExecutionContext のシリアライズに失敗しました", err)
  }
  contextJSON := string(contextJSONBytes)


  // ★ スキーマ変更の意図を示すコメントを追加
  // job_executions テーブルの execution_context カラムを更新することを想定
  query := `
    UPDATE job_executions
    SET end_time = $1, status = $2, exit_status = $3, exit_code = $4, last_updated = $5, version = $6, job_parameters = $7, failure_exceptions = $8, execution_context = $9
    WHERE id = $10; -- TODO: バージョンによる楽観的ロックを追加することも検討 (WHERE id = $10 AND version = $11)
  `
  res, err := r.db.ExecContext(
    ctx,
    query,
    sql.NullTime{Time: jobExecution.EndTime, Valid: !jobExecution.EndTime.IsZero()}, // NULLable 対応
    string(jobExecution.Status),
    string(jobExecution.ExitStatus),
    jobExecution.ExitCode, // TODO: exit_code がゼロ値でない場合のみ保存するなど考慮
    time.Now(), // 更新時刻を記録
    jobExecution.Version, // TODO: version をインクリメントするなど管理ロジックが必要
    string(paramsJSON),
    string(failuresJSON),
    contextJSON, // ★ シリアライズした execution_context カラムにバインド
    jobExecution.ID,
  )
  if err != nil {
    return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の更新に失敗しました", jobExecution.ID), err)
  }

  rowsAffected, err := res.RowsAffected()
  if err != nil {
    return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の更新結果取得に失敗しました", jobExecution.ID), err)
  }
  if rowsAffected == 0 {
    // TODO: 楽観的ロックを使用する場合、ここで StaleObjectStateException のようなエラーを返す
    return exception.NewBatchErrorf("job_repository", "JobExecution (ID: %s) の更新対象が見つかりませんでした (またはバージョン不一致)", jobExecution.ID)
  }

  logger.Debugf("JobExecution (ID: %s) を更新しました。", jobExecution.ID)
  return nil
}

// FindJobExecutionByID は指定された ID の JobExecution をデータベースから取得します。
func (r *SQLJobRepository) FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error) {
  // ★ スキーマ変更の意図を示すコメントを追加
  // job_executions テーブルから execution_context カラムを取得することを想定
  query := `
    SELECT id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context
    FROM job_executions
    WHERE id = $1;
  `
  row := r.db.QueryRowContext(ctx, query, executionID)

  jobExecution := &core.JobExecution{}
  var endTime sql.NullTime
  var exitStatus sql.NullString
  var exitCode sql.NullInt64 // INTEGER型に対応するNull許容型
  var paramsJSON, failuresJSON sql.NullString // JSONTEXT型に対応するNull許容型
  var contextJSON sql.NullString // ★ execution_context 用の Nullable String

  err := row.Scan(
    &jobExecution.ID,
    &jobExecution.JobName,
    &jobExecution.StartTime,
    &endTime,
    &jobExecution.Status, // string にスキャンされる
    &exitStatus,
    &exitCode,
    &jobExecution.CreateTime,
    &jobExecution.LastUpdated,
    &jobExecution.Version,
    &paramsJSON,
    &failuresJSON,
    &contextJSON, // ★ execution_context カラムをスキャン
  )

  if err != nil {
    if err == sql.ErrNoRows {
      return nil, exception.NewBatchErrorf("job_repository", "JobExecution (ID: %s) が見つかりませんでした", executionID)
    }
    return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の取得に失敗しました", executionID), err)
  }

  jobExecution.EndTime = endTime.Time // Nullable 対応
  if exitStatus.Valid {
    jobExecution.ExitStatus = core.ExitStatus(exitStatus.String)
  }
  if exitCode.Valid {
    jobExecution.ExitCode = int(exitCode.Int64)
  }

  // TODO: job_parameters, failure_exceptions を JSON から構造体に戻す
  if paramsJSON.Valid {
    err = json.Unmarshal([]byte(paramsJSON.String), &jobExecution.Parameters)
    if err != nil {
      // TODO: エラーハンドリング - パースに失敗した場合でもジョブ実行は継続できるか？ログ出力して進む？
       logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", executionID, err)
      // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました", executionID), err)
    }
  }
  if failuresJSON.Valid {
     // failure_exceptions は []error ですが、JSONではstringスライスなどで保持することを想定
    // ここでは簡単なstringスライスとしてデコードする例
    var failureMsgs []string // 仮の型
    err = json.Unmarshal([]byte(failuresJSON.String), &failureMsgs)
    if err != nil {
       logger.Errorf("JobExecution (ID: %s) の failure_exceptions のデコードに失敗しました: %v", executionID, err)
    } else {
       // デコードしたstringスライスを error スライスに戻す（文字列から新しいerrorを作成）
       jobExecution.Failureliye = make([]error, len(failureMsgs))
       for i, msg := range failureMsgs {
         jobExecution.Failureliye[i] = fmt.Errorf(msg)
       }
    }
  }
  // ★ 第4段階: ExecutionContext の JSON データをデシリアライズ
  // jobExecution.ExecutionContext は NewJobExecution で初期化済み
  if contextJSON.Valid {
    err = unmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext) // ヘルパー関数を使用
    if err != nil {
      // TODO: エラーハンドリング - デシリアライズに失敗した場合でもジョブ実行は継続できるか？ログ出力して進む？
      logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", executionID, err)
      // リスタート時に ExecutionContext が取得できないと問題になる可能性が高い
      // エラーを返すか、ログレベルを上げて警告するなど検討
      // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました", executionID), err)
    }
  } else {
    // カラムが NULL の場合は空の ExecutionContext を設定 (NewJobExecution で既に設定済みだが念のため)
    jobExecution.ExecutionContext = core.NewExecutionContext()
  }


  // StepExecutions も取得する必要がある
  stepExecutions, err := r.FindStepExecutionsByJobExecutionID(ctx, jobExecution.ID)
  if err != nil {
     // TODO: StepExecutions の取得に失敗した場合のハンドリング
     logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
     // エラーを返すか、ログ出力して JobExecution だけ返すか検討
     // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました", jobExecution.ID), err)
  }
  jobExecution.StepExecutions = stepExecutions


  logger.Debugf("JobExecution (ID: %s) をデータベースから取得しました。", executionID) // ここを修正: jobExecution.ID -> executionID

  return jobExecution, nil
}

// FindLatestJobExecution は指定されたジョブ名の最新の JobExecution をデータベースから取得します。
func (r *SQLJobRepository) FindLatestJobExecution(ctx context.Context, jobName string) (*core.JobExecution, error) {
  // ★ スキーマ変更の意図を示すコメントを追加
  // job_executions テーブルから execution_context カラムを取得することを想定
  query := `
    SELECT id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context
    FROM job_executions
    WHERE job_name = $1
    ORDER BY create_time DESC
    LIMIT 1;
  `
  row := r.db.QueryRowContext(ctx, query, jobName)

  jobExecution := &core.JobExecution{}
  var endTime sql.NullTime
  var exitStatus sql.NullString
  var exitCode sql.NullInt64
  var paramsJSON, failuresJSON sql.NullString
  var contextJSON sql.NullString // ★ execution_context 用の Nullable String

  err := row.Scan(
    &jobExecution.ID,
    &jobExecution.JobName,
    &jobExecution.StartTime,
    &endTime,
    &jobExecution.Status,
    &exitStatus,
    &exitCode,
    &jobExecution.CreateTime,
    &jobExecution.LastUpdated,
    &jobExecution.Version,
    &paramsJSON,
    &failuresJSON,
    &contextJSON, // ★ execution_context カラムをスキャン
  )

  if err != nil {
    if err == sql.ErrNoRows {
      return nil, exception.NewBatchErrorf("job_repository", "ジョブ '%s' の JobExecution が見つかりませんでした", jobName)
    }
    return nil, exception.NewBatchError("job_repository", fmt.Sprintf("ジョブ '%s' の最新 JobExecution の取得に失敗しました", jobName), err)
  }

  jobExecution.EndTime = endTime.Time
  if exitStatus.Valid {
    jobExecution.ExitStatus = core.ExitStatus(exitStatus.String)
  }
  if exitCode.Valid {
    jobExecution.ExitCode = int(exitCode.Int64)
  }

  // TODO: job_parameters, failure_exceptions を JSON から構造体に戻す
  if paramsJSON.Valid {
    err = json.Unmarshal([]byte(paramsJSON.String), &jobExecution.Parameters)
     if err != nil { logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", jobExecution.ID, err) }
  }
  if failuresJSON.Valid {
    var failureMsgs []string // 仮の型
    err = json.Unmarshal([]byte(failuresJSON.String), &failureMsgs)
    if err != nil { logger.Errorf("JobExecution (ID: %s) の failure_exceptions のデcode に失敗しました: %v", jobExecution.ID, err) } else {
      jobExecution.Failureliye = make([]error, len(failureMsgs))
      for i, msg := range failureMsgs {
        jobExecution.Failureliye[i] = fmt.Errorf(msg)
      }
    }
  }
  // ★ 第4段階: ExecutionContext の JSON データをデシリアライズ
  // jobExecution.ExecutionContext は NewJobExecution で初期化済み
  if contextJSON.Valid {
    err = unmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext) // ヘルパー関数を使用
    if err != nil {
      // TODO: エラーハンドリング - デシリアライズに失敗した場合でもジョブ実行は継続できるか？ログ出力して進む？
      logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", jobExecution.ID, err)
      // リスタート時に ExecutionContext が取得できないと問題になる可能性が高い
      // エラーを返すか、ログレベルを上げて警告するなど検討
      // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました", executionID), err) // ここを修正: executionID -> jobExecution.ID
    }
  } else {
    // カラムが NULL の場合は空の ExecutionContext を設定 (NewJobExecution で既に設定済みだが念のため)
    jobExecution.ExecutionContext = core.NewExecutionContext()
  }


  // StepExecutions も取得する必要がある
  stepExecutions, err := r.FindStepExecutionsByJobExecutionID(ctx, jobExecution.ID)
  if err != nil {
     logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
  }
  jobExecution.StepExecutions = stepExecutions


  logger.Debugf("ジョブ '%s' の最新 JobExecution (ID: %s) をデータベースから取得しました。", jobName, jobExecution.ID)

  return jobExecution, nil
}


// SaveStepExecution は新しい StepExecution をデータベースに保存します。
func (r *SQLJobRepository) SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error {
  // TODO: Failureliye をデータベースの形式に合わせて変換 (例: JSON)
  failuresJSON, err := json.Marshal(stepExecution.Failureliye) // 仮の変換
  if err != nil {
    return exception.NewBatchError("job_repository", "StepExecution Failureliye のエンコードに失敗しました", err)
  }

  // ★ 第5段階: ExecutionContext を JSON にシリアライズ
  contextJSONBytes, err := marshalExecutionContext(stepExecution.ExecutionContext)
  if err != nil {
    return exception.NewBatchError("job_repository", "StepExecution ExecutionContext のシリアライズに失敗しました", err)
  }
  contextJSON := string(contextJSONBytes)


  query := `
    INSERT INTO step_executions (id, job_execution_id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
  `
  // StepExecution に JobExecution への参照がある前提で job_execution_id を取得
  jobExecutionID := ""
  if stepExecution.JobExecution != nil {
    jobExecutionID = stepExecution.JobExecution.ID
  } else {
    // StepExecution は必ず JobExecution に紐づく必要があるためエラーとする
    return exception.NewBatchError("job_repository", "StepExecution が JobExecution に紐づいていません", nil)
  }


  _, err = r.db.ExecContext(
    ctx,
    query,
    stepExecution.ID,
    jobExecutionID,
    stepExecution.StepName,
    stepExecution.StartTime,
    sql.NullTime{Time: stepExecution.EndTime, Valid: !stepExecution.EndTime.IsZero()}, // NULLable 対応
    string(stepExecution.Status),
    string(stepExecution.ExitStatus),
    stepExecution.ReadCount,
    stepExecution.WriteCount,
    stepExecution.CommitCount,
    stepExecution.RollbackCount,
    string(failuresJSON),
    contextJSON, // ★ シリアライズした execution_context カラムにバインド
  )
  if err != nil {
    return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の保存に失敗しました", stepExecution.ID), err)
  }

  logger.Debugf("StepExecution (ID: %s) を保存しました。", stepExecution.ID)
  return nil
}

// UpdateStepExecution は既存の StepExecution の状態をデータベースで更新します。
func (r *SQLJobRepository) UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error {
  // TODO: Failureliye をデータベースの形式に合わせて変換 (例: JSON)
  failuresJSON, err := json.Marshal(stepExecution.Failureliye) // 仮の変換
  if err != nil {
    return exception.NewBatchError("job_repository", "StepExecution Failureliye のエンコードに失敗しました", err)
  }

  // ★ 第5段階: ExecutionContext を JSON にシリアライズ
  contextJSONBytes, err := marshalExecutionContext(stepExecution.ExecutionContext)
  if err != nil {
    return exception.NewBatchError("job_repository", "StepExecution ExecutionContext のシリアライズに失敗しました", err)
  }
  contextJSON := string(contextJSONBytes)


  // ★ スキーマ変更の意図を示すコメントを追加
  // step_executions テーブルの execution_context カラムを更新することを想定
  query := `
    UPDATE step_executions
    SET end_time = $1, status = $2, exit_status = $3, read_count = $4, write_count = $5, commit_count = $6, rollback_count = $7, failure_exceptions = $8, execution_context = $9
    WHERE id = $10;
  `
  res, err := r.db.ExecContext(
    ctx,
    query,
    sql.NullTime{Time: stepExecution.EndTime, Valid: !stepExecution.EndTime.IsZero()}, // NULLable 対応
    string(stepExecution.Status),
    string(stepExecution.ExitStatus),
    stepExecution.ReadCount,
    stepExecution.WriteCount,
    stepExecution.CommitCount,
    stepExecution.RollbackCount,
    string(failuresJSON),
    contextJSON, // ★ シリアライズした execution_context カラムにバインド
    stepExecution.ID,
  )
  if err != nil {
    return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の更新に失敗しました", stepExecution.ID), err)
  }

  rowsAffected, err := res.RowsAffected()
  if err != nil {
    return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の更新結果取得に失敗しました", stepExecution.ID), err)
  }
  if rowsAffected == 0 {
    return exception.NewBatchErrorf("job_repository", "StepExecution (ID: %s) の更新対象が見つかりませんでした", stepExecution.ID)
  }

  logger.Debugf("StepExecution (ID: %s) を更新しました。", stepExecution.ID)
  return nil
}

// FindStepExecutionByID は指定された ID の StepExecution をデータベースから取得します。
func (r *SQLJobRepository) FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error) {
  // ★ スキーマ変更の意図を示すコメントを追加
  // step_executions テーブルから execution_context カラムを取得することを想定
  query := `
    SELECT id, job_execution_id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context
    FROM step_executions
    WHERE id = $1;
  `
  row := r.db.QueryRowContext(ctx, query, executionID)

  stepExecution := &core.StepExecution{}
  var jobExecutionID string // 所属する JobExecution ID を取得用
  var endTime sql.NullTime
  var exitStatus sql.NullString
  var failuresJSON sql.NullString // JSONTEXT型に対応するNull許容型
  var contextJSON sql.NullString // ★ execution_context 用の Nullable String

  err := row.Scan(
    &stepExecution.ID,
    &jobExecutionID, // JobExecution ID を一時変数にスキャン
    &stepExecution.StepName,
    &stepExecution.StartTime,
    &endTime,
    &stepExecution.Status, // string にスキャンされる
    &exitStatus,
    &stepExecution.ReadCount,
    &stepExecution.WriteCount,
    &stepExecution.CommitCount,
    &stepExecution.RollbackCount,
    &failuresJSON,
    &contextJSON, // ★ execution_context カラムをスキャン
  )

  if err != nil {
    if err == sql.ErrNoRows {
      return nil, exception.NewBatchErrorf("job_repository", "StepExecution (ID: %s) が見つかりませんでした", executionID)
    }
    return nil, exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の取得に失敗しました", executionID), err)
  }

  stepExecution.EndTime = endTime.Time // Nullable 対応
   if exitStatus.Valid {
    stepExecution.ExitStatus = core.ExitStatus(exitStatus.String)
  }


  // TODO: failure_exceptions を JSON から構造体に戻す
   if failuresJSON.Valid {
     var failureMsgs []string // 仮の型
    err = json.Unmarshal([]byte(failuresJSON.String), &failureMsgs)
    if err != nil {
       logger.Errorf("StepExecution (ID: %s) の failure_exceptions のデcode に失敗しました: %v", executionID, err)
    } else {
       stepExecution.Failureliye = make([]error, len(failureMsgs))
       for i, msg := range failureMsgs {
         msg := msg // ループ変数再宣言
         stepExecution.Failureliye[i] = fmt.Errorf(msg)
       }
    }
  }
  // ★ 第6段階: ExecutionContext の JSON データをデシリアライズ
  // stepExecution.ExecutionContext は NewStepExecution で初期化済み
  if contextJSON.Valid {
    err = unmarshalExecutionContext([]byte(contextJSON.String), &stepExecution.ExecutionContext) // ヘルパー関数を使用
    if err != nil {
      // TODO: エラーハンドリング - デシリアライズに失敗した場合でもステップ実行は継続できるか？ログ出力して進む？
      logger.Errorf("StepExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", executionID, err)
      // リスタート時に ExecutionContext が取得できないと問題になる可能性が高い
      // エラーを返すか、ログレベルを上げて警告するなど検討
      // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました", executionID), err)
    }
  } else {
    // カラムが NULL の場合は空の ExecutionContext を設定 (NewStepExecution で既に設定済みだが念のため)
    stepExecution.ExecutionContext = core.NewExecutionContext()
  }


  // TODO: JobExecution オブジェクトを JobRepository から取得して StepExecution.JobExecution に設定する必要がある
  // これがないと StepExecution から親の JobExecution にアクセスできない
  // ただし、これは循環参照になる可能性や、不要なデータ取得になる可能性もあるため、設計を検討する必要がある。
  // SimpleJobLauncher などで JobExecution を構築する際に StepExecution を紐づける方法がより一般的。
  // ここではシンプルに JobExecution.ID だけ持たせる設計でも良いかもしれないが、JSR352 では関連を持っていることが多い。
  // Option 1: StepExecution に JobExecution のポインタを持たせる（今回 core.go で採用済み）
  // Option 2: StepExecution に JobExecution ID だけ持たせる
  // Option 3: StepExecution 取得時に JobRepository で JobExecution もフェッチして設定する (今のコメント部分)
  // 今回は Option 1 を採用し、FindStepExecutionByID で JobExecution は取得しないこととする（必要なら別途取得）。

  logger.Debugf("StepExecution (ID: %s) をデータベースから取得しました。", executionID) // ここを修正: stepExecution.ID -> executionID

  return stepExecution, nil
}


// FindStepExecutionsByJobExecutionID は指定された JobExecution ID に関連する全ての StepExecution をデータベースから取得します。
func (r *SQLJobRepository) FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error) {
  // ★ スキーマ変更の意図を示すコメントを追加
  // step_executions テーブルから execution_context カラムを取得することを想定
  query := `
    SELECT id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context
    FROM step_executions
    WHERE job_execution_id = $1
    ORDER BY start_time ASC; -- ステップ実行順に並べる
  `
  rows, err := r.db.QueryContext(ctx, query, jobExecutionID)
  if err != nil {
    return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecution の取得に失敗しました", jobExecutionID), err)
  }
  defer rows.Close()

  var stepExecutions []*core.StepExecution
  for rows.Next() {
    stepExecution := &core.StepExecution{}
    var endTime sql.NullTime
    var exitStatus sql.NullString
    var failuresJSON sql.NullString
    var contextJSON sql.NullString // ★ execution_context 用の Nullable String

    err := rows.Scan(
      &stepExecution.ID,
      &stepExecution.StepName,
      &stepExecution.StartTime,
      &endTime,
      &stepExecution.Status,
      &exitStatus,
      &stepExecution.ReadCount,
      &stepExecution.WriteCount,
      &stepExecution.CommitCount,
      &stepExecution.RollbackCount,
      &failuresJSON,
      &contextJSON, // ★ execution_context カラムをスキャン
    )
    if err != nil {
       // TODO: スキャン中にエラーが発生した場合のハンドリング (部分的な結果を返すか、エラーで中断するか)
       logger.Errorf("StepExecution のスキャン中にエラーが発生しました (JobExecution ID: %s): %v", jobExecutionID, err)
       // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution のスキャンに失敗しました (JobExecution ID: %s)", jobExecutionID), err)
       continue // エラーアイテムをスキップして続行する例
    }

    stepExecution.EndTime = endTime.Time
     if exitStatus.Valid {
      stepExecution.ExitStatus = core.ExitStatus(exitStatus.String)
    }

     // TODO: failure_exceptions を JSON から構造体に戻す
     if failuresJSON.Valid {
       var failureMsgs []string // 仮の型
      err = json.Unmarshal([]byte(failuresJSON.String), &failureMsgs)
      if err != nil {
         logger.Errorf("StepExecution (ID: %s) の failure_exceptions のデcode に失敗しました: %v", stepExecution.ID, err)
      } else {
         stepExecution.Failureliye = make([]error, len(failureMsgs))
         for i, msg := range failureMsgs {
           msg := msg // ループ変数再宣言
           stepExecution.Failureliye[i] = fmt.Errorf(msg)
         }
      }
    }
    // ★ 第6段階: ExecutionContext の JSON データをデシリアライズ
    // stepExecution.ExecutionContext は NewStepExecution で初期化済み
    if contextJSON.Valid {
      err = unmarshalExecutionContext([]byte(contextJSON.String), &stepExecution.ExecutionContext) // ヘルパー関数を使用
      if err != nil {
        // TODO: エラーハンドリング - デシリアライズに失敗した場合でもステップ実行は継続できるか？ログ出力して進む？
        logger.Errorf("StepExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", stepExecution.ID, err)
        // リスタート時に ExecutionContext が取得できないと問題になる可能性が高い
        // エラーを返すか、ログレベルを上げて警告するなど検討
        // return nil, exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました", stepExecution.ID), err)
      }
    } else {
      // カラムが NULL の場合は空の ExecutionContext を設定 (NewStepExecution で既に設定済みだが念のため)
      stepExecution.ExecutionContext = core.NewExecutionContext()
    }


    // StepExecution に所属する JobExecution のポインタを設定する (後で呼び出し元で設定することが多い)
    // stepExecution.JobExecution = // 呼び出し元で JobExecution を取得して設定する必要がある

    stepExecutions = append(stepExecutions, stepExecution)
  }

   if err := rows.Err(); err != nil {
     // QueryContext 自体のエラーではなく、行の処理中に発生したエラー
     return stepExecutions, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecution の取得後の行処理中にエラーが発生しました", jobExecutionID), err)
   }


  logger.Debugf("JobExecution (ID: %s) に関連する %d 件の StepExecution を取得しました。", jobExecutionID, len(stepExecutions))

  return stepExecutions, nil
}


// Close はデータベース接続を閉じます。
func (r *SQLJobRepository) Close() error {
  if r.db != nil {
    err := r.db.Close()
    if err != nil {
      return exception.NewBatchError("job_repository", "データベース接続を閉じるのに失敗しました", err)
    }
    logger.Debugf("Job Repository のデータベース接続を閉じました。")
  }
  return nil
}

// SQLJobRepository が JobRepository インターフェースを満たすことを確認
var _ JobRepository = (*SQLJobRepository)(nil)

// ★ 第2段階で追加されたヘルパー関数
// marshalExecutionContext は ExecutionContext を JSON バイトスライスにシリアライズします。
func marshalExecutionContext(ctx core.ExecutionContext) ([]byte, error) {
  if ctx == nil {
    // nil の場合は空の JSON オブジェクトを表すバイトスライスを返す
    return []byte("{}"), nil
  }
  // JSON にシリアライズ
  data, err := json.Marshal(ctx)
  if err != nil {
    // シリアライズ失敗時はエラーを返す
    return nil, fmt.Errorf("ExecutionContext のシリアライズに失敗しました: %w", err)
  }
  return data, nil
}

// unmarshalExecutionContext は JSON バイトスライスを ExecutionContext にデシリアライズします。
// デシリアライズ結果は既存の ExecutionContext マップに格納されます。
// 既存のマップが nil の場合は新しいマップを作成します。
func unmarshalExecutionContext(data []byte, ctx *core.ExecutionContext) error {
  if len(data) == 0 || string(data) == "null" {
    // データが空または "null" の場合は、ExecutionContext を空にする
    if *ctx == nil {
      *ctx = core.NewExecutionContext() // nil の場合は新しいマップを作成
    } else {
      // 既存のマップをクリア
      for k := range *ctx {
        delete(*ctx, k)
      }
    }
    return nil
  }

  // デシリアライズ対象のマップを初期化またはクリア
  if *ctx == nil {
    *ctx = core.NewExecutionContext() // nil の場合は新しいマップを作成
  } else {
    // 既存のマップをクリア
    for k := range *ctx {
      delete(*ctx, k)
    }
  }

  // JSON からマップにデシリアライズ
  err := json.Unmarshal(data, ctx)
  if err != nil {
    // デシリアライズ失敗時はエラーを返す
    return fmt.Errorf("ExecutionContext のデシリアライズに失敗しました: %w", err)
  }
  return nil
}


// TODO: Failureliye の永続化・復元ヘルパー関数またはメソッドを追加 (必要に応じて検討)
// error 型は Marshal/Unmarshal が標準でサポートしていないため、エラーメッセージの文字列リストとして保存するなどの工夫が必要
// func marshalErrors(errs []error) ([]byte, error) { ... }
// func unmarshalErrors(data []byte) ([]error, error) { ... }
