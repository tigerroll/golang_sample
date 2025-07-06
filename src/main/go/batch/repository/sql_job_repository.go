package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	core "sample/src/main/go/batch/job/core"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
	serialization "sample/src/main/go/batch/util/serialization" // 新しい serialization パッケージをインポート
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

// GetDB はデータベース接続を返します。
func (r *SQLJobRepository) GetDB() *sql.DB {
	return r.db
}

// --- JobInstance 関連メソッドの実装 ---

// SaveJobInstance は新しい JobInstance をデータベースに保存します。
func (r *SQLJobRepository) SaveJobInstance(ctx context.Context, jobInstance *core.JobInstance) error {
	// JobParameters を JSON にシリアライズ
	paramsJSONBytes, err := serialization.MarshalJobParameters(jobInstance.Parameters) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "JobInstance JobParameters のシリアライズに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	// job_instances テーブルに保存することを想定
	// テーブル構造例: id UUID PRIMARY KEY, job_name VARCHAR(255), job_parameters JSONB, create_time TIMESTAMP, version INTEGER
	query := `
    INSERT INTO job_instances (id, job_name, job_parameters, create_time, version)
    VALUES ($1, $2, $3, $4, $5);
  `
	_, err = r.db.ExecContext(
		ctx,
		query,
		jobInstance.ID,
		jobInstance.JobName,
		paramsJSON, // シリアライズした job_parameters カラムにバインド
		jobInstance.CreateTime,
		jobInstance.Version,
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) の保存に失敗しました", jobInstance.ID), err, false, false)
	}

	logger.Debugf("JobInstance (ID: %s, JobName: %s) を保存しました。", jobInstance.ID, jobInstance.JobName)
	return nil
}

// FindJobInstanceByJobNameAndParameters は指定されたジョブ名とパラメータに一致する JobInstance をデータベースから検索します。
func (r *SQLJobRepository) FindJobInstanceByJobNameAndParameters(ctx context.Context, jobName string, params core.JobParameters) (*core.JobInstance, error) {
	// JobParameters を JSON にシリアライズして比較に使用
	paramsJSONBytes, err := serialization.MarshalJobParameters(params) // Use serialization package
	if err != nil {
		return nil, exception.NewBatchError("job_repository", "検索用 JobParameters のシリアライズに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	// job_instances テーブルから job_name と job_parameters (JSONB 演算子を使用するなど) で検索することを想定
	// データベースの種類によって JSON の比較方法が異なります。ここでは PostgreSQL の JSONB 演算子 @> を使用する例を示します。
	// 他のデータベースの場合は、JSON 文字列として比較するか、パラメータを分解して比較する必要があります。
	// JSON 文字列としての比較は順序などに依存するため、厳密な一致判定には向かない場合があります。
	// より堅牢な実装のためには、JobParameters の各キーと値を個別のカラムに保存し、それらを組み合わせて検索することを検討してください。
	query := `
    SELECT id, job_name, job_parameters, create_time, version
    FROM job_instances
    WHERE job_name = $1 AND job_parameters @> $2; -- PostgreSQL の JSONB 演算子 @> を使用する例
    -- MySQL の場合: WHERE job_name = ? AND JSON_CONTAINS(job_parameters, ?)
    `
	row := r.db.QueryRowContext(ctx, query, jobName, paramsJSON)

	jobInstance := &core.JobInstance{}
	var paramsJSONFromDB sql.NullString

	err = row.Scan(
		&jobInstance.ID,
		&jobInstance.JobName,
		&paramsJSONFromDB, // job_parameters カラムをスキャン
		&jobInstance.CreateTime,
		&jobInstance.Version,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			// 見つからない場合はエラーではなく nil を返すのが一般的
			return nil, nil // exception.NewBatchErrorf("job_repository", "JobInstance (JobName: %s, Parameters: %+v) が見つかりませんでした", jobName, params)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (JobName: %s) の検索に失敗しました", jobName), err, false, false)
	}

	// 取得した job_parameters を JobParameters 構造体に戻す
	if paramsJSONFromDB.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSONFromDB.String), &jobInstance.Parameters) // Use serialization package
		if err != nil {
			// デコード失敗時はエラーとして扱うか、ログ出力して続行するか検討
			logger.Errorf("JobInstance (ID: %s) の JobParameters のデコードに失敗しました: %v", jobInstance.ID, err)
			// return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) の JobParameters のデコードに失敗しました", jobInstance.ID), err)
		}
	} else {
		jobInstance.Parameters = core.NewJobParameters() // NULL の場合は空の JobParameters を設定
	}

	logger.Debugf("JobInstance (ID: %s, JobName: %s) をデータベースから取得しました。", jobInstance.ID, jobInstance.JobName)

	return jobInstance, nil
}

// FindJobInstanceByID は指定された ID の JobInstance をデータベースから取得します。
func (r *SQLJobRepository) FindJobInstanceByID(ctx context.Context, instanceID string) (*core.JobInstance, error) {
	query := `
    SELECT id, job_name, job_parameters, create_time, version
    FROM job_instances
    WHERE id = $1;
  `
	row := r.db.QueryRowContext(ctx, query, instanceID)

	jobInstance := &core.JobInstance{}
	var paramsJSONFromDB sql.NullString

	err := row.Scan(
		&jobInstance.ID,
		&jobInstance.JobName,
		&paramsJSONFromDB,
		&jobInstance.CreateTime,
		&jobInstance.Version,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, exception.NewBatchErrorf("job_repository", "JobInstance (ID: %s) が見つかりませんでした", instanceID)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) の取得に失敗しました", instanceID), err, false, false)
	}

	// 取得した job_parameters を JobParameters 構造体に戻す
	if paramsJSONFromDB.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSONFromDB.String), &jobInstance.Parameters) // Use serialization package
		if err != nil {
			logger.Errorf("JobInstance (ID: %s) の JobParameters のデコードに失敗しました: %v", jobInstance.ID, err)
		}
	} else {
		jobInstance.Parameters = core.NewJobParameters()
	}

	logger.Debugf("JobInstance (ID: %s) をデータベースから取得しました。", instanceID)

	return jobInstance, nil
}

// GetJobInstanceCount は指定されたジョブ名の JobInstance の数を返します。
func (r *SQLJobRepository) GetJobInstanceCount(ctx context.Context, jobName string) (int, error) {
	query := `
    SELECT COUNT(*) FROM job_instances WHERE job_name = $1;
  `
	var count int
	err := r.db.QueryRowContext(ctx, query, jobName).Scan(&count)
	if err != nil {
		return 0, exception.NewBatchError("job_repository", fmt.Sprintf("ジョブ '%s' の JobInstance 数取得に失敗しました", jobName), err, false, false)
	}
	return count, nil
}

// GetJobNames はリポジトリに存在する全てのジョブ名を返します。
func (r *SQLJobRepository) GetJobNames(ctx context.Context) ([]string, error) {
	query := `
    SELECT DISTINCT job_name FROM job_instances ORDER BY job_name;
  `
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", "ジョブ名の取得に失敗しました", err, false, false)
	}
	defer rows.Close()

	var jobNames []string
	for rows.Next() {
		var jobName string
		if err := rows.Scan(&jobName); err != nil {
			logger.Errorf("ジョブ名のスキャン中にエラーが発生しました: %v", err)
			// エラーが発生しても、取得済みのリストは返すか、エラーで中断するか検討
			// ここではエラーをログ出力し、続行する例
			continue
		}
		jobNames = append(jobNames, jobName)
	}

	if err := rows.Err(); err != nil {
		// QueryContext 自体のエラーではなく、行の処理中に発生したエラー
		return jobNames, exception.NewBatchError("job_repository", "ジョブ名取得後の行処理中にエラーが発生しました", err, false, false)
	}

	logger.Debugf("%d 件のジョブ名を取得しました。", len(jobNames))
	return jobNames, nil
}

// --- JobExecution 関連メソッドの実装 ---

// SaveJobExecution は新しい JobExecution をデータベースに保存します。
func (r *SQLJobRepository) SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
	// JobParameters, Failures をデータベースの形式に合わせて変換 (例: JSON)
	paramsJSONBytes, err := serialization.MarshalJobParameters(jobExecution.Parameters) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "JobParameters のエンコードに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	failuresJSONBytes, err := serialization.MarshalFailures(jobExecution.Failures) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	// ExecutionContext を JSON にシリアライズ
	contextJSONBytes, err := serialization.MarshalExecutionContext(jobExecution.ExecutionContext) // serialization パッケージを使用
	if err != nil {
		return exception.NewBatchError("job_repository", "JobExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	// job_executions テーブルに execution_context カラム (JSONB or JSON 型) を追加することを想定
	// job_instance_id カラムを追加することを想定
	query := `
    INSERT INTO job_executions (id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);
  `
	_, err = r.db.ExecContext(
		ctx,
		query,
		jobExecution.ID,
		jobExecution.JobInstanceID, // job_instance_id カラムにバインド
		jobExecution.JobName,
		jobExecution.StartTime,
		sql.NullTime{Time: jobExecution.EndTime, Valid: !jobExecution.EndTime.IsZero()}, // NULLable 対応
		string(jobExecution.Status),
		string(jobExecution.ExitStatus),
		jobExecution.ExitCode, // TODO: exit_code がゼロ値でない場合のみ保存するなど考慮
		jobExecution.CreateTime,
		jobExecution.LastUpdated,
		jobExecution.Version, // TODO: version の管理ロジックが必要
		paramsJSON,            // シリアライズした job_parameters
		failuresJSON,          // シリアライズした failure_exceptions
		contextJSON,           // シリアライズした execution_context カラムにバインド
		jobExecution.CurrentStepName, // current_step_name カラムにバインド
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の保存に失敗しました", jobExecution.ID), err, false, false)
	}

	logger.Debugf("JobExecution (ID: %s, JobInstanceID: %s) を保存しました。", jobExecution.ID, jobExecution.JobInstanceID)
	return nil
}

// UpdateJobExecution は既存の JobExecution の状態をデータベースで更新します。
func (r *SQLJobRepository) UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
	// JobParameters, Failures をデータベースの形式に合わせて変換 (例: JSON)
	paramsJSONBytes, err := serialization.MarshalJobParameters(jobExecution.Parameters) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "JobParameters のエンコードに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	failuresJSONBytes, err := serialization.MarshalFailures(jobExecution.Failures) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	// ExecutionContext を JSON にシリアライズ
	contextJSONBytes, err := serialization.MarshalExecutionContext(jobExecution.ExecutionContext) // serialization パッケージを使用
	if err != nil {
		return exception.NewBatchError("job_repository", "JobExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	// job_executions テーブルの execution_context, current_step_name カラムを更新することを想定
	query := `
    UPDATE job_executions
    SET end_time = $1, status = $2, exit_status = $3, exit_code = $4, last_updated = $5, version = $6, job_parameters = $7, failure_exceptions = $8, execution_context = $9, current_step_name = $10
    WHERE id = $11; -- TODO: バージョンによる楽観的ロックを追加することも検討 (WHERE id = $11 AND version = $12)
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
		paramsJSON,           // シリアライズした job_parameters
		failuresJSON,         // シリアライズした failure_exceptions
		contextJSON,          // シリアライズした execution_context カラムにバインド
		jobExecution.CurrentStepName, // current_step_name カラムにバインド
		jobExecution.ID,
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の更新に失敗しました", jobExecution.ID), err, false, false)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の更新結果取得に失敗しました", jobExecution.ID), err, false, false)
	}
	if rowsAffected == 0 {
		// TODO: 楽観的ロックを使用する場合、ここで StaleObjectStateException のようなエラーを返す
		return exception.NewBatchErrorf("job_repository", "JobExecution (ID: %s) の更新対象が見つかりませんでした (またはバージョン不一致)", jobExecution.ID)
	}

	logger.Debugf("JobExecution (ID: %s) を更新しました。", jobExecution.ID)
	return nil
}

// FindJobExecutionByID は指定された ID の JobExecution をデータベースから取得します。
// 関連する StepExecution もロードします。
func (r *SQLJobRepository) FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error) {
	// job_executions テーブルから job_instance_id, execution_context, current_step_name カラムを取得することを想定
	query := `
    SELECT id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name
    FROM job_executions
    WHERE id = $1;
  `
	row := r.db.QueryRowContext(ctx, query, executionID)

	jobExecution := &core.JobExecution{}
	var endTime sql.NullTime
	var exitStatus sql.NullString
	var exitCode sql.NullInt64 // INTEGER型に対応するNull許容型
	var paramsJSON, failuresJSON sql.NullString // JSONTEXT型に対応するNull許容型
	var contextJSON sql.NullString              // execution_context 用の Nullable String
	var currentStepName sql.NullString          // current_step_name 用の Nullable String

	err := row.Scan(
		&jobExecution.ID,
		&jobExecution.JobInstanceID, // job_instance_id カラムをスキャン
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
		&contextJSON,       // execution_context カラムをスキャン
		&currentStepName, // current_step_name カラムをスキャン
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, exception.NewBatchErrorf("job_repository", "JobExecution (ID: %s) が見つかりませんでした", executionID)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の取得に失敗しました", executionID), err, false, false)
	}

	jobExecution.EndTime = endTime.Time // Nullable 対応
	if exitStatus.Valid {
		jobExecution.ExitStatus = core.ExitStatus(exitStatus.String)
	}
	if exitCode.Valid {
		jobExecution.ExitCode = int(exitCode.Int64)
	}
	if currentStepName.Valid {
		jobExecution.CurrentStepName = currentStepName.String
	} else {
		jobExecution.CurrentStepName = "" // NULL の場合は空文字列を設定
	}

	// JobParameters を JSON から構造体に戻す
	if paramsJSON.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSON.String), &jobExecution.Parameters) // Use serialization package
		if err != nil {
			// TODO: エラーハンドリング - パースに失敗した場合でもジョブ実行は継続できるか？ログ出力して進む？
			logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", executionID, err)
			// return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました", executionID), err)
		}
	} else {
		jobExecution.Parameters = core.NewJobParameters() // NULL の場合は空の JobParameters を設定
	}

	// Failures を JSON から構造体に戻す
	if failuresJSON.Valid {
		jobExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String)) // Use serialization package
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の Failures のデコードに失敗しました: %v", executionID, err)
		}
	} else {
		jobExecution.Failures = make([]error, 0) // NULL の場合は空のスライスを設定
	}

	// ExecutionContext の JSON データをデシリアライズ
	// jobExecution.ExecutionContext は NewJobExecution で初期化済み
	if contextJSON.Valid {
		err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext) // serialization パッケージを使用
		if err != nil {
			// TODO: エラーハンドリング - デシリアライズに失敗した場合でもジョブ実行は継続できるか？ログ出力して進む？
			logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", executionID, err)
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
		// TODO: StepExecutions の取得に失敗した場合のハンドリング
		logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
		// エラーを返すか、ログ出力して JobExecution だけ返すか検討
		// return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました", jobExecution.ID), err)
	}
	// 取得した StepExecution に JobExecution 参照を設定
	for _, se := range stepExecutions {
		se.JobExecution = jobExecution
	}
	jobExecution.StepExecutions = stepExecutions

	logger.Debugf("JobExecution (ID: %s, JobInstanceID: %s) をデータベースから取得しました。", executionID, jobExecution.JobInstanceID)

	return jobExecution, nil
}

// FindLatestJobExecution は指定された JobInstance の最新の JobExecution をデータベースから取得します。
// JSR352 では JobInstance に紐づく JobExecution を検索することが一般的です。
func (r *SQLJobRepository) FindLatestJobExecution(ctx context.Context, jobInstanceID string) (*core.JobExecution, error) {
	// job_executions テーブルから job_instance_id, execution_context, current_step_name カラムを取得することを想定
	query := `
    SELECT id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name
    FROM job_executions
    WHERE job_instance_id = $1
    ORDER BY create_time DESC
    LIMIT 1;
  `
	row := r.db.QueryRowContext(ctx, query, jobInstanceID)

	jobExecution := &core.JobExecution{}
	var endTime sql.NullTime
	var exitStatus sql.NullString
	var exitCode sql.NullInt64
	var paramsJSON, failuresJSON sql.NullString
	var contextJSON sql.NullString     // execution_context 用の Nullable String
	var currentStepName sql.NullString // current_step_name 用の Nullable String

	err := row.Scan(
		&jobExecution.ID,
		&jobExecution.JobInstanceID, // job_instance_id カラムをスキャン
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
		&contextJSON,       // execution_context カラムをスキャン
		&currentStepName, // current_step_name カラムをスキャン
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, exception.NewBatchErrorf("job_repository", "JobInstance (ID: %s) の JobExecution が見つかりませんでした", jobInstanceID)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) の最新 JobExecution の取得に失敗しました", jobInstanceID), err, false, false)
	}

	jobExecution.EndTime = endTime.Time
	if exitStatus.Valid {
		jobExecution.ExitStatus = core.ExitStatus(exitStatus.String)
	}
	if exitCode.Valid {
		jobExecution.ExitCode = int(exitCode.Int64)
	}
	if currentStepName.Valid {
		jobExecution.CurrentStepName = currentStepName.String
	} else {
		jobExecution.CurrentStepName = "" // NULL の場合は空文字列を設定
	}

	// JobParameters を JSON から構造体に戻す
	if paramsJSON.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSON.String), &jobExecution.Parameters) // Use serialization package
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", jobExecution.ID, err)
		}
	} else {
		jobExecution.Parameters = core.NewJobParameters()
	}

	// Failures を JSON から構造体に戻す
	if failuresJSON.Valid {
		jobExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String)) // Use serialization package
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の Failures のデコードに失敗しました: %v", jobExecution.ID, err)
		}
	} else {
		jobExecution.Failures = make([]error, 0)
	}

	// ExecutionContext の JSON データをデシリアライズ
	// jobExecution.ExecutionContext は NewJobExecution で初期化済み
	if contextJSON.Valid {
		err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext) // serialization パッケージを使用
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
	// 取得した StepExecution に JobExecution 参照を設定
	for _, se := range stepExecutions {
		se.JobExecution = jobExecution
	}
	jobExecution.StepExecutions = stepExecutions

	logger.Debugf("JobInstance (ID: %s) の最新 JobExecution (ID: %s) をデータベースから取得しました。", jobInstanceID, jobExecution.ID)

	return jobExecution, nil
}

// FindJobExecutionsByJobInstance は指定された JobInstance に関連する全ての JobExecution をデータベースから検索します。
// JobRepository インターフェースの実装として追加
func (r *SQLJobRepository) FindJobExecutionsByJobInstance(ctx context.Context, jobInstance *core.JobInstance) ([]*core.JobExecution, error) {
	// job_executions テーブルから job_instance_id, execution_context, current_step_name カラムを取得することを想定
	query := `
    SELECT id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name
    FROM job_executions
    WHERE job_instance_id = $1
    ORDER BY create_time ASC; -- 実行順に並べる
  `
	rows, err := r.db.QueryContext(ctx, query, jobInstance.ID)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) に関連する JobExecution の取得に失敗しました", jobInstance.ID), err, false, false)
	}
	defer rows.Close()

	var jobExecutions []*core.JobExecution
	// 親の JobExecution を一度取得しておく (N+1問題を避けるため)
	// FindJobExecutionByID は StepExecutions もロードするため、ここでは循環参照を避けるために
	// JobExecution の基本情報のみをロードするような別のメソッドを呼び出すか、
	// または FindJobExecutionByID が StepExecutions をロードしないように変更する必要があります。
	// 現状の FindJobExecutionByID は StepExecutions もロードするため、ここで呼び出すと無限ループになる可能性があります。
	// ここでは、FindJobExecutionByID が StepExecutions をロードする際に、
	// その StepExecution の JobExecution フィールドは設定しない、という前提で進めます。
	// そして、FindJobExecutionByID の中で、取得した StepExecution の JobExecution フィールドを設定します。
	// したがって、この FindStepExecutionsByJobExecutionID の中では parentJobExecution の取得は行いません。

	for rows.Next() {
		jobExecution := &core.JobExecution{}
		var endTime sql.NullTime
		var exitStatus sql.NullString
		var exitCode sql.NullInt64
		var paramsJSON, failuresJSON sql.NullString
		var contextJSON sql.NullString     // execution_context 用の Nullable String
		var currentStepName sql.NullString // current_step_name 用の Nullable String

		err := rows.Scan(
			&jobExecution.ID,
			&jobExecution.JobInstanceID, // job_instance_id カラムをスキャン
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
			&contextJSON,       // execution_context カラムをスキャン
			&currentStepName, // current_step_name カラムをスキャン
		)
		if err != nil {
			logger.Errorf("JobExecution のスキャン中にエラーが発生しました (JobInstance ID: %s): %v", jobInstance.ID, err)
			continue // エラーアイテムをスキップして続行する例
		}

		jobExecution.EndTime = endTime.Time
		if exitStatus.Valid {
			jobExecution.ExitStatus = core.ExitStatus(exitStatus.String)
		}
		if exitCode.Valid {
			jobExecution.ExitCode = int(exitCode.Int64)
		}
		if currentStepName.Valid {
			jobExecution.CurrentStepName = currentStepName.String
		} else {
			jobExecution.CurrentStepName = ""
		}

		// JobParameters を JSON から構造体に戻す
		if paramsJSON.Valid {
			err = serialization.UnmarshalJobParameters([]byte(paramsJSON.String), &jobExecution.Parameters) // Use serialization package
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", jobExecution.ID, err)
			}
		} else {
			jobExecution.Parameters = core.NewJobParameters()
		}

		// Failures を JSON から構造体に戻す
		if failuresJSON.Valid {
			jobExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String)) // Use serialization package
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) の Failures のデコードに失敗しました: %v", jobExecution.ID, err)
			}
		} else {
			jobExecution.Failures = make([]error, 0)
		}

		// ExecutionContext の JSON データをデシリアライズ
		if contextJSON.Valid {
			err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext) // serialization パッケージを使用
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", jobExecution.ID, err)
			}
		} else {
			jobExecution.ExecutionContext = core.NewExecutionContext()
		}

		// 各 JobExecution に対して StepExecutions をロードし、JobExecution 参照を設定
		stepExecutions, err := r.FindStepExecutionsByJobExecutionID(ctx, jobExecution.ID)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
		}
		for _, se := range stepExecutions {
			se.JobExecution = jobExecution
		}
		jobExecution.StepExecutions = stepExecutions

		jobExecutions = append(jobExecutions, jobExecution)
	}

	if err := rows.Err(); err != nil {
		return jobExecutions, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) に関連する JobExecution 取得後の行処理中にエラーが発生しました", jobInstance.ID), err, false, false)
	}

	logger.Debugf("JobInstance (ID: %s) に関連する %d 件の JobExecution を取得しました。", jobInstance.ID, len(jobExecutions))

	return jobExecutions, nil
}

// --- StepExecution 関連メソッドの実装 ---

// SaveStepExecution は新しい StepExecution をデータベースに保存します。
func (r *SQLJobRepository) SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error {
	// Failures をデータベースの形式に合わせて変換 (例: JSON)
	failuresJSONBytes, err := serialization.MarshalFailures(stepExecution.Failures) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	// ExecutionContext を JSON にシリアライズ
	contextJSONBytes, err := serialization.MarshalExecutionContext(stepExecution.ExecutionContext) // serialization パッケージを使用
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	query := `
    INSERT INTO step_executions (id, job_execution_id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context, last_updated)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
  `
	// StepExecution に JobExecution への参照がある前提で job_execution_id を取得
	jobExecutionID := ""
	if stepExecution.JobExecution != nil {
		jobExecutionID = stepExecution.JobExecution.ID
	} else {
		// StepExecution は必ず JobExecution に紐づく必要があるためエラーとする
		return exception.NewBatchError("job_repository", "StepExecution が JobExecution に紐づいていません", nil, false, false)
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
		failuresJSON, // シリアライズした failure_exceptions
		contextJSON,  // シリアライズした execution_context カラムにバインド
		stepExecution.LastUpdated, // ★ ここを追加 ($14)
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の保存に失敗しました", stepExecution.ID), err, false, false)
	}

	logger.Debugf("StepExecution (ID: %s, JobExecutionID: %s) を保存しました。", stepExecution.ID, jobExecutionID)
	return nil
}

// UpdateStepExecution は既存の StepExecution の状態をデータベースで更新します。
func (r *SQLJobRepository) UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error {
	// Failures をデータベースの形式に合わせて変換 (例: JSON)
	failuresJSONBytes, err := serialization.MarshalFailures(stepExecution.Failures) // Use serialization package
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	// ExecutionContext を JSON にシリアライズ
	contextJSONBytes, err := serialization.MarshalExecutionContext(stepExecution.ExecutionContext) // serialization パッケージを使用
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	query := `
    UPDATE step_executions
    SET end_time = $1, status = $2, exit_status = $3, read_count = $4, write_count = $5, commit_count = $6, rollback_count = $7, failure_exceptions = $8, execution_context = $9, last_updated = $10
    WHERE id = $11;
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
		failuresJSON, // シリアライズした failure_exceptions
		contextJSON,  // シリアライズした execution_context カラムにバインド
		time.Now(),   // ★ ここを追加 ($10)
		stepExecution.ID, // ★ ここが $11 になる
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の更新に失敗しました", stepExecution.ID), err, false, false)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の更新結果取得に失敗しました", stepExecution.ID), err, false, false)
	}
	if rowsAffected == 0 {
		return exception.NewBatchErrorf("job_repository", "StepExecution (ID: %s) の更新対象が見つかりませんでした", stepExecution.ID)
	}

	logger.Debugf("StepExecution (ID: %s) を更新しました。", stepExecution.ID)
	return nil
}

// FindStepExecutionByID は指定された ID の StepExecution をデータベースから取得します。
func (r *SQLJobRepository) FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error) {
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
	var contextJSON sql.NullString  // execution_context 用の Nullable String

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
		&contextJSON, // execution_context カラムをスキャン
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, exception.NewBatchErrorf("job_repository", "StepExecution (ID: %s) が見つかりませんでした", executionID)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の取得に失敗しました", executionID), err, false, false)
	}

	stepExecution.EndTime = endTime.Time // Nullable 対応
	if exitStatus.Valid {
		stepExecution.ExitStatus = core.ExitStatus(exitStatus.String)
	}

	// Failures を JSON から構造体に戻す
	if failuresJSON.Valid {
		stepExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String)) // Use serialization package
		if err != nil {
			logger.Errorf("StepExecution (ID: %s) の Failures のデコードに失敗しました: %v", executionID, err)
		}
	} else {
		stepExecution.Failures = make([]error, 0)
	}

	// ExecutionContext の JSON データをデシリアライズ
	// stepExecution.ExecutionContext は NewStepExecution で初期化済み
	if contextJSON.Valid {
		err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &stepExecution.ExecutionContext) // serialization パッケージを使用
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

	// JobExecution オブジェクトを JobRepository から取得して StepExecution.JobExecution に設定
	// これがないと StepExecution から親の JobExecution にアクセスできない
	if jobExecutionID != "" {
		jobExecution, err := r.FindJobExecutionByID(ctx, jobExecutionID)
		if err != nil {
			logger.Errorf("StepExecution (ID: %s) の親 JobExecution (ID: %s) の取得に失敗しました: %v", executionID, jobExecutionID, err)
			// エラーを返すか、ログ出力して続行するか検討。ここではログ出力して続行。
		} else {
			stepExecution.JobExecution = jobExecution
		}
	}

	logger.Debugf("StepExecution (ID: %s, JobExecutionID: %s) をデータベースから取得しました。", executionID, jobExecutionID)

	return stepExecution, nil
}

// FindStepExecutionsByJobExecutionID は指定された JobExecution ID に関連する全ての StepExecution をデータベースから取得します。
func (r *SQLJobRepository) FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error) {
	// step_executions テーブルから execution_context カラムを取得することを想定
	query := `
    SELECT id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context
    FROM step_executions
    WHERE job_execution_id = $1
    ORDER BY start_time ASC; -- ステップ実行順に並べる
  `
	rows, err := r.db.QueryContext(ctx, query, jobExecutionID)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecution の取得に失敗しました", jobExecutionID), err, false, false)
	}
	defer rows.Close()

	var stepExecutions []*core.StepExecution
	// 親の JobExecution を一度取得しておく (N+1問題を避けるため)
	// FindJobExecutionByID は StepExecutions もロードするため、ここでは循環参照を避けるために
	// JobExecution の基本情報のみをロードするような別のメソッドを呼び出すか、
	// または FindJobExecutionByID が StepExecutions をロードしないように変更する必要があります。
	// 現状の FindJobExecutionByID は StepExecutions もロードするため、ここで呼び出すと無限ループになる可能性があります。
	// ここでは、FindJobExecutionByID が StepExecutions をロードする際に、
	// その StepExecution の JobExecution フィールドは設定しない、という前提で進めます。
	// そして、FindJobExecutionByID の中で、取得した StepExecution の JobExecution フィールドを設定します。
	// したがって、この FindStepExecutionsByJobExecutionID の中では parentJobExecution の取得は行いません。

	for rows.Next() {
		stepExecution := &core.StepExecution{}
		var endTime sql.NullTime
		var exitStatus sql.NullString
		var failuresJSON sql.NullString
		var contextJSON sql.NullString // execution_context 用の Nullable String

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
			&contextJSON, // execution_context カラムをスキャン
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

		// Failures を JSON から構造体に戻す
		if failuresJSON.Valid {
			stepExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String)) // Use serialization package
			if err != nil {
				logger.Errorf("StepExecution (ID: %s) の Failures のデコードに失敗しました: %v", stepExecution.ID, err)
			}
		} else {
			stepExecution.Failures = make([]error, 0)
		}

		// ExecutionContext の JSON データをデシリアライズ
		// stepExecution.ExecutionContext は NewStepExecution で初期化済み
		if contextJSON.Valid {
			err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &stepExecution.ExecutionContext) // serialization パッケージを使用
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
		// ここでは FindJobExecutionByID が StepExecutions をロードする際に、
		// その StepExecution の JobExecution フィールドは設定しない、という前提で、
		// ここでは parentJobExecution の設定は行いません。
		// 呼び出し元 (例: FindJobExecutionByID) が、取得した JobExecution に StepExecutions を設定する際に、
		// 各 StepExecution の JobExecution フィールドも設定する責務を持つべきです。
		// stepExecution.JobExecution = parentJobExecution // 無限再帰を避けるためコメントアウト

		stepExecutions = append(stepExecutions, stepExecution)
	}

	if err := rows.Err(); err != nil {
		// QueryContext 自体のエラーではなく、行の処理中に発生したエラー
		return stepExecutions, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecution の取得後の行処理中にエラーが発生しました", jobExecutionID), err, false, false)
	}

	logger.Debugf("JobExecution (ID: %s) に関連する %d 件の StepExecution を取得しました。", jobExecutionID, len(stepExecutions))

	return stepExecutions, nil
}

// Close はデータベース接続を閉じます。
func (r *SQLJobRepository) Close() error {
	if r.db != nil {
		err := r.db.Close()
		if err != nil {
			return exception.NewBatchError("job_repository", "データベース接続を閉じるのに失敗しました", err, false, false)
		}
		logger.Debugf("Job Repository のデータベース接続を閉じました。")
	}
	return nil
}

// SQLJobRepository が JobRepository インターフェースを満たすことを確認
var _ JobRepository = (*SQLJobRepository)(nil)
