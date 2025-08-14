package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"sample/pkg/batch/database"
	core "sample/pkg/batch/job/core"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
	serialization "sample/pkg/batch/util/serialization"
)

// SQLJobExecutionRepository は JobExecution インターフェースの SQL データベース実装です。
type SQLJobExecutionRepository struct {
	dbConnection database.DBConnection
	// StepExecutionRepository への参照を保持し、循環参照を解決するためにインターフェースとして持つ
	stepExecutionRepo StepExecution
}

// NewSQLJobExecutionRepository は新しい SQLJobExecutionRepository のインスタンスを作成します。
func NewSQLJobExecutionRepository(dbConn database.DBConnection) *SQLJobExecutionRepository {
	return &SQLJobExecutionRepository{
		dbConnection: dbConn,
	}
}

// SetStepExecutionRepository は StepExecutionRepository の参照を設定します。
// 循環参照を解決するために、コンストラクタではなく別途設定する。
func (r *SQLJobExecutionRepository) SetStepExecutionRepository(repo StepExecution) {
	r.stepExecutionRepo = repo
}

// SaveJobExecution は新しい JobExecution をデータベースに保存します。
func (r *SQLJobExecutionRepository) SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
	paramsJSONBytes, err := serialization.MarshalJobParameters(jobExecution.Parameters)
	if err != nil {
		return exception.NewBatchError("job_repository", "JobParameters のエンコードに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	failuresJSONBytes, err := serialization.MarshalFailures(jobExecution.Failures)
	if err != nil {
		return exception.NewBatchError("job_repository", "Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	contextJSONBytes, err := serialization.MarshalExecutionContext(jobExecution.ExecutionContext)
	if err != nil {
		return exception.NewBatchError("job_repository", "JobExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	query := `
    INSERT INTO job_executions (id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);
  `
	_, err = r.dbConnection.ExecContext(
		ctx,
		query,
		jobExecution.ID,
		jobExecution.JobInstanceID,
		jobExecution.JobName,
		jobExecution.StartTime,
		sql.NullTime{Time: jobExecution.EndTime, Valid: !jobExecution.EndTime.IsZero()},
		string(jobExecution.Status),
		string(jobExecution.ExitStatus),
		jobExecution.ExitCode,
		jobExecution.CreateTime,
		jobExecution.LastUpdated,
		jobExecution.Version,
		paramsJSON,
		failuresJSON,
		contextJSON,
		jobExecution.CurrentStepName,
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の保存に失敗しました", jobExecution.ID), err, false, false)
	}

	logger.Debugf("JobExecution (ID: %s, JobInstanceID: %s) を保存しました。", jobExecution.ID, jobExecution.JobInstanceID)
	return nil
}

// UpdateJobExecution は既存の JobExecution の状態をデータベースで更新します。
func (r *SQLJobExecutionRepository) UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
	paramsJSONBytes, err := serialization.MarshalJobParameters(jobExecution.Parameters)
	if err != nil {
		return exception.NewBatchError("job_repository", "JobParameters のエンコードに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	failuresJSONBytes, err := serialization.MarshalFailures(jobExecution.Failures)
	if err != nil {
		return exception.NewBatchError("job_repository", "Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	contextJSONBytes, err := serialization.MarshalExecutionContext(jobExecution.ExecutionContext)
	if err != nil {
		return exception.NewBatchError("job_repository", "JobExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	query := `
    UPDATE job_executions
    SET end_time = $1, status = $2, exit_status = $3, exit_code = $4, last_updated = $5, version = $6, job_parameters = $7, failure_exceptions = $8, execution_context = $9, current_step_name = $10
    WHERE id = $11;
  `
	res, err := r.dbConnection.ExecContext(
		ctx,
		query,
		sql.NullTime{Time: jobExecution.EndTime, Valid: !jobExecution.EndTime.IsZero()},
		string(jobExecution.Status),
		string(jobExecution.ExitStatus),
		jobExecution.ExitCode,
		time.Now(),
		jobExecution.Version,
		paramsJSON,
		failuresJSON,
		contextJSON,
		jobExecution.CurrentStepName,
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
		return exception.NewBatchErrorf("job_repository", "JobExecution (ID: %s) の更新対象が見つかりませんでした (またはバージョン不一致)", jobExecution.ID)
	}

	logger.Debugf("JobExecution (ID: %s) を更新しました。", jobExecution.ID)
	return nil
}

// FindJobExecutionByID は指定された ID の JobExecution をデータベースから取得します。
// 関連する StepExecution もロードします。
func (r *SQLJobExecutionRepository) FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error) {
	query := `
    SELECT id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name
    FROM job_executions
    WHERE id = $1;
  `
	row := r.dbConnection.QueryRowContext(ctx, query, executionID)

	jobExecution := &core.JobExecution{}
	var endTime sql.NullTime
	var exitStatus sql.NullString
	var exitCode sql.NullInt64
	var paramsJSON, failuresJSON sql.NullString
	var contextJSON sql.NullString
	var currentStepName sql.NullString

	err := row.Scan(
		&jobExecution.ID,
		&jobExecution.JobInstanceID,
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
		&contextJSON,
		&currentStepName,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, exception.NewBatchErrorf("job_repository", "JobExecution (ID: %s) が見つかりませんでした", executionID)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) の取得に失敗しました", executionID), err, false, false)
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

	if paramsJSON.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSON.String), &jobExecution.Parameters)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", executionID, err)
		}
	} else {
		jobExecution.Parameters = core.NewJobParameters()
	}

	if failuresJSON.Valid {
		jobExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String))
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の Failures のデコードに失敗しました: %v", executionID, err)
		}
	} else {
		jobExecution.Failures = make([]error, 0)
	}

	if contextJSON.Valid {
		err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", executionID, err)
		}
	} else {
		jobExecution.ExecutionContext = core.NewExecutionContext()
	}

	// StepExecutions も取得する必要がある
	if r.stepExecutionRepo != nil {
		stepExecutions, err := r.stepExecutionRepo.FindStepExecutionsByJobExecutionID(ctx, jobExecution.ID)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
		}
		// 取得した StepExecution に JobExecution 参照を設定
		for _, se := range stepExecutions {
			se.JobExecution = jobExecution
		}
		jobExecution.StepExecutions = stepExecutions
	} else {
		logger.Warnf("SQLJobExecutionRepository に StepExecutionRepository が設定されていません。StepExecutions はロードされません。")
	}


	logger.Debugf("JobExecution (ID: %s, JobInstanceID: %s) をデータベースから取得しました。", executionID, jobExecution.JobInstanceID)

	return jobExecution, nil
}

// FindLatestJobExecution は指定された JobInstance の最新の JobExecution をデータベースから取得します。
func (r *SQLJobExecutionRepository) FindLatestJobExecution(ctx context.Context, jobInstanceID string) (*core.JobExecution, error) {
	query := `
    SELECT id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name
    FROM job_executions
    WHERE job_instance_id = $1
    ORDER BY create_time DESC
    LIMIT 1;
  `
	row := r.dbConnection.QueryRowContext(ctx, query, jobInstanceID)

	jobExecution := &core.JobExecution{}
	var endTime sql.NullTime
	var exitStatus sql.NullString
	var exitCode sql.NullInt64
	var paramsJSON, failuresJSON sql.NullString
	var contextJSON sql.NullString
	var currentStepName sql.NullString

	err := row.Scan(
		&jobExecution.ID,
		&jobExecution.JobInstanceID,
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
		&contextJSON,
		&currentStepName,
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
		jobExecution.CurrentStepName = ""
	}

	if paramsJSON.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSON.String), &jobExecution.Parameters)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", jobExecution.ID, err)
		}
	} else {
		jobExecution.Parameters = core.NewJobParameters()
	}

	if failuresJSON.Valid {
		jobExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String))
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の Failures のデコードに失敗しました: %v", jobExecution.ID, err)
		}
	} else {
		jobExecution.Failures = make([]error, 0)
	}

	if contextJSON.Valid {
		err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", jobExecution.ID, err)
		}
	} else {
		jobExecution.ExecutionContext = core.NewExecutionContext()
	}

	// StepExecutions も取得する必要がある
	if r.stepExecutionRepo != nil {
		stepExecutions, err := r.stepExecutionRepo.FindStepExecutionsByJobExecutionID(ctx, jobExecution.ID)
		if err != nil {
			logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
		}
		for _, se := range stepExecutions {
			se.JobExecution = jobExecution
		}
		jobExecution.StepExecutions = stepExecutions
	} else {
		logger.Warnf("SQLJobExecutionRepository に StepExecutionRepository が設定されていません。StepExecutions はロードされません。")
	}

	logger.Debugf("JobInstance (ID: %s) の最新 JobExecution (ID: %s) をデータベースから取得しました。", jobInstanceID, jobExecution.ID)

	return jobExecution, nil
}

// FindJobExecutionsByJobInstance は指定された JobInstance に関連する全ての JobExecution をデータベースから検索します。
func (r *SQLJobExecutionRepository) FindJobExecutionsByJobInstance(ctx context.Context, jobInstance *core.JobInstance) ([]*core.JobExecution, error) {
	query := `
    SELECT id, job_instance_id, job_name, start_time, end_time, status, exit_status, exit_code, create_time, last_updated, version, job_parameters, failure_exceptions, execution_context, current_step_name
    FROM job_executions
    WHERE job_instance_id = $1
    ORDER BY create_time ASC;
  `
	rows, err := r.dbConnection.QueryContext(ctx, query, jobInstance.ID)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) に関連する JobExecution の取得に失敗しました", jobInstance.ID), err, false, false)
	}
	defer rows.Close()

	var jobExecutions []*core.JobExecution

	for rows.Next() {
		jobExecution := &core.JobExecution{}
		var endTime sql.NullTime
		var exitStatus sql.NullString
		var exitCode sql.NullInt64
		var paramsJSON, failuresJSON sql.NullString
		var contextJSON sql.NullString
		var currentStepName sql.NullString

		err := rows.Scan(
			&jobExecution.ID,
			&jobExecution.JobInstanceID,
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
			&contextJSON,
			&currentStepName,
		)
		if err != nil {
			logger.Errorf("JobExecution のスキャン中にエラーが発生しました (JobInstance ID: %s): %v", jobInstance.ID, err)
			continue
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

		if paramsJSON.Valid {
			err = serialization.UnmarshalJobParameters([]byte(paramsJSON.String), &jobExecution.Parameters)
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) の JobParameters のデコードに失敗しました: %v", jobExecution.ID, err)
			}
		} else {
			jobExecution.Parameters = core.NewJobParameters()
		}

		if failuresJSON.Valid {
			jobExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String))
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) の Failures のデコードに失敗しました: %v", jobExecution.ID, err)
			}
		} else {
			jobExecution.Failures = make([]error, 0)
		}

		if contextJSON.Valid {
			err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &jobExecution.ExecutionContext)
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", jobExecution.ID, err)
			}
		} else {
			jobExecution.ExecutionContext = core.NewExecutionContext()
		}

		// 各 JobExecution に対して StepExecutions をロードし、JobExecution 参照を設定
		if r.stepExecutionRepo != nil {
			stepExecutions, err := r.stepExecutionRepo.FindStepExecutionsByJobExecutionID(ctx, jobExecution.ID)
			if err != nil {
				logger.Errorf("JobExecution (ID: %s) に関連する StepExecutions の取得に失敗しました: %v", jobExecution.ID, err)
			}
			for _, se := range stepExecutions {
				se.JobExecution = jobExecution
			}
			jobExecution.StepExecutions = stepExecutions
		} else {
			logger.Warnf("SQLJobExecutionRepository に StepExecutionRepository が設定されていません。StepExecutions はロードされません。")
		}

		jobExecutions = append(jobExecutions, jobExecution)
	}

	if err := rows.Err(); err != nil {
		return jobExecutions, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (ID: %s) に関連する JobExecution 取得後の行処理中にエラーが発生しました", jobInstance.ID), err, false, false)
	}

	logger.Debugf("JobInstance (ID: %s) に関連する %d 件の JobExecution を取得しました。", jobInstance.ID, len(jobExecutions))

	return jobExecutions, nil
}

// SQLJobExecutionRepository が JobExecution インターフェースを満たすことを確認
var _ JobExecution = (*SQLJobExecutionRepository)(nil)

