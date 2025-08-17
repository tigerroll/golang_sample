package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"sample/pkg/batch/database" // database パッケージをインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	"sample/pkg/batch/repository/job" // job インターフェースをインポート
	exception "sample/pkg/batch/util/exception" // exception パッケージをインポート
	logger "sample/pkg/batch/util/logger"
	serialization "sample/pkg/batch/util/serialization"
)

// SQLStepExecutionRepository は StepExecution インターフェースの SQL データベース実装です。
type SQLStepExecutionRepository struct {
	dbConnection database.DBConnection
	// JobExecutionRepository への参照を保持し、循環参照を解決するために job.JobExecution インターフェースとして持つ
	jobExecutionRepo job.JobExecution
}

// NewSQLStepExecutionRepository は新しい SQLStepExecutionRepository のインスタンスを作成します。
func NewSQLStepExecutionRepository(dbConn database.DBConnection) *SQLStepExecutionRepository {
	return &SQLStepExecutionRepository{ // SQLStepExecutionRepository を返す
		dbConnection: dbConn,
	}
}

// SetJobExecutionRepository は JobExecutionRepository の参照を設定します。
// 循環参照を解決するために、コンストラクタではなく別途設定する。
func (r *SQLStepExecutionRepository) SetJobExecutionRepository(repo job.JobExecution) {
	r.jobExecutionRepo = repo // job.JobExecution インターフェースとして設定
}

// SaveStepExecution は新しい StepExecution をデータベースに保存します。
func (r *SQLStepExecutionRepository) SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error {
	failuresJSONBytes, err := serialization.MarshalFailures(stepExecution.Failures)
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	contextJSONBytes, err := serialization.MarshalExecutionContext(stepExecution.ExecutionContext)
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	query := `
    INSERT INTO step_executions (id, job_execution_id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context, last_updated, version)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);
  `
	jobExecutionID := ""
	if stepExecution.JobExecution != nil {
		jobExecutionID = stepExecution.JobExecution.ID
	} else {
		return exception.NewBatchError("job_repository", "StepExecution が JobExecution に紐づいていません", nil, false, false)
	}

	_, err = r.dbConnection.ExecContext(
		ctx,
		query,
		stepExecution.ID,
		jobExecutionID,
		stepExecution.StepName,
		stepExecution.StartTime,
		sql.NullTime{Time: stepExecution.EndTime, Valid: !stepExecution.EndTime.IsZero()},
		string(stepExecution.Status),
		string(stepExecution.ExitStatus),
		stepExecution.ReadCount,
		stepExecution.WriteCount,
		stepExecution.CommitCount,
		stepExecution.RollbackCount,
		failuresJSON,
		contextJSON,
		stepExecution.LastUpdated,
		stepExecution.Version,
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の保存に失敗しました", stepExecution.ID), err, false, false)
	}

	logger.Debugf("StepExecution (ID: %s, JobExecutionID: %s) を保存しました。", stepExecution.ID, jobExecutionID)
	return nil
}

// UpdateStepExecution は既存の StepExecution の状態をデータベースで更新します。
func (r *SQLStepExecutionRepository) UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error {
	failuresJSONBytes, err := serialization.MarshalFailures(stepExecution.Failures)
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution Failures のエンコードに失敗しました", err, false, false)
	}
	failuresJSON := string(failuresJSONBytes)

	contextJSONBytes, err := serialization.MarshalExecutionContext(stepExecution.ExecutionContext)
	if err != nil {
		return exception.NewBatchError("job_repository", "StepExecution ExecutionContext のシリアライズに失敗しました", err, false, false)
	}
	contextJSON := string(contextJSONBytes)

	query := `
    UPDATE step_executions
    SET end_time = $1, status = $2, exit_status = $3, read_count = $4, write_count = $5, commit_count = $6, rollback_count = $7, failure_exceptions = $8, execution_context = $9, last_updated = $10
    WHERE id = $11;
  `
	res, err := r.dbConnection.ExecContext(
		ctx,
		query,
		sql.NullTime{Time: stepExecution.EndTime, Valid: !stepExecution.EndTime.IsZero()},
		string(stepExecution.Status),
		string(stepExecution.ExitStatus),
		stepExecution.ReadCount,
		stepExecution.WriteCount,
		stepExecution.CommitCount,
		stepExecution.RollbackCount,
		failuresJSON,
		contextJSON,
		time.Now(),
		stepExecution.ID,
	)
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の更新に失敗しました", stepExecution.ID), err, false, false)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の更新結果取得に失敗しました", stepExecution.ID), err, false, false)
	}
	if rowsAffected == 0 {
		return exception.NewBatchErrorf("job_repository", "StepExecution (ID: %s) の更新対象が見つかりませんでした (またはバージョン不一致)", stepExecution.ID)
	}

	logger.Debugf("StepExecution (ID: %s) を更新しました。", stepExecution.ID)
	return nil
}

// FindStepExecutionByID は指定された ID の StepExecution をデータベースから取得します。
func (r *SQLStepExecutionRepository) FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error) {
	query := `
    SELECT id, job_execution_id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context
    FROM step_executions
    WHERE id = $1;
  `
	row := r.dbConnection.QueryRowContext(ctx, query, executionID)

	stepExecution := &core.StepExecution{}
	var jobExecutionID string
	var endTime sql.NullTime
	var exitStatus sql.NullString
	var failuresJSON sql.NullString
	var contextJSON sql.NullString

	err := row.Scan(
		&stepExecution.ID,
		&jobExecutionID,
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
		&contextJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, exception.NewBatchErrorf("job_repository", "StepExecution (ID: %s) が見つかりませんでした", executionID)
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("StepExecution (ID: %s) の取得に失敗しました", executionID), err, false, false)
	}

	stepExecution.EndTime = endTime.Time
	if exitStatus.Valid {
		stepExecution.ExitStatus = core.ExitStatus(exitStatus.String)
	}

	if failuresJSON.Valid {
		stepExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String))
		if err != nil {
			logger.Errorf("StepExecution (ID: %s) の Failures のデコードに失敗しました: %v", executionID, err)
		}
	} else {
		stepExecution.Failures = make([]error, 0)
	}

	if contextJSON.Valid {
		err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &stepExecution.ExecutionContext)
		if err != nil {
			logger.Errorf("StepExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", executionID, err)
		}
	} else {
		stepExecution.ExecutionContext = core.NewExecutionContext()
	}

	// JobExecution オブジェクトを JobExecutionRepository から取得して StepExecution.JobExecution に設定
	if jobExecutionID != "" && r.jobExecutionRepo != nil {
		jobExecution, err := r.jobExecutionRepo.FindJobExecutionByID(ctx, jobExecutionID)
		if err != nil {
			logger.Errorf("StepExecution (ID: %s) の親 JobExecution (ID: %s) の取得に失敗しました: %v", executionID, jobExecutionID, err)
		} else {
			stepExecution.JobExecution = jobExecution
		}
	}

	logger.Debugf("StepExecution (ID: %s, JobExecutionID: %s) をデータベースから取得しました。", executionID, jobExecutionID)

	return stepExecution, nil
}

// FindStepExecutionsByJobExecutionID は指定された JobExecution ID に関連する全ての StepExecution をデータベースから取得します。
func (r *SQLStepExecutionRepository) FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error) {
	query := `
    SELECT id, step_name, start_time, end_time, status, exit_status, read_count, write_count, commit_count, rollback_count, failure_exceptions, execution_context
    FROM step_executions
    WHERE job_execution_id = $1
    ORDER BY start_time ASC;
  `
	rows, err := r.dbConnection.QueryContext(ctx, query, jobExecutionID)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecution の取得に失敗しました", jobExecutionID), err, false, false)
	}
	defer rows.Close()

	var stepExecutions []*core.StepExecution

	for rows.Next() {
		stepExecution := &core.StepExecution{}
		var endTime sql.NullTime
		var exitStatus sql.NullString
		var failuresJSON sql.NullString
		var contextJSON sql.NullString

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
			&contextJSON,
		)
		if err != nil {
			logger.Errorf("StepExecution のスキャン中にエラーが発生しました (JobExecution ID: %s): %v", jobExecutionID, err)
			continue
		}

		stepExecution.EndTime = endTime.Time
		if exitStatus.Valid {
			stepExecution.ExitStatus = core.ExitStatus(exitStatus.String)
		}

		if failuresJSON.Valid {
			stepExecution.Failures, err = serialization.UnmarshalFailures([]byte(failuresJSON.String))
			if err != nil {
				logger.Errorf("StepExecution (ID: %s) の Failures のデコードに失敗しました: %v", stepExecution.ID, err)
			}
		} else {
			stepExecution.Failures = make([]error, 0)
		}

		if contextJSON.Valid {
			err = serialization.UnmarshalExecutionContext([]byte(contextJSON.String), &stepExecution.ExecutionContext)
			if err != nil {
				logger.Errorf("StepExecution (ID: %s) の ExecutionContext のデシリアライズに失敗しました: %v", stepExecution.ID, err)
			}
		} else {
			stepExecution.ExecutionContext = core.NewExecutionContext()
		}

		// StepExecution に所属する JobExecution のポインタは、呼び出し元 (例: FindJobExecutionByID) で設定する
		// ここで設定すると循環参照になる可能性があるため、ここでは設定しない
		stepExecutions = append(stepExecutions, stepExecution)
	}

	if err := rows.Err(); err != nil {
		return stepExecutions, exception.NewBatchError("job_repository", fmt.Sprintf("JobExecution (ID: %s) に関連する StepExecution の取得後の行処理中にエラーが発生しました", jobExecutionID), err, false, false)
	}

	logger.Debugf("JobExecution (ID: %s) に関連する %d 件の StepExecution を取得しました。", jobExecutionID, len(stepExecutions))

	return stepExecutions, nil
}

// SQLStepExecutionRepository が StepExecution インターフェースを満たすことを確認
var _ job.StepExecution = (*SQLStepExecutionRepository)(nil) // job.StepExecution インターフェースを満たすことを確認
