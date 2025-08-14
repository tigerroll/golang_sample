package repository

import (
	"context"
	"database/sql"
	"fmt"

	"sample/pkg/batch/database"
	core "sample/pkg/batch/job/core"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
	serialization "sample/pkg/batch/util/serialization"
)

// SQLJobInstanceRepository は JobInstance インターフェースの SQL データベース実装です。
type SQLJobInstanceRepository struct {
	dbConnection database.DBConnection
}

// NewSQLJobInstanceRepository は新しい SQLJobInstanceRepository のインスタンスを作成します。
func NewSQLJobInstanceRepository(dbConn database.DBConnection) *SQLJobInstanceRepository {
	return &SQLJobInstanceRepository{
		dbConnection: dbConn,
	}
}

// SaveJobInstance は新しい JobInstance をデータベースに保存します。
func (r *SQLJobInstanceRepository) SaveJobInstance(ctx context.Context, jobInstance *core.JobInstance) error {
	paramsJSONBytes, err := serialization.MarshalJobParameters(jobInstance.Parameters)
	if err != nil {
		return exception.NewBatchError("job_repository", "JobInstance JobParameters のシリアライズに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	query := ` 
    INSERT INTO job_instances (id, job_name, job_parameters, create_time, version)
    VALUES ($1, $2, $3, $4, $5);
  `
	_, err = r.dbConnection.ExecContext(
		ctx,
		query,
		jobInstance.ID,
		jobInstance.JobName,
		paramsJSON,
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
func (r *SQLJobInstanceRepository) FindJobInstanceByJobNameAndParameters(ctx context.Context, jobName string, params core.JobParameters) (*core.JobInstance, error) {
	paramsJSONBytes, err := serialization.MarshalJobParameters(params)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", "検索用 JobParameters のシリアライズに失敗しました", err, false, false)
	}
	paramsJSON := string(paramsJSONBytes)

	query := `
    SELECT id, job_name, job_parameters, create_time, version
    FROM job_instances
    WHERE job_name = $1 AND job_parameters @> $2;
    `
	row := r.dbConnection.QueryRowContext(ctx, query, jobName, paramsJSON)

	jobInstance := &core.JobInstance{}
	var paramsJSONFromDB sql.NullString

	err = row.Scan(
		&jobInstance.ID,
		&jobInstance.JobName,
		&paramsJSONFromDB,
		&jobInstance.CreateTime,
		&jobInstance.Version,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, exception.NewBatchError("job_repository", fmt.Sprintf("JobInstance (JobName: %s) の検索に失敗しました", jobName), err, false, false)
	}

	if paramsJSONFromDB.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSONFromDB.String), &jobInstance.Parameters)
		if err != nil {
			logger.Errorf("JobInstance (ID: %s) の JobParameters のデコードに失敗しました: %v", jobInstance.ID, err)
		}
	} else {
		jobInstance.Parameters = core.NewJobParameters()
	}

	logger.Debugf("JobInstance (ID: %s, JobName: %s) をデータベースから取得しました。", jobInstance.ID, jobInstance.JobName)

	return jobInstance, nil
}

// FindJobInstanceByID は指定された ID の JobInstance をデータベースから取得します。
func (r *SQLJobInstanceRepository) FindJobInstanceByID(ctx context.Context, instanceID string) (*core.JobInstance, error) {
	query := `
    SELECT id, job_name, job_parameters, create_time, version
    FROM job_instances
    WHERE id = $1;
  `
	row := r.dbConnection.QueryRowContext(ctx, query, instanceID)

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

	if paramsJSONFromDB.Valid {
		err = serialization.UnmarshalJobParameters([]byte(paramsJSONFromDB.String), &jobInstance.Parameters)
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
func (r *SQLJobInstanceRepository) GetJobInstanceCount(ctx context.Context, jobName string) (int, error) {
	query := `
    SELECT COUNT(*) FROM job_instances WHERE job_name = $1;
  `
	var count int
	err := r.dbConnection.QueryRowContext(ctx, query, jobName).Scan(&count)
	if err != nil {
		return 0, exception.NewBatchError("job_repository", fmt.Sprintf("ジョブ '%s' の JobInstance 数取得に失敗しました", jobName), err, false, false)
	}
	return count, nil
}

// GetJobNames はリポジトリに存在する全てのジョブ名を返します。
func (r *SQLJobInstanceRepository) GetJobNames(ctx context.Context) ([]string, error) {
	query := `
    SELECT DISTINCT job_name FROM job_instances ORDER BY job_name;
  `
	rows, err := r.dbConnection.QueryContext(ctx, query)
	if err != nil {
		return nil, exception.NewBatchError("job_repository", "ジョブ名の取得に失敗しました", err, false, false)
	}
	defer rows.Close()

	var jobNames []string
	for rows.Next() {
		var jobName string
		if err := rows.Scan(&jobName); err != nil {
			logger.Errorf("ジョブ名のスキャン中にエラーが発生しました: %v", err)
			continue
		}
		jobNames = append(jobNames, jobName)
	}

	if err := rows.Err(); err != nil {
		return jobNames, exception.NewBatchError("job_repository", "ジョブ名取得後の行処理中にエラーが発生しました", err, false, false)
	}

	logger.Debugf("%d 件のジョブ名を取得しました。", len(jobNames))
	return jobNames, nil
}

// SQLJobInstanceRepository が JobInstance インターフェースを満たすことを確認
var _ JobInstance = (*SQLJobInstanceRepository)(nil)

