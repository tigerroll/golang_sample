package sql

import (
	"sample/pkg/batch/database"
	"sample/pkg/batch/repository/job" // job インターフェースをインポート
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// SQLJobRepository は JobRepository インターフェースの SQL データベース実装です。
// 各リポジトリの具体的な実装を埋め込み、委譲します。
type SQLJobRepository struct { // SQLJobRepository を返す
	dbConnection database.DBConnection // DBConnection インターフェースを追加

	// より粒度の細かいインターフェースを埋め込む (JobInstance, JobExecution, StepExecution はこのパッケージ内で定義されたインターフェース名)
	// これらのフィールドは、対応する具体的なリポジトリ実装のポインタを保持します。
	*SQLJobInstanceRepository // SQLJobInstanceRepository を埋め込む
	*SQLJobExecutionRepository // SQLJobExecutionRepository を埋め込む
	*SQLStepExecutionRepository // SQLStepExecutionRepository を埋め込む
}

// NewSQLJobRepository は新しい SQLJobRepository のインスタンスを作成します。
// 既に確立されたデータベース接続の抽象化を受け取ります。
func NewSQLJobRepository(dbConn database.DBConnection) *SQLJobRepository {
	instanceRepo := NewSQLJobInstanceRepository(dbConn) // sql.NewSQLJobInstanceRepository を呼び出す
	stepRepo := NewSQLStepExecutionRepository(dbConn) // sql.NewSQLStepExecutionRepository を呼び出す
	executionRepo := NewSQLJobExecutionRepository(dbConn) // sql.NewSQLJobExecutionRepository を呼び出す

	// 循環参照の解決: 後から相互参照を設定
	stepRepo.SetJobExecutionRepository(executionRepo)
	executionRepo.SetStepExecutionRepository(stepRepo)

	return &SQLJobRepository{
		dbConnection:             dbConn,
		SQLJobInstanceRepository: instanceRepo,
		SQLJobExecutionRepository: executionRepo,
		SQLStepExecutionRepository: stepRepo,
	}
}

// GetDBConnection は JobRepository インターフェースの実装です。
func (r *SQLJobRepository) GetDBConnection() database.DBConnection {
	return r.dbConnection
}

// Close はデータベース接続を閉じます。
func (r *SQLJobRepository) Close() error {
	if r.dbConnection != nil {
		err := r.dbConnection.Close()
		if err != nil {
			return exception.NewBatchError("job_repository", "データベース接続を閉じるのに失敗しました", err, false, false)
		}
		logger.Debugf("Job Repository のデータベース接続を閉じました。")
	}
	return nil
}

// SQLJobRepository が JobRepository インターフェースを満たすことを確認
var _ job.JobRepository = (*SQLJobRepository)(nil) // job.JobRepository インターフェースを満たすことを確認

// 以下は、JobRepository インターフェースのメソッドが、埋め込まれた構造体のメソッドによって
// 自動的に満たされることを示すためのコメントです。
// 例えば、r.SaveJobInstance(ctx, ji) は r.SQLJobInstanceRepository.SaveJobInstance(ctx, ji) に委譲されます。
// このため、SQLJobRepository 自体にこれらのメソッドを再定義する必要はありません。

// SaveJobInstance は JobInstance インターフェースのメソッドを委譲します。
// func (r *SQLJobRepository) SaveJobInstance(ctx context.Context, jobInstance *core.JobInstance) error {
// 	return r.SQLJobInstanceRepository.SaveJobInstance(ctx, jobInstance)
// }

// FindJobInstanceByJobNameAndParameters は JobInstance インターフェースのメソッドを委譲します。
// func (r *SQLJobRepository) FindJobInstanceByJobNameAndParameters(ctx context.Context, jobName string, params core.JobParameters) (*core.JobInstance, error) {
// 	return r.SQLJobInstanceRepository.FindJobInstanceByJobNameAndParameters(ctx, jobName, params)
// }

// ... 他のメソッドも同様に委譲される ...
