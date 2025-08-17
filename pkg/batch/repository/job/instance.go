package job

import (
	"context"

	core "sample/pkg/batch/job/core"
)

// JobInstance は JobInstance の永続化と取得に関する操作を定義します。
type JobInstance interface { // ★ 変更: インターフェース名を JobInstance に
	// SaveJobInstance は新しい JobInstance を永続化します。
	SaveJobInstance(ctx context.Context, jobInstance *core.JobInstance) error

	// FindJobInstanceByJobNameAndParameters は指定されたジョブ名とパラメータに一致する JobInstance を検索します。
	FindJobInstanceByJobNameAndParameters(ctx context.Context, jobName string, params core.JobParameters) (*core.JobInstance, error)

	// FindJobInstanceByID は指定された ID の JobInstance を検索します。
	FindJobInstanceByID(ctx context.Context, instanceID string) (*core.JobInstance, error)

	// GetJobInstanceCount は指定されたジョブ名の JobInstance の数を返します。
	GetJobInstanceCount(ctx context.Context, jobName string) (int, error)

	// GetJobNames はリポジトリに存在する全てのジョブ名を返します。
	GetJobNames(ctx context.Context) ([]string, error)
}
