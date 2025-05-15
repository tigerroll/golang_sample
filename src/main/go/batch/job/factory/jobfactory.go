package factory

import (
  "context"
  "fmt"

  config  "sample/src/main/go/batch/config"
  core    "sample/src/main/go/batch/job/core"
  repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
  //logger  "sample/src/main/go/batch/util/logger"
)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
  config *config.Config
  jobRepository repository.JobRepository // JobRepository を依存として追加
}

// NewJobFactory は JobFactory の新しいインスタンスを作成します。
// JobRepository を引数に追加
func NewJobFactory(cfg *config.Config, repo repository.JobRepository) *JobFactory {
  return &JobFactory{
    config: cfg,
    jobRepository: repo, // JobRepository を初期化
  }
}

// CreateJob は指定されたジョブ名の Job オブジェクトを作成します。
// core.Job を返すように変更
func (f *JobFactory) CreateJob(jobName string) (core.Job, error) {
  switch jobName {
  case "weather":
    // CreateWeatherJob 関数を呼び出す (同じ factory パッケージ内)
    // JobRepository を渡すように変更
    return CreateWeatherJob(context.Background(), f.config, f.jobRepository)

  // 他の Job の case を追加
  default:
    return nil, fmt.Errorf("指定された Job '%s' は存在しません", jobName)
  }
}

// Note: createWeatherJob メソッドの実装は weather_factory.go にあります。