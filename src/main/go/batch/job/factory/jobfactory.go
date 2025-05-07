package factory

import (
  "context"
  "fmt"

  config  "sample/src/main/go/batch/config"
  core    "sample/src/main/go/batch/job/core"
  //logger  "sample/src/main/go/batch/util/logger"
)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
  config *config.Config
}

// NewJobFactory は JobFactory の新しいインスタンスを作成します。
func NewJobFactory(cfg *config.Config) *JobFactory {
  return &JobFactory{
    config: cfg,
  }
}

// CreateJob は指定されたジョブ名の Job オブジェクトを作成します。
// core.Job を返すように変更
func (f *JobFactory) CreateJob(jobName string) (core.Job, error) {
  switch jobName {
  case "weather":
    // CreateWeatherJob 関数を呼び出す (同じ factory パッケージ内)
    // CreateWeatherJob も core.Job を返すように修正される前提
    return CreateWeatherJob(context.Background(), f.config)

  // 他の Job の case を追加
  default:
    return nil, fmt.Errorf("指定された Job '%s' は存在しません", jobName)
  }
}

// Note: createWeatherJob メソッドの実装は weather_factory.go にあります。
