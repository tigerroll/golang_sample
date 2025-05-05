package job

import (
  "context"
  "fmt"
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/repository"
  "sample/src/main/go/batch/step/listener"
  "sample/src/main/go/batch/step/processor"
  "sample/src/main/go/batch/step/reader"
  "sample/src/main/go/batch/step/writer"
  "sample/src/main/go/batch/util/logger"
)

// Job インターフェース
type Job interface {
  Run(ctx context.Context) error
}

type JobFactory struct {
  config *config.Config
  repo   repository.WeatherRepository
}

func NewJobFactory(cfg *config.Config, repo repository.WeatherRepository) *JobFactory {
  return &JobFactory{
    config: cfg,
    repo:   repo,
  }
}

func (f *JobFactory) CreateJob(jobName string) (Job, error) {
  switch jobName {
  case "weather":
    return f.createWeatherJob()
  // 他の Job の case を追加
  default:
    return nil, fmt.Errorf("指定された Job '%s' は存在しません", jobName)
  }
}

func (f *JobFactory) createWeatherJob() (*WeatherJob, error) {
  logger.Debugf("Creating WeatherReader")
  reader := reader.NewWeatherReader(f.config)
  logger.Debugf("Creating WeatherProcessor")
  processor := processor.NewWeatherProcessor()
  logger.Debugf("Creating WeatherWriter")
  writer := writer.NewWeatherWriter(f.repo) // writer のみを受け取る
  // エラーが発生しない NewWeatherWriter の場合、err は常に nil になります。

  weatherJob := NewWeatherJob(
    f.repo,
    reader,
    processor,
    writer,
    f.config,
  )
  logger.Debugf("Registering LoggingListener")
  retryListener := listener.NewRetryListener(f.config) // RetryListener を生成
  weatherJob.RegisterListener("Reader", retryListener)   // Reader に登録
  weatherJob.RegisterListener("Processor", retryListener) // Processor に登録
  weatherJob.RegisterListener("Writer", retryListener)   // Writer に登録
  loggingListener := listener.NewLoggingListener()       // LoggingListener も登録

  weatherJob.RegisterListener("Reader", loggingListener)
  weatherJob.RegisterListener("Processor", loggingListener)
  weatherJob.RegisterListener("Writer", loggingListener)
  logger.Debugf("WeatherJob created successfully")

  return weatherJob, nil
}

// WeatherJob に Run メソッドを実装させるため、ここに空の Run メソッドを定義しておくか、
// WeatherJob の型を Job インターフェースとして扱う
