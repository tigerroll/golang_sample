// src/main/go/batch/job/factory.go
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

  weatherJob := NewWeatherJob(
    f.repo,
    reader,
    processor,
    writer,
    f.config,
  )

  logger.Debugf("Registering Step Listeners") // ログメッセージも修正
  retryListener := listener.NewRetryListener(f.config)
  // RegisterListener を RegisterStepListener に修正
  weatherJob.RegisterStepListener("Reader", retryListener)
  weatherJob.RegisterStepListener("Processor", retryListener)
  weatherJob.RegisterStepListener("Writer", retryListener)

  loggingListener := listener.NewLoggingListener()
  // RegisterListener を RegisterStepListener に修正
  weatherJob.RegisterStepListener("Reader", loggingListener)
  weatherJob.RegisterStepListener("Processor", loggingListener)
  weatherJob.RegisterStepListener("Writer", loggingListener)

  logger.Debugf("WeatherJob created successfully")

  return weatherJob, nil
}
