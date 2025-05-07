// src/main/go/batch/job/factory.go
package job

import (
  "context"
  "fmt"

  config        "sample/src/main/go/batch/config"
  repository    "sample/src/main/go/batch/repository"
  jobListener   "sample/src/main/go/batch/job/listener"
  stepListener  "sample/src/main/go/batch/step/listener"
  processor     "sample/src/main/go/batch/step/processor"
  reader        "sample/src/main/go/batch/step/reader"
  writer        "sample/src/main/go/batch/step/writer"
  logger        "sample/src/main/go/batch/util/logger"
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
  logger.Debugf("Creating WeatherJob components")

  reader := reader.NewWeatherReader(f.config)
  processor := processor.NewWeatherProcessor()
  writer := writer.NewWeatherWriter(f.repo)

  weatherJob := NewWeatherJob(
    f.repo,
    reader,
    processor,
    writer,
    f.config,
  )

  // ステップリスナーを JobFactory で登録
  logger.Debugf("Registering Step Listeners in JobFactory")
  retryListener := stepListener.NewRetryListener(f.config)
  weatherJob.RegisterStepListener("Reader", retryListener)
  weatherJob.RegisterStepListener("Processor", retryListener)
  weatherJob.RegisterStepListener("Writer", retryListener)

  loggingStepListener := stepListener.NewLoggingListener()
  weatherJob.RegisterStepListener("Reader", loggingStepListener)
  weatherJob.RegisterStepListener("Processor", loggingStepListener)
  weatherJob.RegisterStepListener("Writer", loggingStepListener)

  // JobExecutionListener を JobFactory で生成し、ジョブに登録 (ここに移動)
  logger.Debugf("Registering Job Execution Listeners in JobFactory")
  loggingJobListener := jobListener.NewLoggingJobListener()
  weatherJob.RegisterJobListener(loggingJobListener)
  // 必要に応じて他の JobExecutionListener もここで生成・登録
  // metricsJobListener := jobListener.NewMetricsJobListener(...)
  // weatherJob.RegisterJobListener(metricsJobListener)


  logger.Debugf("WeatherJob created and configured successfully")

  return weatherJob, nil
}
