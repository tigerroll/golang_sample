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
}

// NewJobFactory は JobFactory の新しいインスタンスを作成します。
// repo 引数を削除
func NewJobFactory(cfg *config.Config) *JobFactory {
  return &JobFactory{
    config: cfg,
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
  logger.Debugf("Creating WeatherJob components and dependencies")

  // リポジトリを JobFactory の内部で生成する
  repo, err := repository.NewWeatherRepository(context.Background(), *f.config)
  if err != nil {
    // リポジトリの生成に失敗した場合はエラーを返す
    return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
  }
  // Note: リポジトリの Close は Job の Run メソッド内で defer されるため、ここでは不要

  reader := reader.NewWeatherReader(f.config)
  processor := processor.NewWeatherProcessor()
  // writer の生成時に生成した repo を渡す
  writer := writer.NewWeatherWriter(repo)

  // Job の生成時に生成した repo を渡す
  weatherJob := NewWeatherJob(
    repo,
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

  logger.Debugf("WeatherJob created and configured successfully")

  return weatherJob, nil
}
