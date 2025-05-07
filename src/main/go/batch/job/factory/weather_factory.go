package factory

import (
  "context"
  "fmt"

  config        "sample/src/main/go/batch/config"
  job           "sample/src/main/go/batch/job"
  core          "sample/src/main/go/batch/job/core"
  jobListener   "sample/src/main/go/batch/job/listener"
  repository    "sample/src/main/go/batch/repository"
  stepListener  "sample/src/main/go/batch/step/listener"
  processor     "sample/src/main/go/batch/step/processor"
  reader        "sample/src/main/go/batch/step/reader"
  writer        "sample/src/main/go/batch/step/writer"
  logger        "sample/src/main/go/batch/util/logger"
)

// CreateWeatherJob は WeatherJob オブジェクトとその依存関係を作成し、リスナーを登録します。
// これは JobFactory.CreateJob メソッドから呼び出されます。
// core.Job を返すように変更
func CreateWeatherJob(ctx context.Context, cfg *config.Config) (core.Job, error) {
  logger.Debugf("Creating WeatherJob components and dependencies in weather_factory")

  // リポジトリをこのファクトリ関数内で生成する
  repo, err := repository.NewWeatherRepository(ctx, *cfg) // context も渡す
  if err != nil {
    // リポジトリの生成に失敗した場合はエラーを返す
    return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
  }
  // Note: リポジトリの Close は Job の Run メソッド内で defer されるため、ここでは不要

  // 他のコンポーネントを生成
  reader := reader.NewWeatherReader(cfg)
  processor := processor.NewWeatherProcessor()
  // writer の生成時に生成した repo を渡す
  writer := writer.NewWeatherWriter(repo)

  // WeatherJob オブジェクトを生成時に依存関係と config を渡す
  // job.NewWeatherJob を呼び出し
  weatherJob := job.NewWeatherJob(
    repo, // <- 生成した repo を渡す
    reader,
    processor,
    writer,
    cfg,
  )

  // ステップリスナーをここで登録
  logger.Debugf("Registering Step Listeners in weather_factory")
  retryListener := stepListener.NewRetryListener(cfg)
  weatherJob.RegisterStepListener("Reader", retryListener)
  weatherJob.RegisterStepListener("Processor", retryListener)
  weatherJob.RegisterStepListener("Writer", retryListener)

  loggingStepListener := stepListener.NewLoggingListener()
  weatherJob.RegisterStepListener("Reader", loggingStepListener)
  weatherJob.RegisterStepListener("Processor", loggingStepListener)
  weatherJob.RegisterStepListener("Writer", loggingStepListener)

  // JobExecutionListener をここで生成し、ジョブに登録
  logger.Debugf("Registering Job Execution Listeners in weather_factory")
  loggingJobListener := jobListener.NewLoggingJobListener()
  weatherJob.RegisterJobListener(loggingJobListener)
  // 必要に応じて他の JobExecutionListener もここで生成・登録

  logger.Debugf("WeatherJob created and configured successfully in weather_factory")

  // *job.WeatherJob は core.Job インターフェースを満たすため、そのまま core.Job として返せる
  return weatherJob, nil
}
