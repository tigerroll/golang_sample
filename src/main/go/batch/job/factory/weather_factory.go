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
  // step パッケージのインターフェースをインポート
  stepReader    "sample/src/main/go/batch/step/reader"
  stepProcessor "sample/src/main/go/batch/step/processor"
  stepWriter    "sample/src/main/go/batch/step/writer"
  logger        "sample/src/main/go/batch/util/logger"
)

// CreateWeatherJob は WeatherJob オブジェクトとその依存関係を作成し、リスナーを登録します。
// Reader, Processor, Writer はインターフェース型として Job に渡されます。
func CreateWeatherJob(ctx context.Context, cfg *config.Config) (core.Job, error) {
  logger.Debugf("Creating WeatherJob components and dependencies in weather_factory")

  // リポジトリをこのファクトリ関数内で生成する
  repo, err := repository.NewWeatherRepository(ctx, *cfg)
  if err != nil {
    return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
  }

  // Reader, Processor, Writer の具体的な実装を生成し、インターフェース型として保持
  reader := stepReader.NewWeatherReader(cfg) // Concrete type *reader.WeatherReader
  processor := stepProcessor.NewWeatherProcessor() // Concrete type *processor.WeatherProcessor
  writer := stepWriter.NewWeatherWriter(repo) // Concrete type *writer.WeatherWriter

  // WeatherJob オブジェクトを生成時に依存関係 (インターフェース型) と config を渡す
  // NewWeatherJob はインターフェース型を受け取るように修正が必要
  weatherJob := job.NewWeatherJob(
    repo,
    reader, // <- インターフェース型として渡される (*reader.WeatherReader は stepReader.Reader を満たす)
    processor, // <- インターフェース型として渡される (*processor.WeatherProcessor は stepProcessor.Processor を満たす)
    writer, // <- インターフェース型として渡される (*writer.WeatherWriter は stepWriter.Writer を満たす)
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

  logger.Debugf("WeatherJob created and configured successfully in weather_factory")

  return weatherJob, nil
}

// JobFactory の CreateJob メソッド (src/main/go/batch/job/factory/jobfactory.go) は、
// CreateWeatherJob の戻り値 (*job.WeatherJob は core.Job を満たす) をそのまま core.Job として返せばOKです。
// CreateWeatherJob の戻り値の型宣言のみ core.Job に修正が必要でした (前回のContext対応で修正済み)。
