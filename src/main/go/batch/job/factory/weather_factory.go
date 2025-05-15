package factory

import (
  "context"
  "fmt"

  config        "sample/src/main/go/batch/config" // config パッケージをインポート
  job           "sample/src/main/go/batch/job"
  core          "sample/src/main/go/batch/job/core"
  jobListener   "sample/src/main/go/batch/job/listener"
  repository    "sample/src/main/go/batch/repository"
  stepListener  "sample/src/main/go/batch/step/listener"
  // step パッケージのインターフェースをインポート
  step          "sample/src/main/go/batch/step" // step パッケージをインポート
  stepProcessor "sample/src/main/go/batch/step/processor"
  stepReader    "sample/src/main/go/batch/step/reader"
  stepWriter    "sample/src/main/go/batch/step/writer"
  logger        "sample/src/main/go/batch/util/logger"
)

// CreateWeatherJob は WeatherJob オブジェクトとその依存関係を作成し、リスナーを登録します。
// これは JobFactory.CreateJob メソッドから呼び出されます。
// JobRepository を引数に追加
func CreateWeatherJob(ctx context.Context, cfg *config.Config, jobRepository repository.JobRepository) (core.Job, error) {
  logger.Debugf("Creating WeatherJob components and dependencies in weather_factory")

  // リポジトリをこのファクトリ関数内で生成する
  // Repository は DatabaseConfig 全体、または DatabaseConfig から必要な部分を渡すように設計されている可能性が高い
  // 現在の Repository は config.Config 全体を受け取っていたが、DatabaseConfig に絞るべき
  // NewWeatherRepository は context と DatabaseConfig を受け取るように修正が必要（repository/factory.go の NewWeatherRepository も修正）
  // repo, err := repository.NewWeatherRepository(ctx, *cfg) // cfg 全体を渡している箇所
  // ここでは repository/factory.go の NewWeatherRepository が config.Config 全体を受け取るままと仮定し、
  // Repository 自体のコンストラクタが DatabaseConfig を受け取るように修正されている前提とする。
  // 理想的には、repository.NewWeatherRepository(ctx context.Context, dbConfig *config.DatabaseConfig) のようなシグネチャが望ましい。
  // 現在のコードでは repository.NewWeatherRepository が config.Config 全体を受け取るので、それに合わせるが、依存は増える
  repo, err := repository.NewWeatherRepository(ctx, *cfg) // ← この呼び出しはそのまま
  if err != nil {
    return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
  }
  // Note: リポジトリのリソース解放 (Close メソッドを持つ場合) は Job の Run メソッド内で defer されるため、ここでは不要

  // Reader に渡す設定構造体を生成
  weatherReaderCfg := &config.WeatherReaderConfig{
    APIEndpoint: cfg.Batch.APIEndpoint,
    APIKey:      cfg.Batch.APIKey,
  }
  // Reader の生成時に小さい設定構造体を渡す
  reader := stepReader.NewWeatherReader(weatherReaderCfg)

  // Processor は現時点で設定が不要
  processor := stepProcessor.NewWeatherProcessor()

  // Writer は Repository に依存しており、Writer 自身に渡す設定は現時点では不要
  writer := stepWriter.NewWeatherWriter(repo)

  // RetryListener に渡す設定構造体は RetryConfig そのものを使用
  retryCfg := &cfg.Batch.Retry
  // RetryListener の生成時に RetryConfig を渡す
  retryListener := stepListener.NewRetryListener(retryCfg)

  // LoggingListener に渡す設定構造体は LoggingConfig を使用
  loggingCfg := &cfg.System.Logging
  // LoggingListener の生成時に LoggingConfig を渡す
  loggingStepListener := stepListener.NewLoggingListener(loggingCfg)
  // JobLoggingListener の生成時にも LoggingConfig を渡す
  loggingJobListener := jobListener.NewLoggingJobListener(loggingCfg)

  // --- ステップの生成 ---
  // ChunkOrientedStep を生成
  weatherProcessingStep := step.NewChunkOrientedStep(
    "WeatherProcessingStep", // ステップ名
    reader,
    processor,
    writer,
    cfg.Batch.ChunkSize, // チャンクサイズを設定から取得
    retryCfg,            // リトライ設定を渡す
  )

  // ステップリスナーを ChunkOrientedStep に登録
  logger.Debugf("Registering Step Listeners to WeatherProcessingStep")
  weatherProcessingStep.RegisterListener(retryListener)
  weatherProcessingStep.RegisterListener(loggingStepListener)

  // --- ジョブの生成 ---
  // WeatherJob オブジェクトを生成時に JobRepository とステップリストを渡す
  // NewWeatherJob は JobRepository と []core.Step を受け取るように修正済み
  weatherJob := job.NewWeatherJob(
    jobRepository, // JobRepository を渡す
    []core.Step{weatherProcessingStep}, // ステップリストを渡す
    cfg, // Job 自身は Config 全体が必要なためそのまま渡す
  )

  // JobExecutionListener をここで生成し、ジョブに登録
  logger.Debugf("Registering Job Execution Listeners to WeatherJob")
  weatherJob.RegisterJobListener(loggingJobListener)

  logger.Debugf("WeatherJob created and configured successfully in weather_factory")

  return weatherJob, nil
}