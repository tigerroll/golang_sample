package job

import (
  "context"
  "fmt"
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/repository"
  "sample/src/main/go/batch/step/reader"
  "sample/src/main/go/batch/step/processor"
  "sample/src/main/go/batch/step/writer"
  "sample/src/main/go/batch/util/logger"
)

type WeatherJob struct {
  repo      repository.WeatherRepository
  reader    *reader.WeatherReader
  processor *processor.WeatherProcessor
  writer    *writer.WeatherWriter
  config    *config.Config
}

func NewWeatherJob(ctx context.Context, cfg *config.Config) (*WeatherJob, error) {
  repo, err := repository.NewWeatherRepository(ctx, *cfg)
  if err != nil {
    return nil, fmt.Errorf("リポジトリの初期化に失敗しました: %w", err)
  }
  reader := reader.NewWeatherReader(cfg)
  processor := processor.NewWeatherProcessor()
  writer := writer.NewWeatherWriter(repo)

  return &WeatherJob{
    repo:      repo,
    reader:    reader,
    processor: processor,
    writer:    writer,
    config:    cfg,
  }, nil
}

func (j *WeatherJob) Run(ctx context.Context) error {
  defer func() {
    if err := j.repo.Close(); err != nil {
      logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
    }
  }()

  logger.Infof("Weather Job を開始します。")

  forecastData, err := j.reader.Read(ctx)
  if err != nil {
    return fmt.Errorf("データの読み込みに失敗しました: %w", err)
  }
  logger.Debugf("リーダーからデータを読み込みました: %+v", forecastData)

  processedData, err := j.processor.Process(ctx, forecastData)
  if err != nil {
    return fmt.Errorf("データの加工に失敗しました: %w", err)
  }
  logger.Debugf("プロセッサーでデータを加工しました: %+v", processedData)

  if err := j.writer.Write(ctx, processedData); err != nil {
    return fmt.Errorf("データの書き込みに失敗しました: %w", err)
  }
  logger.Infof("ライターでデータを書き込みました。")

  logger.Infof("Weather Job を完了しました。")
  return nil
}
