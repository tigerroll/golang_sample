package job

import (
  "context"
  "fmt"
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/domain/entity"
  "sample/src/main/go/batch/repository"
  "sample/src/main/go/batch/step/listener"
  "sample/src/main/go/batch/step/processor"
  "sample/src/main/go/batch/step/reader"
  "sample/src/main/go/batch/step/writer"
  "sample/src/main/go/batch/util/logger"
  "time"
)

type WeatherJob struct {
  repo      repository.WeatherRepository
  reader    *reader.WeatherReader
  processor *processor.WeatherProcessor
  writer    *writer.WeatherWriter
  config    *config.Config
  listeners map[string][]listener.StepExecutionListener
}

func NewWeatherJob(
  repo repository.WeatherRepository,
  reader *reader.WeatherReader,
  processor *processor.WeatherProcessor,
  writer *writer.WeatherWriter,
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo:      repo,
    reader:    reader,
    processor: processor,
    writer:    writer,
    config:    cfg,
    listeners: make(map[string][]listener.StepExecutionListener),
  }
}

func (j *WeatherJob) RegisterListener(stepName string, l listener.StepExecutionListener) {
  if _, ok := j.listeners[stepName]; !ok {
    j.listeners[stepName] = make([]listener.StepExecutionListener, 0)
  }
  j.listeners[stepName] = append(j.listeners[stepName], l)
}

func (j *WeatherJob) Run(ctx context.Context) error {
  retryConfig := j.config.Batch.Retry // リトライ設定を取得
  var err error

  logger.Infof("Weather Job を開始します。")

  defer func() {
    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
      }
    }
  }()

  for attempt := 0; attempt < retryConfig.MaxAttempts; attempt++ {
    var forecastData interface{}
    var processedData interface{}
    var startTime time.Time
    var elapsed time.Duration
    var readerErr bool

    // Reader
    j.notifyBeforeStep(ctx, "Reader", nil)
    startTime = time.Now()
    forecastData, err = j.reader.Read(ctx)
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Reader", forecastData, err, elapsed)
    if err != nil {
      logger.Errorf("データの読み込みに失敗しました (リトライ %d): %v", attempt+1, err)
      readerErr = true
    } else {
      logger.Debugf("リーダーからデータを読み込みました: %+v", forecastData)
    }

    if readerErr {
      if attempt < retryConfig.MaxAttempts-1 {
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
        continue // リトライ
      } else {
        return fmt.Errorf("データの読み込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, err)
      }
    }

    // Processor
    j.notifyBeforeStep(ctx, "Processor", forecastData)
    startTime = time.Now()
    forecast, ok := forecastData.(*entity.OpenMeteoForecast)
    if !ok {
      err = fmt.Errorf("forecastData の型が *entity.OpenMeteoForecast ではありません: %T", forecastData)
    } else {
      processedData, err = j.processor.Process(ctx, forecast)
    }
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Processor", processedData, err, elapsed)
    if err != nil {
      logger.Errorf("データの加工に失敗しました (リトライ %d): %v", attempt+1, err)
      if attempt < retryConfig.MaxAttempts-1 {
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
        continue // リトライ
      } else {
        return fmt.Errorf("データの加工に最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, err)
      }
    } else {
      logger.Debugf("プロセッサーでデータを加工しました: %+v", processedData)
    }

    // Writer
    j.notifyBeforeStep(ctx, "Writer", processedData)
    startTime = time.Now()
    err = j.writer.Write(ctx, processedData.([]*entity.WeatherDataToStore))
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Writer", processedData, err, elapsed)
    if err != nil {
      logger.Errorf("データの書き込みに失敗しました (リトライ %d): %v", attempt+1, err)
      if attempt < retryConfig.MaxAttempts-1 {
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
        continue // リトライ
      } else {
        return fmt.Errorf("データの書き込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, err)
      }
    } else {
      logger.Infof("ライターでデータを書き込みました。")
      break // Success!
    }
  }

  logger.Infof("Weather Job を完了しました。")
  return nil
}

func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepName string, data interface{}) {
  if listeners, ok := j.listeners[stepName]; ok {
    for _, l := range listeners {
      l.BeforeStep(ctx, stepName, data)
    }
  }
}

func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  if listeners, ok := j.listeners[stepName]; ok {
    for _, l := range listeners {
      if loggingListener, ok := l.(*listener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepName, data, err, duration)
      } else {
        l.AfterStep(ctx, stepName, data, err)
      }
    }
  }
}
