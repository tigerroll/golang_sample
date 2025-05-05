// src/main/go/batch/job/weather_job.go
package job

import (
  "context"
  "fmt"
  "sample/src/main/go/batch/config"
  "sample/src/main/go/batch/domain/entity"
  "sample/src/main/go/batch/step/listener"    // step/listener パッケージをインポート
  joblistener "sample/src/main/go/batch/job/listener" // job/listener パッケージをインポートし、別名をつける
  "sample/src/main/go/batch/step/processor"
  "sample/src/main/go/batch/step/reader"
  "sample/src/main/go/batch/step/writer"
  "sample/src/main/go/batch/repository"
  "sample/src/main/go/batch/util/logger"
  "time"
)

type WeatherJob struct {
  repo         repository.WeatherRepository
  reader       *reader.WeatherReader
  processor    *processor.WeatherProcessor
  writer       *writer.WeatherWriter
  config       *config.Config
  listeners    map[string][]listener.StepExecutionListener // ステップリスナー
  jobListeners []joblistener.JobExecutionListener      // ジョブリスナーを追加
}

func NewWeatherJob(
  repo repository.WeatherRepository,
  reader *reader.WeatherReader,
  processor *processor.WeatherProcessor,
  writer *writer.WeatherWriter,
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo:         repo,
    reader:       reader,
    processor:    processor,
    writer:       writer,
    config:       cfg,
    listeners:    make(map[string][]listener.StepExecutionListener),
    jobListeners: make([]joblistener.JobExecutionListener, 0), // 初期化
  }
}

// RegisterStepListener は特定のステップに StepExecutionListener を登録します。 (既存メソッド名を変更)
func (j *WeatherJob) RegisterStepListener(stepName string, l listener.StepExecutionListener) {
  if _, ok := j.listeners[stepName]; !ok {
    j.listeners[stepName] = make([]listener.StepExecutionListener, 0)
  }
  j.listeners[stepName] = append(j.listeners[stepName], l)
}

// RegisterJobListener は JobExecutionListener を登録します。(新規追加)
func (j *WeatherJob) RegisterJobListener(l joblistener.JobExecutionListener) {
  j.jobListeners = append(j.jobListeners, l)
}

// ジョブリスナーへの通知メソッド (新規追加)
func (j *WeatherJob) notifyBeforeJob(ctx context.Context) {
  for _, l := range j.jobListeners {
    l.BeforeJob(ctx, j.config.Batch.JobName)
  }
}

// ジョブリスナーへの通知メソッド (新規追加)
func (j *WeatherJob) notifyAfterJob(ctx context.Context, jobErr error) {
  for _, l := range j.jobListeners {
    l.AfterJob(ctx, j.config.Batch.JobName, jobErr)
  }
}

// Run メソッドにリスナー呼び出しを追加
func (j *WeatherJob) Run(ctx context.Context) error {
  retryConfig := j.config.Batch.Retry
  var runErr error // ジョブ全体の最終的なエラーを保持する変数

  // ジョブ開始前リスナーを呼び出し
  j.notifyBeforeJob(ctx)

  // ジョブ完了後に必ず AfterJob リスナーを呼び出すための defer
  defer func() {
    j.notifyAfterJob(ctx, runErr) // ここで runErr を渡す
    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
        // クローズのエラーも重要であれば、runErr に合成することも検討
        if runErr == nil {
          runErr = fmt.Errorf("リポジトリのクローズエラー: %w", err)
        } else {
          runErr = fmt.Errorf("ジョブ実行エラー: %w, リポジトリクローズエラー: %v", runErr, err)
        }
      }
    }
  }()

  logger.Infof("Weather Job を開始します。")

  for attempt := 0; attempt < retryConfig.MaxAttempts; attempt++ {
    var forecastData interface{}
    var processedData interface{}
    var startTime time.Time
    var elapsed time.Duration
    var stepErr error // 各ステップでのエラーを保持

    // Reader
    j.notifyBeforeStep(ctx, "Reader", nil)
    startTime = time.Now()
    forecastData, stepErr = j.reader.Read(ctx)
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Reader", forecastData, stepErr, elapsed)

    if stepErr != nil {
      logger.Errorf("データの読み込みに失敗しました (リトライ %d): %v", attempt+1, stepErr)
      if attempt < retryConfig.MaxAttempts-1 {
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
        continue // リトライ
      } else {
        runErr = fmt.Errorf("データの読み込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, stepErr)
        break // リトライ上限に達したらループを抜ける
      }
    } else {
      logger.Debugf("リーダーからデータを読み込みました: %+v", forecastData)
    }

    if runErr != nil { // Readerでエラーが発生し、リトライ上限に達した場合
      break // 後続のステップは実行しない
    }

    // Processor
    j.notifyBeforeStep(ctx, "Processor", forecastData)
    startTime = time.Now()
    forecast, ok := forecastData.(*entity.OpenMeteoForecast)
    if !ok {
      stepErr = fmt.Errorf("forecastData の型が *entity.OpenMeteoForecast ではありません: %T", forecastData)
    } else {
      processedData, stepErr = j.processor.Process(ctx, forecast)
    }
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Processor", processedData, stepErr, elapsed)

    if stepErr != nil {
      logger.Errorf("データの加工に失敗しました (リトライ %d): %v", attempt+1, stepErr)
      if attempt < retryConfig.MaxAttempts-1 {
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
        continue // リトライ
      } else {
        runErr = fmt.Errorf("データの加工に最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, stepErr)
        break // リトライ上限に達したらループを抜ける
      }
    } else {
      logger.Debugf("プロセッサーでデータを加工しました: %+v", processedData)
    }

    if runErr != nil { // Processorでエラーが発生し、リトライ上限に達した場合
      break // 後続のステップは実行しない
    }

    // Writer
    j.notifyBeforeStep(ctx, "Writer", processedData)
    startTime = time.Now()
    // processedData が []*entity.WeatherDataToStore であることを確認してから渡す
    if dataToStore, ok := processedData.([]*entity.WeatherDataToStore); ok {
      stepErr = j.writer.Write(ctx, dataToStore)
    } else {
      stepErr = fmt.Errorf("processedData の型が []*entity.WeatherDataToStore ではありません: %T", processedData)
    }
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Writer", processedData, stepErr, elapsed)

    if stepErr != nil {
      logger.Errorf("データの書き込みに失敗しました (リトライ %d): %v", attempt+1, stepErr)
      if attempt < retryConfig.MaxAttempts-1 {
        time.Sleep(time.Duration(retryConfig.InitialInterval) * time.Second)
        continue // リトライ
      } else {
        runErr = fmt.Errorf("データの書き込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, stepErr)
        break // リトライ上限に達したらループを抜ける
      }
    } else {
      logger.Infof("ライターでデータを書き込みました。")
      break // Success! ループを抜ける
    }
  }

  // defer で AfterJob が呼ばれるため、ここではログ出力のみ
  if runErr != nil {
    logger.Errorf("Weather Job がエラーで終了しました: %v", runErr)
  } else {
    logger.Infof("Weather Job を完了しました。")
  }

  return runErr // defer で設定された runErr が返される
}

// ステップリスナーへの通知メソッド (既存)
func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepName string, data interface{}) {
  if listeners, ok := j.listeners[stepName]; ok {
    for _, l := range listeners {
      l.BeforeStep(ctx, stepName, data)
    }
  }
}

// ステップリスナーへの通知メソッド (既存)
func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  if listeners, ok := j.listeners[stepName]; ok {
    for _, l := range listeners {
      // LoggingListener の AfterStepWithDuration を特別に呼び出す場合
      if loggingListener, ok := l.(*listener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepName, data, err, duration)
      } else {
        l.AfterStep(ctx, stepName, data, err)
      }
    }
  }
}
