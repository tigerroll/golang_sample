package job

import (
  "context"
  "fmt"
  "time"

  config        "sample/src/main/go/batch/config"
  entity        "sample/src/main/go/batch/domain/entity"
  core          "sample/src/main/go/batch/job/core"
  jobListener   "sample/src/main/go/batch/job/listener"
  repository    "sample/src/main/go/batch/repository"
  stepListener  "sample/src/main/go/batch/step/listener"
  processor     "sample/src/main/go/batch/step/processor"
  reader        "sample/src/main/go/batch/step/reader"
  writer        "sample/src/main/go/batch/step/writer"
  logger        "sample/src/main/go/batch/util/logger"
)

type WeatherJob struct {
  repo             repository.WeatherRepository
  reader           *reader.WeatherReader
  processor        *processor.WeatherProcessor
  writer           *writer.WeatherWriter
  config           *config.Config
  stepListeners    map[string][]stepListener.StepExecutionListener
  jobListeners     []jobListener.JobExecutionListener
}

// WeatherJob が core.Job インターフェースを満たすことを宣言 (明示的な実装宣言は不要だが意図を示す)
var _ core.Job = (*WeatherJob)(nil)

func NewWeatherJob(
  repo repository.WeatherRepository,
  reader *reader.WeatherReader,
  processor *processor.WeatherProcessor,
  writer *writer.WeatherWriter,
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo:             repo,
    reader:           reader,
    processor:        processor,
    writer:           writer,
    config:           cfg,
    stepListeners:    make(map[string][]stepListener.StepExecutionListener),
    jobListeners:     make([]jobListener.JobExecutionListener, 0),
  }
}

// RegisterStepListener は特定のステップに StepExecutionListener を登録します。
func (j *WeatherJob) RegisterStepListener(stepName string, l stepListener.StepExecutionListener) {
  if _, ok := j.stepListeners[stepName]; !ok {
    j.stepListeners[stepName] = make([]stepListener.StepExecutionListener, 0)
  }
  j.stepListeners[stepName] = append(j.stepListeners[stepName], l)
}

// RegisterJobListener は JobExecutionListener を登録します。
func (j *WeatherJob) RegisterJobListener(l jobListener.JobExecutionListener) {
  j.jobListeners = append(j.jobListeners, l)
}

// ジョブリスナーへの通知メソッド
func (j *WeatherJob) notifyBeforeJob(ctx context.Context) {
  for _, l := range j.jobListeners {
    l.BeforeJob(ctx, j.config.Batch.JobName)
  }
}

// ジョブリスナーへの通知メソッド
func (j *WeatherJob) notifyAfterJob(ctx context.Context, jobErr error) {
  for _, l := range j.jobListeners {
    l.AfterJob(ctx, j.config.Batch.JobName, jobErr)
  }
}

// Run メソッド (core.Job インターフェースの実装)
func (j *WeatherJob) Run(ctx context.Context) error {
  retryConfig := j.config.Batch.Retry
  var runErr error // ジョブ全体の最終的なエラーを保持する変数

  // ジョブ開始前リスナーを呼び出し
  j.notifyBeforeJob(ctx)

  // ジョブ完了後に必ず JobExecutionListener の AfterJob を呼び出し、リポジトリをクローズするための defer
  defer func() {
    // JobExecutionListener の AfterJob を呼び出し
    j.notifyAfterJob(ctx, runErr) // ここで runErr を渡す

    // リポジトリのクローズ処理
    // JobFactory で生成された repo は Job 構造体に保持されており、ここでクローズされる
    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
        // クローズのエラーも重要であれば、runErr に合成することも検討
        if runErr == nil {
          runErr = fmt.Errorf("リポジトリのクローズエラー: %w", err)
        } else {
          // 既存のエラーがある場合は、新しいエラー情報を追加する形にするか、合成する
          // 例: logger.Errorf("既存のエラー: %v, リポジトリクローズエラー: %v", runErr, err)
          // あるいは、複数のエラーを保持できるカスタムエラー型を使用する
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

// ステップリスナーへの通知メソッド
func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepName string, data interface{}) {
  if stepListeners, ok := j.stepListeners[stepName]; ok {
    for _, l := range stepListeners {
      l.BeforeStep(ctx, stepName, data)
    }
  }
}

// ステップリスナーへの通知メソッド
func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  if stepListeners, ok := j.stepListeners[stepName]; ok {
    for _, l := range stepListeners {
      // LoggingListener の AfterStepWithDuration を特別に呼び出す場合
      if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepName, data, err, duration)
      } else {
        l.AfterStep(ctx, stepName, data, err)
      }
    }
  }
}

// JobName メソッドを追加 (SimpleJobLauncher でジョブ名を取得するために使用)
// core.Job インターフェースに JobName() string を追加した場合に実装
func (j *WeatherJob) JobName() string {
  return j.config.Batch.JobName
}
