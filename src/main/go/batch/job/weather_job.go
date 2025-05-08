package job

import (
  "context"
  "fmt"
  "time"

  config         "sample/src/main/go/batch/config"
  core           "sample/src/main/go/batch/job/core"
  jobListener    "sample/src/main/go/batch/job/listener"
  repository     "sample/src/main/go/batch/repository"
  // step パッケージのインターフェースをインポート
  stepReader     "sample/src/main/go/batch/step/reader"
  stepProcessor  "sample/src/main/go/batch/step/processor"
  stepWriter     "sample/src/main/go/batch/step/writer"
  stepListener   "sample/src/main/go/batch/step/listener" // stepListener パッケージをインポート
  logger         "sample/src/main/go/batch/util/logger"
)

type WeatherJob struct {
  repo             repository.WeatherRepository
  // 具体的な構造体ではなく、インターフェース型を使用
  reader           stepReader.Reader
  processor        stepProcessor.Processor
  writer           stepWriter.Writer
  config           *config.Config
  // stepExecutionListener を stepListener に修正
  stepListeners    map[string][]stepListener.StepExecutionListener
  jobListeners     []jobListener.JobExecutionListener
}

// WeatherJob が core.Job インターフェースを満たすことを宣言 (明示的な実装宣言は不要だが意図を示す)
var _ core.Job = (*WeatherJob)(nil)

// NewWeatherJob 関数がインターフェース型を引数として受け取るように修正
func NewWeatherJob(
  repo repository.WeatherRepository,
  reader stepReader.Reader, // インターフェース型
  processor stepProcessor.Processor, // インターフェース型
  writer stepWriter.Writer, // インターフェース型
  cfg *config.Config,
) *WeatherJob {
  return &WeatherJob{
    repo:             repo,
    reader:           reader,
    processor:        processor,
    writer:           writer,
    config:           cfg,
    // stepExecutionListener を stepListener に修正
    stepListeners:    make(map[string][]stepListener.StepExecutionListener),
    jobListeners:     make([]jobListener.JobExecutionListener, 0),
  }
}

// RegisterStepListener は特定のステップに StepExecutionListener を登録します。
// stepExecutionListener を stepListener に修正
func (j *WeatherJob) RegisterStepListener(stepName string, l stepListener.StepExecutionListener) {
  if _, ok := j.stepListeners[stepName]; !ok {
    // stepExecutionListener を stepListener に修正
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
    if closer, ok := j.repo.(interface{ Close() error }); ok {
      if err := closer.Close(); err != nil {
        logger.Errorf("リポジトリのクローズに失敗しました: %v", err)
        if runErr == nil {
          runErr = fmt.Errorf("リポジトリのクローズエラー: %w", err)
        } else {
          // 既存のエラーがある場合は、新しいエラー情報を追加する形にするか、合成する
        }
      }
    }
  }()

  logger.Infof("Weather Job を開始します。")

  for attempt := 0; attempt < retryConfig.MaxAttempts; attempt++ {
    // ループ全体でも Context の完了をチェック
    select {
    case <-ctx.Done():
      runErr = ctx.Err()
      logger.Warnf("Context がキャンセルされたため、ジョブの実行を中断します: %v", runErr)
      goto endJob // ジョブ全体を中断するためラベル付き break の代わりに goto を使用
    default:
    }

    var readItem interface{} // Reader から読み込んだアイテム (interface{})
    var processedItem interface{} // Processor で処理したアイテム (interface{})
    var startTime time.Time
    var elapsed time.Duration
    var stepErr error // 各ステップでのエラーを保持

    // Reader
    j.notifyBeforeStep(ctx, "Reader", nil)
    startTime = time.Now()
    // Reader の Read メソッドを呼び出し (インターフェース経由)
    readItem, stepErr = j.reader.Read(ctx)
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Reader", readItem, stepErr, elapsed)

    if stepErr != nil {
      logger.Errorf("データの読み込みに失敗しました (リトライ %d): %v", attempt+1, stepErr)
      if ctx.Err() != nil {
        runErr = ctx.Err()
        goto endJob // Context キャンセルで中断
      }
      if attempt < retryConfig.MaxAttempts-1 {
        select {
        case <-time.After(time.Duration(retryConfig.InitialInterval) * time.Second):
          // スリープ完了
        case <-ctx.Done():
          runErr = ctx.Err()
          goto endJob // ジョブ全体を中断
        }
        continue // リトライ
      } else {
        runErr = fmt.Errorf("データの読み込みに最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, stepErr)
        break // リトライ上限に達したらループを抜ける
      }
    } else {
      logger.Debugf("リーダーからデータを読み込みました: %+v", readItem)
    }

    if runErr != nil {
      break // 後続のステップは実行しない
    }

    // Processor
    j.notifyBeforeStep(ctx, "Processor", readItem)
    startTime = time.Now()
    // Processor の Process メソッドを呼び出し (インターフェース経由)
    // 読み込んだアイテム (readItem) を Processor に渡す
    processedItem, stepErr = j.processor.Process(ctx, readItem)
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Processor", processedItem, stepErr, elapsed)

    if stepErr != nil {
      logger.Errorf("データの加工に失敗しました (リトライ %d): %v", attempt+1, stepErr)
      if ctx.Err() != nil {
        runErr = ctx.Err()
        goto endJob // Context キャンセルで中断
      }
      if attempt < retryConfig.MaxAttempts-1 {
        select {
        case <-time.After(time.Duration(retryConfig.InitialInterval) * time.Second):
          // スリープ完了
        case <-ctx.Done():
          runErr = ctx.Err()
          goto endJob // ジョブ全体を中断
        }
        continue // リトライ
      } else {
        runErr = fmt.Errorf("データの加工に最大リトライ回数 (%d) 失敗しました: %w", retryConfig.MaxAttempts, stepErr)
        break // リトライ上限に達したらループを抜ける
      }
    } else {
      logger.Debugf("プロセッサーでデータを加工しました: %+v", processedItem)
    }

    if runErr != nil {
      break // 後続のステップは実行しない
    }

    // Writer
    j.notifyBeforeStep(ctx, "Writer", processedItem)
    startTime = time.Now()
    // Writer の Write メソッドを呼び出し (インターフェース経由)
    // 処理済みアイテム (processedItem) を Writer に渡す
    stepErr = j.writer.Write(ctx, processedItem)
    elapsed = time.Since(startTime)
    j.notifyAfterStep(ctx, "Writer", processedItem, stepErr, elapsed)

    if stepErr != nil {
      logger.Errorf("データの書き込みに失敗しました (リトライ %d): %v", attempt+1, stepErr)
      if ctx.Err() != nil {
        runErr = ctx.Err()
        goto endJob // Context キャンセルで中断
      }
      if attempt < retryConfig.MaxAttempts-1 {
        select {
        case <-time.After(time.Duration(retryConfig.InitialInterval) * time.Second):
          // スリープ完了
        case <-ctx.Done():
          runErr = ctx.Err()
          goto endJob // ジョブ全体を中断
        }
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

endJob: // Context キャンセル時などにジャンプするラベル

  // defer で AfterJob が呼ばれるため、ここではログ出力のみ
  if runErr != nil {
    logger.Errorf("Weather Job がエラーで終了しました: %v", runErr)
  } else {
    logger.Infof("Weather Job を完了しました。")
  }

  return runErr // defer で設定された runErr が返される
}

// ステップリスナーへの通知メソッド
// BeforeStep に ctx context.Context を追加
func (j *WeatherJob) notifyBeforeStep(ctx context.Context, stepName string, data interface{}) {
  if stepListeners, ok := j.stepListeners[stepName]; ok {
    for _, l := range stepListeners {
      l.BeforeStep(ctx, stepName, data)
    }
  }
}

// ステップリスナーへの通知メソッド
// AfterStepWithDuration に ctx context.Context を追加
// StepExecutionListener インターフェースの AfterStep メソッドにも Context を渡すように修正
func (j *WeatherJob) notifyAfterStep(ctx context.Context, stepName string, data interface{}, err error, duration time.Duration) {
  if stepListeners, ok := j.stepListeners[stepName]; ok {
    for _, l := range stepListeners {
      // LoggingListener の AfterStepWithDuration を特別に呼び出す場合
      // stepExecutionListener を stepListener に修正
      if loggingListener, ok := l.(*stepListener.LoggingListener); ok {
        loggingListener.AfterStepWithDuration(ctx, stepName, data, err, duration)
      } else {
        // StepExecutionListener インターフェースの AfterStep メソッドに Context を渡して呼び出し
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
