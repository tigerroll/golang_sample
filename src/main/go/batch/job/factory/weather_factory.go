package factory

import (
  "context"
  "fmt"
  // "io" // DummyReader が移動したので io パッケージのインポートは不要になる可能性がある

  config "sample/src/main/go/batch/config" // config パッケージをインポート
  job "sample/src/main/go/batch/job"
  core "sample/src/main/go/batch/job/core"
  jobListener "sample/src/main/go/batch/job/listener"
  repository "sample/src/main/go/batch/repository"
  stepListener "sample/src/main/go/batch/step/listener"
  // step パッケージのインターフェースをインポート
  step "sample/src/main/go/batch/step" // step パッケージをインポート
  stepProcessor "sample/src/main/go/batch/step/processor" // stepProcessor パッケージをインポート
  stepReader "sample/src/main/go/batch/step/reader" // stepReader パッケージをインポート
  stepWriter "sample/src/main/go/batch/step/writer" // stepWriter パッケージをインポート
  logger "sample/src/main/go/batch/util/logger"

  // ★ ダミーコンポーネントをそれぞれのパッケージからインポート ★
  dummyReader "sample/src/main/go/batch/step/reader" // dummy_reader.go がこのパッケージに属する
  dummyProcessor "sample/src/main/go/batch/step/processor" // dummy_processor.go がこのパッケージに属する
  dummyWriter "sample/src/main/go/batch/step/writer" // dummy_writer.go がこのパッケージに属する
)

// --- ダミーコンポーネントの定義 (それぞれのパッケージファイルに移動) ---
// DummyReader, DummyProcessor, DummyWriter の定義はここから削除されます。


// --- JobFactory の実装 (jobfactory.go に移動) ---
// JobFactory, NewJobFactory, JobFactory.CreateJob は jobfactory.go で定義されているため、ここから削除します。


// CreateWeatherJob は WeatherJob オブジェクトとその依存関係を作成し、リスナーを登録します。
// これは JobFactory.CreateJob メソッドから呼び出されます。
// JobRepository を引数に追加
// ★ FlowDefinition を作成し、WeatherJob に渡すように変更
// ★ 複数ステップを定義するように変更
func CreateWeatherJob(ctx context.Context, cfg *config.Config, jobRepository repository.JobRepository) (core.Job, error) {
  logger.Debugf("Creating WeatherJob components and dependencies in weather_factory")

  // リポジトリをこのファクトリ関数内で生成する
  repo, err := repository.NewWeatherRepository(ctx, *cfg)
  if err != nil {
    return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
  }
  // Note: リポジトリのリソース解放 (Close メソッドを持つ場合) は Job の Run メソッド内で defer されるため、ここでは不要

  // --- コンポーネントの生成 ---
  // Reader, Processor, Writer の実際のインスタンスとダミーインスタンスを生成
  weatherReaderCfg := &config.WeatherReaderConfig{
    APIEndpoint: cfg.Batch.APIEndpoint,
    APIKey:      cfg.Batch.APIKey,
  }
  actualReader := stepReader.NewWeatherReader(weatherReaderCfg)
  actualProcessor := stepProcessor.NewWeatherProcessor()
  actualWriter := stepWriter.NewWeatherWriter(repo)

  // ★ ダミーコンポーネントをそれぞれのパッケージから生成 ★
  dummyReaderInstance := dummyReader.NewDummyReader()
  dummyProcessorInstance := dummyProcessor.NewDummyProcessor()
  dummyWriterInstance := dummyWriter.NewDummyWriter()

  // RetryListener と LoggingListener は共通して使用
  retryCfg := &cfg.Batch.Retry
  retryListener := stepListener.NewRetryListener(retryCfg)
  loggingCfg := &cfg.System.Logging
  loggingStepListener := stepListener.NewLoggingListener(loggingCfg)
  loggingJobListener := jobListener.NewLoggingJobListener(loggingCfg)


  // --- ステップの生成 (複数ステップ) ---

  // FetchAndProcessStep: Reader と Processor を使用し、Writer はダミー
  fetchAndProcessStep := step.NewChunkOrientedStep(
    "FetchAndProcessStep", // ステップ名
    actualReader,
    actualProcessor,
    dummyWriterInstance, // ★ ダミー Writer インスタンスを使用
    cfg.Batch.ChunkSize,
    retryCfg,
  )
  // ステップリスナーを FetchAndProcessStep に登録
  logger.Debugf("Registering Step Listeners to FetchAndProcessStep")
  fetchAndProcessStep.RegisterListener(retryListener)
  fetchAndProcessStep.RegisterListener(loggingStepListener)


  // SaveDataStep: Writer を使用し、Reader と Processor はダミー
  saveDataStep := step.NewChunkOrientedStep(
    "SaveDataStep", // ステップ名
    dummyReaderInstance, // ★ ダミー Reader インスタンスを使用
    dummyProcessorInstance, // ★ ダミー Processor インスタンスを使用
    actualWriter,
    cfg.Batch.ChunkSize, // ChunkSize は Writer 側でも使用される可能性があるため渡す
    retryCfg, // リトライ設定 (書き込み処理のリトライに使用)
  )
  // ステップリスナーを SaveDataStep に登録
  logger.Debugf("Registering Step Listeners to SaveDataStep")
  saveDataStep.RegisterListener(retryListener)
  saveDataStep.RegisterListener(loggingStepListener)


  // --- フロー定義の生成 ---
  // ★ 複数ステップとそれらの間の遷移ルールを定義します。
  flow := core.NewFlowDefinition("FetchAndProcessStep") // 開始要素は FetchAndProcessStep

  // フローにステップを追加
  flow.AddElement("FetchAndProcessStep", fetchAndProcessStep)
  flow.AddElement("SaveDataStep", saveDataStep)

  // --- 遷移ルールの追加 ---

  // FetchAndProcessStep からの遷移ルール
  // COMPLETED で終了したら SaveDataStep へ遷移
  flow.AddTransitionRule(
    "FetchAndProcessStep",          // from
    string(core.ExitStatusCompleted), // on
    "SaveDataStep",                 // to
    false, core.ExitStatusUnknown, // end, endStatus
    false, core.ExitStatusUnknown, // fail, failStatus
    false, false, // stop, restartable
  )
  // FAILED で終了したらジョブを FAILED で終了
  flow.AddTransitionRule(
    "FetchAndProcessStep",       // from
    string(core.ExitStatusFailed), // on
    "",                            // to (終了遷移)
    false, core.ExitStatusUnknown, // end, endStatus
    true, core.ExitStatusFailed, // fail, failStatus
    false, false, // stop, restartable
  )

  // SaveDataStep からの遷移ルール
  // COMPLETED で終了したらジョブを COMPLETED で終了
  flow.AddTransitionRule(
    "SaveDataStep",           // from
    string(core.ExitStatusCompleted), // on
    "",                       // to (終了遷移)
    true, core.ExitStatusCompleted, // end, endStatus
    false, core.ExitStatusUnknown, // fail, failStatus
    false, false, // stop, restartable
  )
  // FAILED で終了したらジョブを FAILED で終了
  flow.AddTransitionRule(
    "SaveDataStep",       // from
    string(core.ExitStatusFailed), // on
    "",                   // to (終了遷移)
    false, core.ExitStatusUnknown, // end, endStatus
    true, core.ExitStatusFailed, // fail, failStatus
    false, false, // stop, restartable
  )


  // --- ジョブの生成 ---
  // WeatherJob オブジェクトを生成時に JobRepository と FlowDefinition を渡す
  weatherJob := job.NewWeatherJob(
    jobRepository,
    cfg,
    flow, // ★ FlowDefinition を渡す
  )

  // JobExecutionListener をここで生成し、ジョブに登録
  logger.Debugf("Registering Job Execution Listeners to WeatherJob")
  weatherJob.RegisterJobListener(loggingJobListener)

  logger.Debugf("WeatherJob created and configured successfully with multiple steps and transitions in weather_factory")

  return weatherJob, nil
}
