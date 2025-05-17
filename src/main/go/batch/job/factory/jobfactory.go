// (修正)
package factory

import (
  "context"
  "fmt"

  config  "sample/src/main/go/batch/config"
  core    "sample/src/main/go/batch/job/core"
  impl    "sample/src/main/go/batch/job/impl" // impl パッケージをインポート
  jobListener "sample/src/main/go/batch/job/listener" // jobListener パッケージをインポート
  repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
  stepListener "sample/src/main/go/batch/step/listener" // stepListener パッケージをインポート
  step "sample/src/main/go/batch/step" // step パッケージをインポート
  stepProcessor "sample/src/main/go/batch/step/processor" // stepProcessor パッケージをインポート
  stepReader "sample/src/main/go/batch/step/reader" // stepReader パッケージをインポート
  stepWriter "sample/src/main/go/batch/step/writer" // stepWriter パッケージをインポート
  logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート

  // ★ ダミーコンポーネントをそれぞれのパッケージからインポート ★
  dummyReader "sample/src/main/go/batch/step/reader" // dummy_reader.go がこのパッケージに属する
  dummyProcessor "sample/src/main/go/batch/step/processor" // dummy_processor.go がこのパッケージに属する
  dummyWriter "sample/src/main/go/batch/step/writer" // dummy_writer.go がこのパッケージに属する
)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
  config *config.Config
  jobRepository repository.JobRepository // JobRepository を依存として追加
}

// NewJobFactory は新しい JobFactory のインスタンスを作成します。
// JobRepository を引数に追加
func NewJobFactory(cfg *config.Config, repo repository.JobRepository) *JobFactory {
  return &JobFactory{
    config: cfg,
    jobRepository: repo, // JobRepository を初期化
  }
}

// CreateJob は指定されたジョブ名の core.Job オブジェクトを作成します。
// impl パッケージで定義された NewWeatherJob 関数を呼び出すように変更します。
func (f *JobFactory) CreateJob(jobName string) (core.Job, error) { // Returns core.Job, error
  logger.Debugf("JobFactory で Job '%s' の作成を試みます。", jobName)
  switch jobName {
  case "weather":
    // --- WeatherJob の依存関係とフロー定義をここで構築 ---
    logger.Debugf("Creating WeatherJob components and dependencies in JobFactory")

    // リポジトリをこのファクトリ関数内で生成する (WeatherRepository)
    // Note: JobRepository は JobFactory の依存として渡されている
    repo, err := repository.NewWeatherRepository(context.Background(), *f.config) // Context を渡す
    if err != nil {
      return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
    }
    // Note: リポジトリのリソース解放 (Close メソッドを持つ場合) は Job の Run メソッド内で defer されるため、ここでは不要

    // --- コンポーネントの生成 ---
    // Reader, Processor, Writer の実際のインスタンスとダミーインスタンスを生成
    weatherReaderCfg := &config.WeatherReaderConfig{
      APIEndpoint: f.config.Batch.APIEndpoint,
      APIKey:      f.config.Batch.APIKey,
    }
    actualReader := stepReader.NewWeatherReader(weatherReaderCfg)
    actualProcessor := stepProcessor.NewWeatherProcessor()
    actualWriter := stepWriter.NewWeatherWriter(repo) // WeatherRepository を渡す

    // ★ ダミーコンポーネントをそれぞれのパッケージからインポート ★
    dummyReaderInstance := dummyReader.NewDummyReader()
    dummyProcessorInstance := dummyProcessor.NewDummyProcessor()
    dummyWriterInstance := dummyWriter.NewDummyWriter()

    // RetryListener と LoggingListener は共通して使用
    retryCfg := &f.config.Batch.Retry
    retryListener := stepListener.NewRetryListener(retryCfg)
    // LoggingConfig を config から取得し、ポインタを渡す
    loggingCfg := &f.config.System.Logging
    loggingStepListener := stepListener.NewLoggingListener(loggingCfg)
    loggingJobListener := jobListener.NewLoggingJobListener(loggingCfg)


    // --- ステップの生成 (複数ステップ) ---

    // FetchAndProcessStep: Reader と Processor を使用し、Writer はダミー
    fetchAndProcessStep := step.NewChunkOrientedStep(
      "FetchAndProcessStep", // ステップ名
      actualReader,
      actualProcessor,
      dummyWriterInstance, // ★ ダミー Writer インスタンスを使用
      f.config.Batch.ChunkSize,
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
      actualWriter, // ★ Actual Writer インスタンスを使用
      f.config.Batch.ChunkSize, // ChunkSize は Writer 側でも使用される可能性があるため渡す
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
    // impl パッケージで定義された NewWeatherJob コンストラクタを呼び出す
    // このコンストラクタは *impl.WeatherJob を返しますが、core.Job インターフェースとして扱われます。
    // ★ NewWeatherJob の戻り値を一度 *impl.WeatherJob 型の変数に代入 ★
    var weatherJobImpl *impl.WeatherJob = impl.NewWeatherJob(
      f.jobRepository, // JobRepository を渡す
      f.config, // Config を渡す
      flow, // 構築した FlowDefinition を渡す
    )

    // JobExecutionListener をここで生成し、ジョブに登録
    logger.Debugf("Registering Job Execution Listeners to WeatherJob")
    loggingJobListener = jobListener.NewLoggingJobListener(&f.config.System.Logging) // LoggingConfig を渡す
    // ★ *impl.WeatherJob 型の変数に対して RegisterListener を呼び出す ★
    weatherJobImpl.RegisterListener(loggingJobListener)


    logger.Debugf("WeatherJob created and configured successfully with multiple steps and transitions in JobFactory")

    // ★ *impl.WeatherJob 型を core.Job インターフェース型にキャストして返す ★
    return weatherJobImpl, nil // Returns core.Job, error

  // 他の Job の case を追加
  default:
    return nil, fmt.Errorf("指定された Job '%s' は存在しません", jobName)
  }
}

// Note: weather_factory.go は削除されました。
// NewWeatherJob 関数は impl/weather_job.go に定義されています。
// JobFactory は impl/weather_job.go の NewWeatherJob を直接呼び出します。
