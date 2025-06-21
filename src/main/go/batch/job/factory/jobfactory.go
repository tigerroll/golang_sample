package factory

import (
	"context"
	"fmt"

	config "sample/src/main/go/batch/config"
	core "sample/src/main/go/batch/job/core"
	impl "sample/src/main/go/batch/job/impl" // impl パッケージをインポート
	jsl "sample/src/main/go/batch/job/jsl"   // JSL loader をインポート
	jobListener "sample/src/main/go/batch/job/listener" // jobListener パッケージをインポート
	repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
	stepProcessor "sample/src/main/go/batch/step/processor" // stepProcessor パッケージをインポート
	stepReader "sample/src/main/go/batch/step/reader" // stepReader パッケージをインポート
	stepWriter "sample/src/main/go/batch/step/writer" // stepWriter パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート

	// ★ ダミーコンポーネントをそれぞれのパッケージからインポート ★
	dummyProcessor "sample/src/main/go/batch/step/processor" // dummy_processor.go がこのパッケージに属する
	dummyReader "sample/src/main/go/batch/step/reader"       // dummy_reader.go がこのパッケージに属する
	dummyWriter "sample/src/main/go/batch/step/writer"       // dummy_writer.go がこのパッケージに属する
)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
	config        *config.Config
	jobRepository repository.JobRepository // JobRepository を依存として追加
}

// NewJobFactory は新しい JobFactory のインスタンスを作成します。
// JobRepository を引数に追加
func NewJobFactory(cfg *config.Config, repo repository.JobRepository) *JobFactory {
	// アプリケーション起動時にJSL定義を一度ロード
	if err := jsl.LoadJSLDefinitions(); err != nil {
		logger.Fatalf("JSL 定義のロードに失敗しました: %v", err)
	}
	return &JobFactory{
		config:        cfg,
		jobRepository: repo, // JobRepository を初期化
	}
}

// CreateJob は指定されたジョブ名の core.Job オブジェクトを作成します。
// JSL 定義からジョブを構築するロジックをここに集約します。
func (f *JobFactory) CreateJob(jobName string) (core.Job, error) { // Returns core.Job, error
	logger.Debugf("JobFactory で Job '%s' の作成を試みます。", jobName)

	// 1. JSL 定義からジョブをロード
	jslJob, ok := jsl.GetJobDefinition(jobName)
	if !ok {
		return nil, fmt.Errorf("指定された Job '%s' のJSL定義が見つかりません", jobName)
	}

	// 2. JSL 定義に基づいてコンポーネントをインスタンス化し、レジストリに登録
	componentRegistry := make(map[string]interface{})

	// WeatherRepository は WeatherWriter が必要とするので、ここで作成する。
	weatherRepo, err := repository.NewWeatherRepository(context.Background(), *f.config)
	if err != nil {
		return nil, fmt.Errorf("WeatherRepository の生成に失敗しました: %w", err)
	}
	// Note: リポジトリのリソース解放 (Close メソッドを持つ場合) は Job の Run メソッド内で defer されるため、ここでは不要

	// 実際のコンポーネントインスタンスを生成し、レジストリに登録
	weatherReaderCfg := &config.WeatherReaderConfig{
		APIEndpoint: f.config.Batch.APIEndpoint,
		APIKey:      f.config.Batch.APIKey,
	}
	componentRegistry["weatherReader"] = stepReader.NewWeatherReader(weatherReaderCfg)
	componentRegistry["weatherProcessor"] = stepProcessor.NewWeatherProcessor()
	componentRegistry["weatherWriter"] = stepWriter.NewWeatherWriter(weatherRepo) // WeatherRepository を渡す

	// ダミーコンポーネントもレジストリに登録
	componentRegistry["dummyReader"] = dummyReader.NewDummyReader()
	componentRegistry["dummyProcessor"] = dummyProcessor.NewDummyProcessor()
	componentRegistry["dummyWriter"] = dummyWriter.NewDummyWriter()

	// 3. JSL Flow を core.FlowDefinition に変換
	// jobRepository を ConvertJSLToCoreFlow に渡す
	// StepExecutionListener を生成
	stepListeners := []stepListener.StepExecutionListener{
		stepListener.NewLoggingListener(&f.config.System.Logging), // LoggingListener を追加
		stepListener.NewRetryListener(&f.config.Batch.Retry),     // RetryListener を追加
	}
	coreFlow, err := jsl.ConvertJSLToCoreFlow(jslJob.Flow, componentRegistry, f.jobRepository, &f.config.Batch.Retry, stepListeners)
	if err != nil {
		return nil, fmt.Errorf("JSL ジョブ '%s' のフロー変換に失敗しました: %w", jobName, err)
	}

	// 4. JobExecutionListener を生成し、ジョブに登録
	jobListeners := []jobListener.JobExecutionListener{
		jobListener.NewLoggingJobListener(&f.config.System.Logging), // LoggingConfig を渡す
		// 他の共通リスナーがあればここに追加
	}

	// 5. Job インスタンスの作成
	// impl パッケージで定義された NewWeatherJob コンストラクタを呼び出す
	// このコンストラクタは *impl.WeatherJob を返しますが、core.Job インターフェースとして扱われます。
	weatherJobImpl := impl.NewWeatherJob(
		f.jobRepository, // JobRepository を渡す
		f.config,        // Config を渡す
		jobListeners,    // 構築した JobExecutionListener を渡す
		coreFlow,        // 構築した FlowDefinition を渡す
	)

	logger.Debugf("WeatherJob created and configured successfully from JSL definition '%s'", jobName)

	return weatherJobImpl, nil // Returns core.Job, error
}
