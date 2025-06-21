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
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
	exception "sample/src/main/go/batch/util/exception" // exception パッケージをインポート

	// ★ ダミーコンポーネントをそれぞれのパッケージからインポート ★
	dummyProcessor "sample/src/main/go/batch/step/processor" // dummy_processor.go がこのパッケージに属する
	itemListener "sample/src/main/go/batch/step/listener" // ★ 追加: itemListener パッケージをインポート
	itemRetryListener "sample/src/main/go/batch/step/listener" // ★ 追加: itemRetryListener パッケージをインポート
	stepListener "sample/src/main/go/batch/step/listener" // ★ 追加: stepListener パッケージをインポート
	dummyReader "sample/src/main/go/batch/step/reader"       // dummy_reader.go がこのパッケージに属する
	dummyWriter "sample/src/main/go/batch/step/writer"       // dummy_writer.go がこのパッケージに属する
)

// ComponentBuilder は、特定のコンポーネント（Reader, Processor, Writer）を生成するための関数型です。
// 依存関係（Config, Repositoryなど）を受け取り、生成されたコンポーネントのインターフェースとエラーを返します。
// ジェネリックインターフェースを返すため、any を使用します。
type ComponentBuilder func(cfg *config.Config, repo repository.WeatherRepository) (any, error)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
	config            *config.Config
	jobRepository     repository.JobRepository // JobRepository を依存として追加
	componentBuilders map[string]ComponentBuilder // コンポーネント生成関数を保持するマップ
}

// NewJobFactory は新しい JobFactory のインスタンスを作成します。
// JobRepository を引数に追加
func NewJobFactory(cfg *config.Config, repo repository.JobRepository) *JobFactory {
	// アプリケーション起動時にJSL定義を一度ロード
	if err := jsl.LoadJSLDefinitions(); err != nil {
		logger.Fatalf("JSL 定義のロードに失敗しました: %v", err)
	}

	jf := &JobFactory{
		config:        cfg,
		jobRepository: repo, // JobRepository を初期化
		componentBuilders: make(map[string]ComponentBuilder),
	}

	// コンポーネントビルダーを登録
	jf.registerComponentBuilders()

	return jf
}

// registerComponentBuilders は、利用可能な全てのコンポーネントのビルド関数を登録します。
func (f *JobFactory) registerComponentBuilders() {
	f.componentBuilders["weatherReader"] = func(cfg *config.Config, repo repository.WeatherRepository) (any, error) {
		weatherReaderCfg := &config.WeatherReaderConfig{
			APIEndpoint: cfg.Batch.APIEndpoint,
			APIKey:      cfg.Batch.APIKey,
		}
		// Reader[*entity.OpenMeteoForecast] を返す
		return NewWeatherReader(weatherReaderCfg), nil
	}
	f.componentBuilders["weatherProcessor"] = func(cfg *config.Config, repo repository.WeatherRepository) (any, error) {
		// Processor[*entity.OpenMeteoForecast, []*entity.WeatherDataToStore] を返す
		return NewWeatherProcessor(), nil
	}
	f.componentBuilders["weatherWriter"] = func(cfg *config.Config, repo repository.WeatherRepository) (any, error) {
		// Writer[*entity.WeatherDataToStore] を返す
		return NewWeatherWriter(repo), nil
	}
	f.componentBuilders["dummyReader"] = func(cfg *config.Config, repo repository.WeatherRepository) (any, error) {
		// Reader[any] を返す
		return dummyReader.NewDummyReader(), nil
	}
	f.componentBuilders["dummyProcessor"] = func(cfg *config.Config, repo repository.WeatherRepository) (any, error) {
		// Processor[any, any] を返す
		return dummyProcessor.NewDummyProcessor(), nil
	}
	f.componentBuilders["dummyWriter"] = func(cfg *config.Config, repo repository.WeatherRepository) (any, error) {
		// Writer[any] を返す
		return dummyWriter.NewDummyWriter(), nil
	}
	// 他のコンポーネントがあればここに追加登録
}

// CreateJob は指定されたジョブ名の core.Job オブジェクトを作成します。
// JSL 定義からジョブを構築するロジックをここに集約します。
func (f *JobFactory) CreateJob(jobName string) (core.Job, error) { // Returns core.Job, error
	logger.Debugf("JobFactory で Job '%s' の作成を試みます。", jobName)

	// 1. JSL 定義からジョブをロード
	jslJob, ok := jsl.GetJobDefinition(jobName)
	if !ok {
		return nil, exception.NewBatchErrorf("job_factory", "指定された Job '%s' のJSL定義が見つかりません", jobName)
	}

	// 2. JSL 定義に基づいてコンポーネントをインスタンス化し、レジストリに登録
	componentRegistry := make(map[string]any) // map[string]any に変更

	// WeatherRepository は WeatherWriter が必要とするので、ここで作成する。
	weatherRepo, err := repository.NewWeatherRepository(context.Background(), *f.config)
	if err != nil {
		return nil, exception.NewBatchError("job_factory", "WeatherRepository の生成に失敗しました", err, false, false)
	}
	// Note: リポジトリのリソース解放 (Close メソッドを持つ場合) は Job の Run メソッド内で defer されるため、ここでは不要

	// 登録されたビルド関数を使用してコンポーネントを動的にインスタンス化
	for componentRefName, builder := range f.componentBuilders {
		instance, err := builder(f.config, weatherRepo) // Config と WeatherRepository を渡す
		if err != nil {
			return nil, exception.NewBatchError("job_factory", fmt.Sprintf("コンポーネント '%s' のビルドに失敗しました", componentRefName), err, false, false)
		}
		componentRegistry[componentRefName] = instance
		logger.Debugf("コンポーネント '%s' をレジストリに登録しました。", componentRefName)
	}

	// 3. JSL Flow を core.FlowDefinition に変換
	// jobRepository を ConvertJSLToCoreFlow に渡す
	// StepExecutionListener を生成
	// アイテムレベルリスナーもここで生成し、JSLAdaptedStep に渡す
	// 現状は StepExecutionListener のリストにまとめて渡すため、型アサーションで判別する
	stepListeners := []stepListener.StepExecutionListener{
		stepListener.NewLoggingListener(&f.config.System.Logging), // LoggingListener を追加
		stepListener.NewRetryListener(&f.config.Batch.Retry),     // RetryListener を追加
	}
	itemReadListeners := []core.ItemReadListener{}
	itemProcessListeners := []core.ItemProcessListener{}
	itemWriteListeners := []core.ItemWriteListener{}
	skipListeners := []stepListener.SkipListener{
		itemListener.NewLoggingSkipListener(), // LoggingSkipListener を追加
	}
	retryItemListeners := []stepListener.RetryItemListener{
		itemRetryListener.NewLoggingRetryItemListener(), // LoggingRetryItemListener を追加
	}

	// ConvertJSLToCoreFlow に componentRegistry (map[string]any) を渡す
	// そして、JSLAdaptedStep のコンストラクタに渡す際に、適切な型アサーションを行う
	coreFlow, err := jsl.ConvertJSLToCoreFlow(jslJob.Flow, componentRegistry, f.jobRepository, &f.config.Batch.Retry, f.config.Batch.ItemRetry, f.config.Batch.ItemSkip, stepListeners, itemReadListeners, itemProcessListeners, itemWriteListeners, skipListeners, retryItemListeners)
	if err != nil {
		return nil, exception.NewBatchError("job_factory", fmt.Sprintf("JSL ジョブ '%s' のフロー変換に失敗しました", jobName), err, false, false)
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
