// src/main/go/batch/job/factory/jobfactory.go
package factory

import (
	"fmt"
	"reflect" // reflect パッケージをインポート

	config "sample/src/main/go/batch/config"
	core "sample/src/main/go/batch/job/core"
	jsl "sample/src/main/go/batch/job/jsl"   // JSL loader をインポート
	jobListener "sample/src/main/go/batch/job/listener" // jobListener パッケージをインポート
	repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
	weather_repo "sample/src/main/go/batch/weather/repository" // weather_repo パッケージをインポート
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
	exception "sample/src/main/go/batch/util/exception" // exception パッケージをインポート
)

// ComponentBuilder は、特定のコンポーネント（Reader, Processor, Writer）を生成するための関数型です。
// 依存関係 (config, repo など) を受け取り、生成されたコンポーネントのインターフェースとエラーを返します。
// ジェネリックインターフェースを返すため、any を使用します。
type ComponentBuilder func(cfg *config.Config, weatherRepo weather_repo.WeatherRepository) (any, error) // weatherRepo を受け取るように修正

// JobBuilder は、特定の Job を生成するための関数型です。
// 依存関係 (jobRepository, config, listeners, flow) を受け取り、生成された core.Job インターフェースとエラーを返します。
type JobBuilder func(
	jobRepository repository.JobRepository,
	cfg *config.Config,
	listeners []jobListener.JobExecutionListener,
	flow *core.FlowDefinition,
) (core.Job, error)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
	config            *config.Config
	jobRepository     repository.JobRepository // JobRepository を依存として追加
	componentBuilders map[string]ComponentBuilder // コンポーネント生成関数を保持するマップ
	jobBuilders       map[string]JobBuilder       // ★ 追加: ジョブ生成関数を保持するマップ
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
		jobBuilders:       make(map[string]JobBuilder), // ★ 初期化
	}
	return jf
}

// RegisterComponentBuilder は、指定された名前でコンポーネントビルド関数を登録します。
// このメソッドは main.go など、アプリケーションの初期化フェーズで呼び出されます。
func (f *JobFactory) RegisterComponentBuilder(name string, builder ComponentBuilder) {
	f.componentBuilders[name] = builder
	logger.Debugf("JobFactory: コンポーネントビルダー '%s' を登録しました。", name)
}

// RegisterJobBuilder は、指定された名前でジョブビルド関数を登録します。
// このメソッドは main.go など、アプリケーションの初期化フェーズで呼び出されます。
func (f *JobFactory) RegisterJobBuilder(name string, builder JobBuilder) {
	f.jobBuilders[name] = builder
	logger.Debugf("JobFactory: ジョブビルダー '%s' を登録しました。", name)
}

// registerComponentBuilders は、利用可能な全てのコンポーネントのビルド関数を登録します。
func (f *JobFactory) registerComponentBuilders() { /* このメソッドは削除または空にする */ }

// registerJobBuilders は、利用可能な全てのジョブのビルド関数を登録します。
func (f *JobFactory) registerJobBuilders() { /* このメソッドは削除または空にする */ }

// CreateJob は指定されたジョブ名の core.Job オブジェクトを作成します。
// JSL 定義からジョブを構築するロジックをここに集約します。
func (f *JobFactory) CreateJob(jobName string) (core.Job, error) { // Returns core.Job, error
	logger.Debugf("JobFactory で Job '%s' の作成を試みます。", jobName)

	// 1. JSL 定義からジョブをロード
	jslJob, ok := jsl.GetJobDefinition(jobName)
	if !ok {
		return nil, exception.NewBatchErrorf("job_factory", "指定された Job '%s' のJSL定義が見つかりません", jobName)
	}

	// 2. ジョブビルダーを取得
	jobBuilder, found := f.jobBuilders[jobName]
	if !found {
		return nil, exception.NewBatchErrorf("job_factory", "指定された Job '%s' のビルダーが登録されていません", jobName)
	}

	// 3. JSL 定義に基づいてコンポーネントをインスタンス化し、レジストリに登録
	// NOTE: weatherRepo は JobFactory の CreateJob メソッド内で生成されるため、
	// ComponentBuilder のシグネチャには残しておく必要があります。
	// ただし、weather_repo パッケージ自体は JobFactory のトップレベルでは不要です。
	// 必要に応じて、weatherRepo の生成ロジックを ComponentBuilder のクロージャ内に移動することも検討できます。
	componentInstances := make(map[string]any) // map[string]any に変更

	// WeatherRepository は WeatherWriter が必要とするので、ここで作成する。
	// config.Database.Type に応じて適切なリポジトリを生成
	var weatherRepo weather_repo.WeatherRepository // ここで weather_repo を使用
	var err error
	switch f.config.Database.Type {
	case "postgres":
		db, dbErr := repository.NewPostgresRepositoryFromConfig(f.config.Database)
		if dbErr != nil {
			return nil, exception.NewBatchError("job_factory", "PostgresRepository の生成に失敗しました", dbErr, false, false)
		}
		weatherRepo = db // weather_repo.WeatherRepository 型
	case "mysql":
		db, dbErr := repository.NewMySQLRepositoryFromConfig(f.config.Database)
		if dbErr != nil {
			return nil, exception.NewBatchError("job_factory", "MySQLRepository の生成に失敗しました", dbErr, false, false)
		}
		weatherRepo = db // weather_repo.WeatherRepository 型
	case "redshift":
		db, dbErr := repository.NewRedshiftRepositoryFromConfig(f.config.Database)
		if dbErr != nil {
			return nil, exception.NewBatchError("job_factory", "RedshiftRepository の生成に失敗しました", dbErr, false, false)
		}
		weatherRepo = db // weather_repo.WeatherRepository 型
	default:
		return nil, exception.NewBatchErrorf("job_factory", "未対応のデータベースタイプです: %s", f.config.Database.Type)
	}
	if err != nil { // This err check is for the switch block, but it's always nil here. The dbErr is checked inside.
		return nil, exception.NewBatchError("job_factory", "WeatherRepository の生成に失敗しました", err, false, false)
	}
	// Note: リポジトリのリソース解放 (Close メソッドを持つ場合) は Job の Run メソッド内で defer されるため、ここでは不要

	// 登録されたビルド関数を使用してコンポーネントを動的にインスタンス化
	for componentRefName, builder := range f.componentBuilders {
		instance, err := builder(f.config, weatherRepo) // Config と WeatherRepository を渡す
		if err != nil {
			return nil, exception.NewBatchError("job_factory", fmt.Sprintf("コンポーネント '%s' のビルドに失敗しました", componentRefName), err, false, false)
		}
		componentInstances[componentRefName] = instance
		logger.Debugf("コンポーネント '%s' (Type: %s) を生成しました。", componentRefName, reflect.TypeOf(instance))
	}

	// 4. JSL Flow を core.FlowDefinition に変換
	// jobRepository を ConvertJSLToCoreFlow に渡す
	// StepExecutionListener を生成
	// アイテムレベルリスナーもここで生成し、JSLAdaptedStep に渡す
	// 現状は StepExecutionListener のリストにまとめて渡すため、型アサーションで判別する
	stepListeners := []jobListener.StepExecutionListener{ // jobListener.StepExecutionListener を使用
		jobListener.NewLoggingListener(&f.config.System.Logging), // LoggingListener を追加
		jobListener.NewRetryListener(&f.config.Batch.Retry),     // RetryListener を追加
	}
	itemReadListeners := []core.ItemReadListener{}
	itemProcessListeners := []core.ItemProcessListener{}
	itemWriteListeners := []core.ItemWriteListener{}
	skipListeners := []jobListener.SkipListener{ // jobListener.SkipListener を使用
		jobListener.NewLoggingSkipListener(), // LoggingSkipListener を追加
	}
	retryItemListeners := []jobListener.RetryItemListener{ // jobListener.RetryItemListener を使用
		jobListener.NewLoggingRetryItemListener(), // LoggingRetryItemListener を追加
	}

	// ConvertJSLToCoreFlow に componentRegistry (map[string]any) を渡す
	// そして、JSLAdaptedStep のコンストラクタに渡す際に、適切な型アサーションを行う
	coreFlow, err := jsl.ConvertJSLToCoreFlow(jslJob.Flow, componentInstances, f.jobRepository, &f.config.Batch.Retry, f.config.Batch.ItemRetry, f.config.Batch.ItemSkip, stepListeners, itemReadListeners, itemProcessListeners, itemWriteListeners, skipListeners, retryItemListeners)
	if err != nil {
		return nil, exception.NewBatchError("job_factory", fmt.Sprintf("JSL ジョブ '%s' のフロー変換に失敗しました", jobName), err, false, false)
	}

	// 5. JobExecutionListener を生成し、ジョブに登録
	jobListeners := []jobListener.JobExecutionListener{
		jobListener.NewLoggingJobListener(&f.config.System.Logging), // LoggingConfig を渡す
		// 他の共通リスナーがあればここに追加
	}

	// 6. 登録されたジョブビルダーを使用して Job インスタンスを作成
	jobInstance, err := jobBuilder(
		f.jobRepository, // JobRepository を渡す
		f.config,        // Config を渡す
		jobListeners,    // 構築した JobExecutionListener を渡す
		coreFlow,        // 構築した FlowDefinition を渡す
	)
	// jobBuilder から返されたエラーをチェック
	if err != nil {
		return nil, exception.NewBatchError("job_factory", fmt.Sprintf("ジョブ '%s' のインスタンス化に失敗しました", jobName), err, false, false)
	}

	logger.Debugf("Job '%s' created and configured successfully from JSL definition.", jobName)

	return jobInstance, nil // Returns core.Job, error
}
