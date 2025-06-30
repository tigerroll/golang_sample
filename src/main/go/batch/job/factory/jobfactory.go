package factory

import (
	"database/sql" // Add sql import for *sql.DB
	"fmt"
	"reflect"

	config "sample/src/main/go/batch/config"
	core "sample/src/main/go/batch/job/core"
	jsl "sample/src/main/go/batch/job/jsl"
	jobListener "sample/src/main/go/batch/job/listener"
	stepListener "sample/src/main/go/batch/step/listener" // Keep stepListener for step-level listeners
	repository "sample/src/main/go/batch/repository"
	logger "sample/src/main/go/batch/util/logger"
	exception "sample/src/main/go/batch/util/exception"
)

// ComponentBuilder は、特定のコンポーネント（Reader, Processor, Writer）を生成するための関数型です。
// 依存関係 (config, db など) を受け取り、生成されたコンポーネントのインターフェースとエラーを返します。
// ジェネリックインターフェースを返すため、any を使用します。
type ComponentBuilder func(cfg *config.Config, db *sql.DB) (any, error) // Changed to *sql.DB

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
	jobRepository     repository.JobRepository
	componentBuilders map[string]ComponentBuilder
	jobBuilders       map[string]JobBuilder
}

// NewJobFactory は新しい JobFactory のインスタンスを作成します。
// JobRepository を引数に追加
func NewJobFactory(cfg *config.Config, repo repository.JobRepository) *JobFactory {
	// JSL 定義のロードは main.go で行われるため、ここでは不要
	// if err := jsl.LoadJSLDefinitions(); err != nil {
	// 	logger.Fatalf("JSL 定義のロードに失敗しました: %v", err)
	// }

	jf := &JobFactory{
		config:        cfg,
		jobRepository: repo,
		componentBuilders: make(map[string]ComponentBuilder),
		jobBuilders:       make(map[string]JobBuilder),
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
	// JobRepository から基盤となる *sql.DB 接続を取得し、ComponentBuilder に渡す
	sqlJobRepo, ok := f.jobRepository.(*repository.SQLJobRepository)
	if !ok {
		return nil, exception.NewBatchErrorf("job_factory", "JobRepository の実装が予期された型ではありません。*sql.DB 接続を取得できません。")
	}
	dbConnection := sqlJobRepo.GetDB()
	if dbConnection == nil {
		return nil, exception.NewBatchErrorf("job_factory", "JobRepository からデータベース接続を取得できませんでした。")
	}

	componentInstances := make(map[string]any) // map[string]any に変更

	// 登録されたビルド関数を使用してコンポーネントを動的にインスタンス化
	for componentRefName, builder := range f.componentBuilders {
		instance, err := builder(f.config, dbConnection) // Pass Config and *sql.DB
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
	stepListeners := []stepListener.StepExecutionListener{ // Use stepListener.StepExecutionListener
		stepListener.NewLoggingListener(&f.config.System.Logging), // LoggingListener を追加
		stepListener.NewRetryListener(&f.config.Batch.Retry),     // RetryListener を追加
	}
	itemReadListeners := []core.ItemReadListener{}
	itemProcessListeners := []core.ItemProcessListener{}
	itemWriteListeners := []core.ItemWriteListener{}
	skipListeners := []stepListener.SkipListener{ // Use stepListener.SkipListener
		stepListener.NewLoggingSkipListener(), // LoggingSkipListener を追加
	}
	retryItemListeners := []stepListener.RetryItemListener{ // Use stepListener.RetryItemListener
		stepListener.NewLoggingRetryItemListener(), // LoggingRetryItemListener を追加
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

	return jobInstance, nil
}
