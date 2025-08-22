package factory

import (
	"fmt"

	component "sample/pkg/batch/job/component"
	config "sample/pkg/batch/config"
	core "sample/pkg/batch/job/core"
	jsl "sample/pkg/batch/job/jsl" // jsl パッケージをインポート
	jobListener "sample/pkg/batch/job/listener"
	logger "sample/pkg/batch/util/logger"
	exception "sample/pkg/batch/util/exception"
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
)

// StepExecutionListenerBuilder は StepExecutionListener を生成するための関数型です。
type StepExecutionListenerBuilder func(cfg *config.Config) (core.StepExecutionListener, error)

// ItemReadListenerBuilder は core.ItemReadListener を生成するための関数型です。
type ItemReadListenerBuilder func(cfg *config.Config) (core.ItemReadListener, error)

// ItemProcessListenerBuilder は core.ItemProcessListener を生成するための関数型です。
type ItemProcessListenerBuilder func(cfg *config.Config) (core.ItemProcessListener, error)

// ItemWriteListenerBuilder は core.ItemWriteListener を生成するための関数型です。
type ItemWriteListenerBuilder func(cfg *config.Config) (core.ItemWriteListener, error)

// SkipListenerBuilder は SkipListener を生成するための関数型です。
type SkipListenerBuilder func(cfg *config.Config) (core.SkipListener, error)

// RetryItemListenerBuilder は RetryItemListener を生成するための関数型です。
type RetryItemListenerBuilder func(cfg *config.Config) (core.RetryItemListener, error)


// JobListenerBuilder は JobExecutionListener を生成するための関数型です。
// 依存関係 (config など) を受け取り、生成されたリスナーとエラーを返します。
type JobListenerBuilder func(cfg *config.Config) (jobListener.JobExecutionListener, error)

// JobParametersIncrementerBuilder は JobParametersIncrementer を生成するための関数型です。
type JobParametersIncrementerBuilder func(cfg *config.Config, properties map[string]string) (core.JobParametersIncrementer, error) // ★ 追加

// JobBuilder は、特定の Job を生成するための関数型です。
// 依存関係 (jobRepository, config, listeners, flow) を受け取り、生成された core.Job インターフェースとエラーを返します。
type JobBuilder func(
	jobRepository job.JobRepository,
	cfg *config.Config,
	listeners []jobListener.JobExecutionListener,
	flow *core.FlowDefinition,
) (core.Job, error)

// JobFactory は Job オブジェクトを生成するためのファクトリです。
type JobFactory struct {
	config            *config.Config
	jobRepository     job.JobRepository
	componentBuilders map[string]component.ComponentBuilder
	jobBuilders       map[string]JobBuilder
	jobListenerBuilders map[string]JobListenerBuilder
	stepListenerBuilders map[string]StepExecutionListenerBuilder
	itemReadListenerBuilders map[string]ItemReadListenerBuilder
	itemProcessListenerBuilders map[string]ItemProcessListenerBuilder
	itemWriteListenerBuilders map[string]ItemWriteListenerBuilder
	skipListenerBuilders map[string]SkipListenerBuilder
	retryItemListenerBuilders map[string]RetryItemListenerBuilder
	jobParametersIncrementerBuilders map[string]JobParametersIncrementerBuilder // ★ 追加
}

// NewJobFactory は新しい JobFactory のインスタンスを作成します。
// JobRepository を引数に追加
func NewJobFactory(cfg *config.Config, repo job.JobRepository) *JobFactory { // job.JobRepository を受け取る
	jf := &JobFactory{
		config:        cfg,
		jobRepository: repo,
		componentBuilders: make(map[string]component.ComponentBuilder),
		jobBuilders:       make(map[string]JobBuilder),
		jobListenerBuilders: make(map[string]JobListenerBuilder), // 初期化
		stepListenerBuilders: make(map[string]StepExecutionListenerBuilder),
		itemReadListenerBuilders: make(map[string]ItemReadListenerBuilder),
		itemProcessListenerBuilders: make(map[string]ItemProcessListenerBuilder),
		itemWriteListenerBuilders: make(map[string]ItemWriteListenerBuilder),
		skipListenerBuilders: make(map[string]SkipListenerBuilder),
		retryItemListenerBuilders: make(map[string]RetryItemListenerBuilder),
		jobParametersIncrementerBuilders: make(map[string]JobParametersIncrementerBuilder), // ★ 追加
	}
	return jf
}

// RegisterComponentBuilder は、指定された名前でコンポーネントビルド関数を登録します。
// このメソッドは main.go など、アプリケーションの初期化フェーズで呼び出されます。
func (f *JobFactory) RegisterComponentBuilder(name string, builder component.ComponentBuilder) {
	f.componentBuilders[name] = builder
	logger.Debugf("JobFactory: コンポーネントビルダー '%s' を登録しました。", name)
}

// RegisterJobBuilder は、指定された名前でジョブビルド関数を登録します。
// このメソッドは main.go など、アプリケーションの初期化フェーズで呼び出されます。
func (f *JobFactory) RegisterJobBuilder(name string, builder JobBuilder) {
	f.jobBuilders[name] = builder
	logger.Debugf("JobFactory: ジョブビルダー '%s' を登録しました。", name)
}

// RegisterJobListenerBuilder は、指定された名前で JobExecutionListener ビルド関数を登録します。
func (f *JobFactory) RegisterJobListenerBuilder(name string, builder JobListenerBuilder) {
	f.jobListenerBuilders[name] = builder
	logger.Debugf("JobFactory: JobExecutionListener ビルダー '%s' を登録しました。", name)
}

// RegisterStepExecutionListenerBuilder は、指定された名前で StepExecutionListener ビルド関数を登録します。
func (f *JobFactory) RegisterStepExecutionListenerBuilder(name string, builder StepExecutionListenerBuilder) {
	f.stepListenerBuilders[name] = builder
	logger.Debugf("JobFactory: StepExecutionListener ビルダー '%s' を登録しました。", name)
}

// RegisterItemReadListenerBuilder は、指定された名前で ItemReadListener ビルド関数を登録します。
func (f *JobFactory) RegisterItemReadListenerBuilder(name string, builder ItemReadListenerBuilder) {
	f.itemReadListenerBuilders[name] = builder
	logger.Debugf("JobFactory: ItemReadListener ビルダー '%s' を登録しました。", name)
}

// RegisterItemProcessListenerBuilder は、指定された名前で ItemProcessListener ビルド関数を登録します。
func (f *JobFactory) RegisterItemProcessListenerBuilder(name string, builder ItemProcessListenerBuilder) {
	f.itemProcessListenerBuilders[name] = builder
	logger.Debugf("JobFactory: ItemProcessListener ビルダー '%s' を登録しました。", name)
}

// RegisterItemWriteListenerBuilder は、指定された名前で ItemWriteListener ビルド関数を登録します。
func (f *JobFactory) RegisterItemWriteListenerBuilder(name string, builder ItemWriteListenerBuilder) {
	f.itemWriteListenerBuilders[name] = builder
	logger.Debugf("JobFactory: ItemWriteListener ビルダー '%s' を登録しました。", name)
}

// RegisterSkipListenerBuilder は、指定された名前で SkipListener ビルド関数を登録します。
func (f *JobFactory) RegisterSkipListenerBuilder(name string, builder SkipListenerBuilder) {
	f.skipListenerBuilders[name] = builder
	logger.Debugf("JobFactory: SkipListener ビルダー '%s' を登録しました。", name)
}

// RegisterRetryItemListenerBuilder は、指定された名前で RetryItemListener ビルド関数を登録します。
func (f *JobFactory) RegisterRetryItemListenerBuilder(name string, builder RetryItemListenerBuilder) {
	f.retryItemListenerBuilders[name] = builder
	logger.Debugf("JobFactory: RetryItemListener ビルダー '%s' を登録しました。", name)
}

// RegisterJobParametersIncrementerBuilder は、指定された名前で JobParametersIncrementer ビルド関数を登録します。
func (f *JobFactory) RegisterJobParametersIncrementerBuilder(name string, builder JobParametersIncrementerBuilder) { // ★ 追加
	f.jobParametersIncrementerBuilders[name] = builder
	logger.Debugf("JobFactory: JobParametersIncrementer ビルダー '%s' を登録しました。", name)
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

	// 2. ジョブビルダーを取得
	jobBuilder, found := f.jobBuilders[jobName]
	if !found {
		return nil, exception.NewBatchErrorf("job_factory", "指定された Job '%s' のビルダーが登録されていません", jobName)
	}

	// 4. JSL Flow を core.FlowDefinition に変換
	// jsl.ConvertJSLToCoreFlow に componentBuilders (map[string]component.ComponentBuilder) を渡す
	// ConvertJSLToCoreFlow に渡すリスナービルダーマップ
	stepListenerBuilders := make(map[string]any)
	for k, v := range f.stepListenerBuilders {
		stepListenerBuilders[k] = (func(*config.Config) (core.StepExecutionListener, error))(v)
	}
	itemReadListenerBuilders := make(map[string]any)
	for k, v := range f.itemReadListenerBuilders {
		itemReadListenerBuilders[k] = (func(*config.Config) (core.ItemReadListener, error))(v)
	}
	itemProcessListenerBuilders := make(map[string]any)
	for k, v := range f.itemProcessListenerBuilders {
		itemProcessListenerBuilders[k] = (func(*config.Config) (core.ItemProcessListener, error))(v)
	}
	itemWriteListenerBuilders := make(map[string]any)
	for k, v := range f.itemWriteListenerBuilders {
		itemWriteListenerBuilders[k] = (func(*config.Config) (core.ItemWriteListener, error))(v)
	}
	skipListenerBuilders := make(map[string]any)
	for k, v := range f.skipListenerBuilders {
		skipListenerBuilders[k] = (func(*config.Config) (core.SkipListener, error))(v)
	}
	retryItemListenerBuilders := make(map[string]any)
	for k, v := range f.retryItemListenerBuilders {
		retryItemListenerBuilders[k] = (func(*config.Config) (core.RetryItemListener, error))(v)
	}

	// ConvertJSLToCoreFlow に componentBuilders を渡すように変更
	coreFlow, err := jsl.ConvertJSLToCoreFlow(
		jslJob.Flow,
		f.componentBuilders, // ここを componentBuilders に変更
		f.jobRepository,
		f.config, // Config 全体を渡す
		stepListenerBuilders,
		itemReadListenerBuilders,
		itemProcessListenerBuilders,
		itemWriteListenerBuilders,
		skipListenerBuilders,
		retryItemListenerBuilders,
	)
	if err != nil {
		return nil, exception.NewBatchError("job_factory", fmt.Sprintf("JSL ジョブ '%s' のフロー変換に失敗しました", jobName), err, false, false)
	}

	// 5. JSL 定義から JobExecutionListener をインスタンス化
	var jobListeners []jobListener.JobExecutionListener
	for _, listenerRef := range jslJob.Listeners {
		builder, found := f.jobListenerBuilders[listenerRef.Ref]
		if !found {
			return nil, exception.NewBatchErrorf("job_factory", "JobExecutionListener '%s' のビルダーが登録されていません", listenerRef.Ref)
		}
		listenerInstance, err := builder(f.config) // Config を渡す
		if err != nil {
			return nil, exception.NewBatchError("job_factory", fmt.Sprintf("JobExecutionListener '%s' のビルドに失敗しました", listenerRef.Ref), err, false, false)
		}
		jobListeners = append(jobListeners, listenerInstance)
		logger.Debugf("JobExecutionListener '%s' を生成しました。", listenerRef.Ref)
	}
	// デフォルトのリスナーを JSL で指定されたリスナーに追加することも可能
	// 例: jobListeners = append(jobListeners, joblistener.NewDefaultJobListener())

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

// GetJobParametersIncrementer は指定されたジョブの JobParametersIncrementer を構築して返します。
func (f *JobFactory) GetJobParametersIncrementer(jobName string) core.JobParametersIncrementer { // ★ 追加
	jslJob, ok := jsl.GetJobDefinition(jobName)
	if !ok || jslJob.Incrementer.Ref == "" {
		return nil
	}

	builder, found := f.jobParametersIncrementerBuilders[jslJob.Incrementer.Ref]
	if !found {
		logger.Warnf("JobFactory: JobParametersIncrementer '%s' のビルダーが登録されていません。", jslJob.Incrementer.Ref)
		return nil
	}

	incrementer, err := builder(f.config, jslJob.Incrementer.Properties)
	if err != nil {
		logger.Errorf("JobFactory: JobParametersIncrementer '%s' のビルドに失敗しました: %v", jslJob.Incrementer.Ref, err)
		return nil
	}
	return incrementer
}
