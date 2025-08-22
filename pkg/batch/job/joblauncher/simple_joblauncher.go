package joblauncher

import (
	"context"
	"fmt" // fmt パッケージをインポート (エラーメッセージで使用)
	"sync" // ★ 追加

	core "sample/pkg/batch/job/core"
	factory "sample/pkg/batch/job/factory" // factory パッケージをインポート
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
)

// SimpleJobLauncher は JobLauncher インターフェースのシンプルな実装です。
// JobExecution の基本的なライフサイクル管理と JobRepository を使用した永続化を行います。
// JobFactory を依存として追加し、ジョブオブジェクトの生成も担当します。
type SimpleJobLauncher struct { // SimpleJobLauncher を返す
	jobRepository job.JobRepository // job.JobRepository を依存として追加
	jobFactory    factory.JobFactory     // JobFactory を依存として追加
	// 実行中のジョブのキャンセル関数を保持するマップ
	activeJobCancellations map[string]context.CancelFunc // ★ 追加
	mu                     sync.Mutex                    // ★ 追加: マップ保護のためのミューテックス
}

// NewSimpleJobLauncher は新しい SimpleJobLauncher のインスタンスを作成します。
// JobRepository と JobFactory の実装を受け取るように変更します。
func NewSimpleJobLauncher(jobRepository job.JobRepository, jobFactory factory.JobFactory) *SimpleJobLauncher { // job.JobRepository を受け取る
	return &SimpleJobLauncher{
		jobRepository: jobRepository, // JobRepository を初期化
		jobFactory:    jobFactory,    // JobFactory を初期化
		activeJobCancellations: make(map[string]context.CancelFunc), // ★ 追加
	}
}

// RegisterCancelFunc は実行中のジョブのキャンセル関数を登録します。
func (l *SimpleJobLauncher) RegisterCancelFunc(executionID string, cancelFunc context.CancelFunc) { // ★ 追加
	l.mu.Lock()
	defer l.mu.Unlock()
	l.activeJobCancellations[executionID] = cancelFunc
	logger.Debugf("JobExecution (ID: %s) の CancelFunc を登録しました。", executionID)
}

// UnregisterCancelFunc は実行中のジョブのキャンセル関数を登録解除します。
func (l *SimpleJobLauncher) UnregisterCancelFunc(executionID string) { // ★ 追加
	l.mu.Lock()
	defer l.mu.Unlock()
	if cancelFunc, ok := l.activeJobCancellations[executionID]; ok {
		cancelFunc() // 念のためキャンセルを呼び出す
		delete(l.activeJobCancellations, executionID)
		logger.Debugf("JobExecution (ID: %s) の CancelFunc を登録解除しました。", executionID)
	}
}

// GetCancelFunc は指定された JobExecution ID のキャンセル関数を取得します。
func (l *SimpleJobLauncher) GetCancelFunc(executionID string) (context.CancelFunc, bool) { // ★ 追加
	l.mu.Lock()
	defer l.mu.Unlock()
	cancelFunc, ok := l.activeJobCancellations[executionID]
	return cancelFunc, ok
}

// Launch は指定された Job を JobParameters とともに起動し、JobExecution を管理します。
// job 引数の型を core.Job に、params を core.JobParameters に、戻り値を *core.JobExecution に変更
// JobLauncher インターフェースを満たすようにメソッドシグネチャを維持
func (l *SimpleJobLauncher) Launch(ctx context.Context, jobName string, params core.JobParameters) (*core.JobExecution, error) { // ★ 修正: jobName を直接受け取る
	logger.Infof("JobLauncher を使用して Job '%s' を起動するよ。", jobName)

	// Step 1: JobFactory を使用して Job オブジェクトを取得 (JobParametersIncrementer の取得のため、先にJobを取得)
	batchJob, err := l.jobFactory.CreateJob(jobName)
	if err != nil {
		logger.Errorf("Job '%s' の作成に失敗しました: %v", jobName, err)
		return nil, exception.NewBatchError("job_launcher", fmt.Sprintf("Job '%s' の作成に失敗しました", jobName), err, false, false)
	}

	// Step 2: JobParameters のバリデーション
	if err := batchJob.ValidateParameters(params); err != nil { // ★ 追加
		logger.Errorf("Job '%s': JobParameters のバリデーションに失敗しました: %v", jobName, err)
		return nil, exception.NewBatchError("job_launcher", "JobParameters のバリデーションエラー", err, false, false)
	}

	// Step 3: JobInstance の取得または作成
	// ジョブ名とパラメータに一致する既存の JobInstance を検索します。
	jobInstance, err := l.jobRepository.FindJobInstanceByJobNameAndParameters(ctx, jobName, params)
	if err != nil {
		logger.Errorf("JobInstance (JobName: %s, Parameters: %+v) の検索に失敗しました: %v", jobName, params, err)
		return nil, exception.NewBatchError("job_launcher", "起動処理エラー: JobInstance の検索に失敗しました", err, false, false)
	}

	if jobInstance == nil {
		// 既存の JobInstance が見つからない場合、JobParametersIncrementer を使用して新しいパラメータを生成
		if incrementer := l.jobFactory.GetJobParametersIncrementer(jobName); incrementer != nil { // ★ 追加
			params = incrementer.GetNext(params)
			logger.Infof("JobParametersIncrementer を使用して新しい JobParameters を生成しました: %+v", params.Params)
		}

		// 新しい JobInstance を作成し永続化します。
		logger.Debugf("JobInstance (JobName: %s, Parameters: %+v) が見つかりませんでした。新しい JobInstance を作成します。", jobName, params)
		jobInstance = core.NewJobInstance(jobName, params) // 新しい JobInstance を作成
		err = l.jobRepository.SaveJobInstance(ctx, jobInstance)
		if err != nil {
			logger.Errorf("新しい JobInstance (ID: %s) の保存に失敗しました: %v", jobInstance.ID, err)
			return nil, exception.NewBatchError("job_launcher", "起動処理エラー: 新しい JobInstance の保存に失敗しました", err, false, false)
		}
		logger.Infof("新しい JobInstance (ID: %s, JobName: %s) を作成し保存しました。", jobInstance.ID, jobInstance.JobName)
	} else {
		logger.Infof("既存の JobInstance (ID: %s, JobName: %s) を使用します。", jobInstance.ID, jobInstance.JobName)
		// TODO: リスタート可能な JobExecution が存在するかチェックするロジックを追加 (フェーズ3)
		//       存在する場合、その JobExecution をロードして再開処理を行う。
	}

	// Step 4: JobExecution の作成 (まだ永続化されていない状態)
	jobExecution := core.NewJobExecution(jobInstance.ID, jobName, params) // JobInstance.ID を追加
	// NewJobExecution 時点では Status は JobStatusStarting です

	// Context with CancelFunc を作成し、JobExecution に設定
	jobCtx, cancel := context.WithCancel(ctx) // ★ 追加
	jobExecution.CancelFunc = cancel          // ★ 追加
	l.RegisterCancelFunc(jobExecution.ID, cancel) // ★ 追加: CancelFunc を登録

	logger.Infof("Job '%s' (Execution ID: %s, Job Instance ID: %s) の起動処理を始めるよ。", jobName, jobExecution.ID, jobInstance.ID)

	// Step 5: JobExecution を JobRepository に保存 (Initial Save)
	err = l.jobRepository.SaveJobExecution(jobCtx, jobExecution) // ★ 変更: jobCtx を渡す
	if err != nil {
		l.UnregisterCancelFunc(jobExecution.ID) // ★ 追加: エラー時は登録解除
		logger.Errorf("JobExecution (ID: %s) の初期永続化に失敗しました: %v", jobExecution.ID, err)
		return jobExecution, exception.NewBatchError("job_launcher", "起動処理エラー: JobExecution の初期保存に失敗しました", err, false, false)
	}
	logger.Debugf("JobExecution (ID: %s) を JobRepository に初期保存しました。", jobExecution.ID)

	// Step 6: JobExecution の状態を Started に更新し、永続化
	jobExecution.MarkAsStarted() // StartTime, LastUpdated, Status を更新

	err = l.jobRepository.UpdateJobExecution(jobCtx, jobExecution) // ★ 変更: jobCtx を渡す
	if err != nil {
		logger.Errorf("JobExecution (ID: %s) の Started 状態への更新に失敗しました: %v", jobExecution.ID, err)
		jobExecution.AddFailureException(exception.NewBatchError("job_launcher", "JobExecution 状態更新エラー (Started)", err, false, false))
	} else {
		logger.Debugf("JobExecution (ID: %s) を JobRepository で Started に更新しました。", jobExecution.ID)
	}

	logger.Infof("Job '%s' (Execution ID: %s, Job Instance ID: %s) を実行するよ。", jobName, jobExecution.ID, jobInstance.ID)

	// Step 7: core.Job の Run メソッドを実行し、JobExecution と JobParameters を渡す
	runErr := batchJob.Run(jobCtx, jobExecution, params) // ★ 変更: jobCtx を渡す

	// Step 8: ジョブ実行完了後の JobExecution の状態を永続化
	updateErr := l.jobRepository.UpdateJobExecution(jobCtx, jobExecution) // ★ 変更: jobCtx を渡す
	if updateErr != nil {
		logger.Errorf("JobExecution (ID: %s) の最終状態の更新に失敗しました: %v", jobExecution.ID, updateErr)
		jobExecution.AddFailureException(exception.NewBatchError("job_launcher", "JobExecution 最終状態更新エラー", updateErr, false, false))
		if runErr == nil {
			runErr = exception.NewBatchError("job_launcher", "JobExecution 最終状態の永続化に失敗しました", updateErr, false, false)
		} else {
			runErr = exception.NewBatchError("job_launcher", fmt.Sprintf("Job実行エラー (%v), 永続化エラー (%v)", runErr, updateErr), runErr, false, false)
		}
	} else {
		logger.Debugf("JobExecution (ID: %s) を JobRepository で最終状態 (%s) に更新しました。", jobExecution.ID, jobExecution.Status)
	}

	l.UnregisterCancelFunc(jobExecution.ID) // ★ 追加: 実行完了後に CancelFunc を登録解除

	return jobExecution, runErr
}
