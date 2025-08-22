package joboperator

import (
	"context"
	"fmt"
	"time" // time パッケージは現在使用されていないため削除

	core "sample/pkg/batch/job/core" // core パッケージをインポート
	factory "sample/pkg/batch/job/factory" // JobFactory を使用するために factory パッケージをインポート
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	exception "sample/pkg/batch/util/exception" // exception パッケージをインポート
	logger "sample/pkg/batch/util/logger"
)

// DefaultJobOperator は JobOperator インターフェースのデフォルト実装です。
// JobRepository を使用してバッチメタデータを管理し、ジョブの実行を調整します。
type DefaultJobOperator struct {
	jobRepository job.JobRepository // job.JobRepository に変更
	jobFactory    factory.JobFactory // JobFactory を依存として追加 (ジョブオブジェクト生成のため)
}

// DefaultJobOperator が JobOperator インターフェースを満たすことを確認します。
var _ JobOperator = (*DefaultJobOperator)(nil)

// NewDefaultJobOperator は新しい DefaultJobOperator のインスタンスを作成します。
// JobRepository と JobFactory の実装を受け取ります。
func NewDefaultJobOperator(jobRepository job.JobRepository, jobFactory factory.JobFactory) *DefaultJobOperator { // job.JobRepository を受け取る
	return &DefaultJobOperator{
		jobRepository: jobRepository,
		jobFactory:    jobFactory, // JobFactory を初期化
	}
}

// Restart は指定された JobExecution を再開します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) Restart(ctx context.Context, executionID string) (*core.JobExecution, error) {
	logger.Infof("JobOperator: Restart メソッドが呼び出されたよ。Execution ID: %s", executionID)

	// 1. 前回の JobExecution をロード
	prevJobExecution, err := o.jobRepository.FindJobExecutionByID(ctx, executionID)
	if err != nil {
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("再起動処理エラー: JobExecution (ID: %s) のロードに失敗しました", executionID), err, false, false)
	}
	if prevJobExecution == nil {
		return nil, exception.NewBatchErrorf("job_operator", "再起動処理エラー: JobExecution (ID: %s) が見つかりませんでした", executionID)
	}

	// 2. 再起動可能状態かチェック
	// JSR352 では FAILED または STOPPED 状態の JobExecution のみ再起動可能です。
	if prevJobExecution.Status != core.BatchStatusFailed && prevJobExecution.Status != core.BatchStatusStopped { // ★ 修正: core.BatchStatusFailed と core.BatchStatusStopped を使用
		return nil, exception.NewBatchErrorf("job_operator", "再起動処理エラー: JobExecution (ID: %s) は再起動可能な状態じゃないよ (現在の状態: %s)", executionID, prevJobExecution.Status)
	}
	logger.Infof("JobExecution (ID: %s) は再起動可能な状態 (%s) だよ。", executionID, prevJobExecution.Status)

	// 3. JobInstance をロード
	jobInstance, err := o.jobRepository.FindJobInstanceByID(ctx, prevJobExecution.JobInstanceID)
	if err != nil {
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("再起動処理エラー: JobInstance (ID: %s) のロードに失敗しました", prevJobExecution.JobInstanceID), err, false, false)
	}
	if jobInstance == nil {
		return nil, exception.NewBatchErrorf("job_operator", "再起動処理エラー: JobInstance (ID: %s) が見つかりませんでした", prevJobExecution.JobInstanceID)
	}

	// 4. 新しい JobExecution を作成 (同じ JobInstance に紐づく)
	// JobParameters は前回の JobExecution から引き継ぎます。
	newJobExecution := core.NewJobExecution(jobInstance.ID, prevJobExecution.JobName, prevJobExecution.Parameters)

	// 5. 前回の ExecutionContext を新しい JobExecution に引き継ぐ
	// ExecutionContext はジョブの状態を保持するため、再起動時に引き継ぐ必要があります。
	newJobExecution.ExecutionContext = prevJobExecution.ExecutionContext.Copy()
	logger.Debugf("JobExecution (ID: %s) の ExecutionContext を新しい JobExecution (ID: %s) に引き継いだよ。", prevJobExecution.ID, newJobExecution.ID)

	// 6. 再開するステップ名を決定
	// 失敗または停止したステップから再開します。
	newJobExecution.CurrentStepName = prevJobExecution.CurrentStepName
	if newJobExecution.CurrentStepName == "" {
		// もし CurrentStepName が空の場合（例: ジョブ開始直後に失敗）、フローの開始要素から再開
		// このケースは通常、JobFactory で Job を作成する際にフローの StartElement を取得して設定すべきだが、念のため。
		// TODO: JobFactory.CreateJob で Job を取得した後、その Job の GetFlow().StartElement を取得して設定するロジックを追加検討
		//       ここではシンプルに、JobExecution に CurrentStepName が設定されていることを前提とする。
		logger.Warnf("JobExecution (ID: %s) の CurrentStepName が空だよ。フローの開始要素から再開を試みるよ。", prevJobExecution.ID)
		// この時点では Job オブジェクトがないため、正確な StartElement は不明。
		// Job.Run メソッドがこの空文字列を適切に処理することを期待する。
	}
	logger.Infof("新しい JobExecution (ID: %s) はステップ '%s' から再開するよ。", newJobExecution.ID, newJobExecution.CurrentStepName)

	// 7. 新しい JobExecution を JobRepository に保存 (Initial Save)
	err = o.jobRepository.SaveJobExecution(ctx, newJobExecution)
	if err != nil {
		logger.Errorf("再起動処理エラー: 新しい JobExecution (ID: %s) の初期永続化に失敗しました: %v", newJobExecution.ID, err)
		return newJobExecution, exception.NewBatchError("job_operator", fmt.Sprintf("再起動処理エラー: 新しい JobExecution (ID: %s) の初期保存に失敗しました", newJobExecution.ID), err, false, false)
	}
	logger.Debugf("新しい JobExecution (ID: %s) を JobRepository に初期保存したよ。", newJobExecution.ID)

	// 8. 新しい JobExecution の状態を Started に更新し、永続化
	newJobExecution.MarkAsStarted()
	err = o.jobRepository.UpdateJobExecution(ctx, newJobExecution)
	if err != nil {
		logger.Errorf("再起動処理エラー: 新しい JobExecution (ID: %s) の Started 状態への更新に失敗しました: %v", newJobExecution.ID, err)
		newJobExecution.AddFailureException(exception.NewBatchError("job_operator", "JobExecution 状態更新エラー (Started)", err, false, false))
	} else {
		logger.Debugf("新しい JobExecution (ID: %s) を JobRepository で Started に更新したよ。", newJobExecution.ID)
	}

	// 9. JobFactory を使用して Job オブジェクトを取得
	batchJob, err := o.jobFactory.CreateJob(newJobExecution.JobName)
	if err != nil {
		logger.Errorf("再起動処理エラー: Job '%s' の作成に失敗しました: %v", newJobExecution.JobName, err)
		newJobExecution.MarkAsFailed(exception.NewBatchError("job_operator", "Job オブジェクトの作成に失敗しました", err, false, false))
		updateErr := o.jobRepository.UpdateJobExecution(ctx, newJobExecution)
		if updateErr != nil {
			logger.Errorf("JobExecution (ID: %s) の最終状態更新に失敗しました (Job作成エラー後): %v", newJobExecution.ID, updateErr)
			newJobExecution.AddFailureException(exception.NewBatchError("job_operator", "JobExecution 最終状態更新エラー (Job作成エラー後)", updateErr, false, false))
		}
		return newJobExecution, exception.NewBatchError("job_operator", fmt.Sprintf("Job '%s' の作成に失敗しました", newJobExecution.JobName), err, false, false)
	}

	logger.Infof("Job '%s' (Execution ID: %s, Job Instance ID: %s) の再実行を始めるよ。", newJobExecution.JobName, newJobExecution.ID, jobInstance.ID)

	// 10. core.Job の Run メソッドを実行し、新しい JobExecution と JobParameters を渡す
	runErr := batchJob.Run(ctx, newJobExecution, newJobExecution.Parameters) // ★ 修正: newJobExecution.Parameters を追加

	// 11. ジョブ実行完了後の JobExecution の状態を永続化
	updateErr := o.jobRepository.UpdateJobExecution(ctx, newJobExecution)
	if updateErr != nil {
		logger.Errorf("JobExecution (ID: %s) の最終状態の更新に失敗しました: %v", newJobExecution.ID, updateErr)
		newJobExecution.AddFailureException(exception.NewBatchError("job_operator", "JobExecution 最終状態更新エラー", updateErr, false, false))
		if runErr == nil {
			runErr = exception.NewBatchError("job_operator", fmt.Sprintf("Job実行エラー (%v), 永続化エラー (%v)", runErr, updateErr), runErr, false, false)
		} else {
			runErr = exception.NewBatchError("job_operator", fmt.Sprintf("Job実行エラー (%v), 永続化エラー (%v)", runErr, updateErr), runErr, false, false)
		}
	} else {
		logger.Debugf("JobExecution (ID: %s) を JobRepository で最終状態 (%s) に更新したよ。", newJobExecution.ID, newJobExecution.Status)
	}

	return newJobExecution, runErr
}

// Stop は指定された JobExecution を停止します。
// JobOperator インターフェースの実装スタブです。
func (o *DefaultJobOperator) Stop(ctx context.Context, executionID string) error {
	logger.Infof("JobOperator: Stop メソッドが呼び出されたよ。Execution ID: %s", executionID)
	// TODO: 実行中のジョブに停止を通知するロジックを実装
	return exception.NewBatchErrorf("job_operator", "Stop メソッドはまだ実装されていません")
}

// Abandon は指定された JobExecution を放棄します。
// JobOperator インターフェースの実装スタブです。
func (o *DefaultJobOperator) Abandon(ctx context.Context, executionID string) error {
	logger.Infof("JobOperator: Abandon メソッドが呼び出されたよ。Execution ID: %s", executionID)

	// 1. JobExecution をロード
	jobExecution, err := o.jobRepository.FindJobExecutionByID(ctx, executionID)
	if err != nil {
		return exception.NewBatchError("job_operator", fmt.Sprintf("放棄処理エラー: JobExecution (ID: %s) のロードに失敗しました", executionID), err, false, false)
	}
	if jobExecution == nil {
		return exception.NewBatchErrorf("job_operator", "放棄処理エラー: JobExecution (ID: %s) が見つかりませんでした", executionID)
	}

	// 2. JobExecution の状態を ABANDONED に更新
	// 既に終了状態の場合は更新しない (JSR-352の仕様に準拠)
	if jobExecution.Status.IsFinished() {
		logger.Warnf("JobExecution (ID: %s) は既に終了状態 (%s) なので放棄できません。", executionID, jobExecution.Status)
		return exception.NewBatchErrorf("job_operator", "放棄処理エラー: JobExecution (ID: %s) は既に終了状態です (%s)", executionID, jobExecution.Status)
	}

	jobExecution.Status = core.BatchStatusAbandoned
	jobExecution.ExitStatus = core.ExitStatusAbandoned // ExitStatus も ABANDONED に設定
	jobExecution.EndTime = time.Now() // 終了時刻を設定
	jobExecution.LastUpdated = time.Now()
	logger.Infof("JobExecution (ID: %s) の状態を ABANDONED に更新したよ。", executionID)

	// 3. JobExecution を永続化
	err = o.jobRepository.UpdateJobExecution(ctx, jobExecution)
	if err != nil {
		return exception.NewBatchError("job_operator", fmt.Sprintf("放棄処理エラー: JobExecution (ID: %s) の状態更新に失敗しました", executionID), err, false, false)
	}

	logger.Infof("JobExecution (ID: %s) を正常に放棄したよ。", executionID)
	return nil
}

// GetJobExecution は指定された ID の JobExecution を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobExecution(ctx context.Context, executionID string) (*core.JobExecution, error) {
	logger.Infof("JobOperator: GetJobExecution メソッドが呼び出されたよ。Execution ID: %s", executionID)
	// JobRepository の FindJobExecutionByID を呼び出す
	jobExecution, err := o.jobRepository.FindJobExecutionByID(ctx, executionID)
	if err != nil {
		// JobRepository からのエラーをそのまま返す
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("JobExecution (ID: %s) の取得に失敗しました", executionID), err, false, false)
	}
	logger.Debugf("JobExecution (ID: %s) を JobRepository から取得したよ。", executionID)
	return jobExecution, nil
}

// GetJobExecutions は指定された JobInstance に関連する全ての JobExecution を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobExecutions(ctx context.Context, instanceID string) ([]*core.JobExecution, error) {
	logger.Infof("JobOperator: GetJobExecutions メソッドが呼び出されたよ。Instance ID: %s", instanceID)

	// まず JobInstance を取得
	jobInstance, err := o.jobRepository.FindJobInstanceByID(ctx, instanceID)
	if err != nil {
		// JobInstance が見つからない場合や取得エラーの場合
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("JobInstance (ID: %s) の取得に失敗しました", instanceID), err, false, false)
	}
	if jobInstance == nil {
		// JobInstance が見つからなかった場合
		logger.Warnf("JobInstance (ID: %s) が見つからなかったよ。", instanceID)
		return []*core.JobExecution{}, nil // 空のスライスを返す
	}

	// JobInstance に関連する全ての JobExecution を取得
	jobExecutions, err := o.jobRepository.FindJobExecutionsByJobInstance(ctx, jobInstance)
	if err != nil {
		// JobRepository からのエラーをそのまま返す
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("JobInstance (ID: %s) に関連する JobExecution の取得に失敗しました", instanceID), err, false, false)
	}

	logger.Debugf("JobInstance (ID: %s) に関連する %d 件の JobExecution を取得したよ。", instanceID, len(jobExecutions))
	return jobExecutions, nil
}

// GetLastJobExecution は指定された JobInstance の最新の JobExecution を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetLastJobExecution(ctx context.Context, instanceID string) (*core.JobExecution, error) {
	logger.Infof("JobOperator: GetLastJobExecution メソッドが呼び出されたよ。Instance ID: %s", instanceID)

	// JobRepository の FindLatestJobExecution を呼び出す
	jobExecution, err := o.jobRepository.FindLatestJobExecution(ctx, instanceID)
	if err != nil {
		// JobRepository からのエラーをそのまま返す
		// JobExecution が見つからない場合も JobRepository が nil, sql.ErrNoRows を返す想定
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("JobInstance (ID: %s) の最新 JobExecution の取得に失敗しました", instanceID), err, false, false)
	}
	// JobExecution が見つからなかった場合は nil が返される

	if jobExecution != nil {
		logger.Debugf("JobInstance (ID: %s) の最新 JobExecution (ID: %s) を JobRepository から取得したよ。", instanceID, jobExecution.ID)
	} else {
		logger.Warnf("JobInstance (ID: %s) が見つからなかったよ。", instanceID)
	}

	return jobExecution, nil
}

// GetJobInstance は指定された ID の JobInstance を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobInstance(ctx context.Context, instanceID string) (*core.JobInstance, error) {
	logger.Infof("JobOperator: GetJobInstance メソッドが呼び出されたよ。Instance ID: %s", instanceID)
	// JobRepository の FindJobInstanceByID を呼び出す
	jobInstance, err := o.jobRepository.FindJobInstanceByID(ctx, instanceID)
	if err != nil {
		// JobRepository からのエラーをそのまま返す
		// JobInstance が見つからない場合も JobRepository が nil, sql.ErrNoRows を返す想定
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("JobInstance (ID: %s) の取得に失敗しました", instanceID), err, false, false)
	}
	// JobInstance が見つからなかった場合は nil が返される

	if jobInstance != nil {
		logger.Debugf("JobInstance (ID: %s) を JobRepository から取得したよ。", instanceID)
	} else {
		logger.Warnf("JobInstance (ID: %s) が見つからなかったよ。", instanceID)
	}

	return jobInstance, nil
}

// GetJobInstances は指定されたジョブ名とパラメータに一致する JobInstance を検索します。
// JobOperator インターフェースの実装です。
// JSR352では複数返す場合があるため、リストを返します。
func (o *DefaultJobOperator) GetJobInstances(ctx context.Context, jobName string, params core.JobParameters) ([]*core.JobInstance, error) {
	logger.Infof("JobOperator: GetJobInstances メソッドが呼び出されたよ。Job Name: %s, Parameters: %+v", jobName, params)
	// JobRepository の FindJobInstanceByJobNameAndParameters は単一の JobInstance を返すため、
	// このメソッドのインターフェース定義 (複数返す) とは少し異なります。
	// JSR352 の GetJobInstances は、指定された JobName と JobParameters に一致する JobInstance のリストを返します。
	// SQLJobRepository の FindJobInstanceByJobNameAndParameters は JobParameters で一意に JobInstance を検索する想定の実装になっています。
	// ここでは、既存の FindJobInstanceByJobNameAndParameters を呼び出し、結果が nil でなければリストに入れて返すようにします。
	// TODO: 厳密な JSR352 に合わせる場合は、JobRepository に JobName と JobParameters で複数検索するメソッドを追加する必要があります。

	jobInstance, err := o.jobRepository.FindJobInstanceByJobNameAndParameters(ctx, jobName, params)
	if err != nil {
		// JobRepository からのエラーをそのまま返す
		return nil, exception.NewBatchError("job_operator", fmt.Sprintf("JobInstance (JobName: %s, Parameters: %+v) の検索に失敗しました", jobName, params), err, false, false)
	}

	var jobInstances []*core.JobInstance
	if jobInstance != nil {
		jobInstances = append(jobInstances, jobInstance)
		logger.Debugf("JobName '%s' と Parameters に一致する JobInstance (ID: %s) を取得したよ。", jobName, jobInstance.ID)
	} else {
		logger.Warnf("JobName '%s' と Parameters に一致する JobInstance は見つからなかったよ。", jobName)
		jobInstances = []*core.JobInstance{} // 見つからない場合は空のスライスを返す
	}

	return jobInstances, nil
}

// GetJobNames は登録されている全てのジョブ名を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobNames(ctx context.Context) ([]string, error) {
	logger.Infof("JobOperator: GetJobNames メソッドが呼び出されたよ。")
	// JobRepository の GetJobNames を呼び出す
	jobNames, err := o.jobRepository.GetJobNames(ctx)
	if err != nil {
		// JobRepository からのエラーをそのまま返す
		return nil, exception.NewBatchError("job_operator", "登録されているジョブ名の取得に失敗しました", err, false, false)
	}
	logger.Debugf("%d 件のジョブ名を取得したよ。", len(jobNames))
	return jobNames, nil
}

// GetParameters は指定された JobExecution の JobParameters を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetParameters(ctx context.Context, executionID string) (core.JobParameters, error) {
	logger.Infof("JobOperator: GetParameters メソッドが呼び出されたよ。Execution ID: %s", executionID)

	// まず JobExecution を取得
	jobExecution, err := o.jobRepository.FindJobExecutionByID(ctx, executionID)
	if err != nil {
		// JobExecution が見つからない場合や取得エラーの場合
		return core.NewJobParameters(), exception.NewBatchError("job_operator", fmt.Sprintf("JobExecution (ID: %s) の取得に失敗しました", executionID), err, false, false)
	}
	if jobExecution == nil {
		// JobExecution が見つからなかった場合
		logger.Warnf("JobExecution (ID: %s) が見つからなかったよ。", executionID)
		return core.NewJobParameters(), exception.NewBatchErrorf("job_operator", "JobExecution (ID: %s) が見つかりませんでした", executionID) // エラーとして返す
	}

	logger.Debugf("JobExecution (ID: %s) の JobParameters を取得したよ。", executionID)
	return jobExecution.Parameters, nil
}
