// (修正 - JobOperator メソッド実装)
package job

import (
  "context"
  "fmt"
  // "time" // time パッケージは現在使用されていないため削除

  core "sample/src/main/go/batch/job/core"
  factory "sample/src/main/go/batch/job/factory" // JobFactory を使用するために factory パッケージをインポート
  "sample/src/main/go/batch/repository"
  logger "sample/src/main/go/batch/util/logger"
)

// DefaultJobOperator は JobOperator インターフェースのデフォルト実装です。
// JobRepository を使用してバッチメタデータを管理し、ジョブの実行を調整します。
type DefaultJobOperator struct {
  jobRepository repository.JobRepository
  jobFactory    factory.JobFactory // JobFactory を依存として追加 (ジョブオブジェクト生成のため)
}

// DefaultJobOperator が JobOperator インターフェースを満たすことを確認します。
var _ JobOperator = (*DefaultJobOperator)(nil)

// NewDefaultJobOperator は新しい DefaultJobOperator のインスタンスを作成します。
// JobRepository と JobFactory の実装を受け取ります。
func NewDefaultJobOperator(jobRepository repository.JobRepository, jobFactory factory.JobFactory) *DefaultJobOperator {
  return &DefaultJobOperator{
    jobRepository: jobRepository,
    jobFactory:    jobFactory, // JobFactory を初期化
  }
}

// Start は指定されたジョブを JobParameters とともに起動します。
// JobOperator インターフェースの実装です。
// SimpleJobLauncher.Launch のロジックをここに移行します。
func (o *DefaultJobOperator) Start(ctx context.Context, jobName string, params core.JobParameters) (*core.JobExecution, error) {
  logger.Infof("JobOperator を使用して Job '%s' を起動します。", jobName)

  // Step 1: JobInstance の取得または作成
  // ジョブ名とパラメータに一致する既存の JobInstance を検索します。
  jobInstance, err := o.jobRepository.FindJobInstanceByJobNameAndParameters(ctx, jobName, params)
  if err != nil {
    // 検索エラーが発生した場合
    logger.Errorf("JobInstance (JobName: %s, Parameters: %+v) の検索に失敗しました: %v", jobName, params, err)
    return nil, fmt.Errorf("起動処理エラー: JobInstance の検索に失敗しました: %w", err)
  }

  if jobInstance == nil {
    // 既存の JobInstance が見つからない場合、新しい JobInstance を作成し永続化します。
    logger.Debugf("JobInstance (JobName: %s, Parameters: %+v) が見つかりませんでした。新しい JobInstance を作成します。", jobName, params)
    jobInstance = core.NewJobInstance(jobName, params) // 新しい JobInstance を作成
    err = o.jobRepository.SaveJobInstance(ctx, jobInstance)
    if err != nil {
      // JobInstance の保存に失敗した場合
      logger.Errorf("新しい JobInstance (ID: %s) の保存に失敗しました: %v", jobInstance.ID, err)
      return nil, fmt.Errorf("起動処理エラー: 新しい JobInstance の保存に失敗しました: %w", err)
    }
    logger.Infof("新しい JobInstance (ID: %s, JobName: %s) を作成し保存しました。", jobInstance.ID, jobInstance.JobName)
  } else {
    logger.Infof("既存の JobInstance (ID: %s, JobName: %s) を使用します。", jobInstance.ID, jobInstance.JobName)
    // TODO: リスタート可能な JobExecution が存在するかチェックするロジックを追加 (フェーズ3)
    //       存在する場合、その JobExecution をロードして再開処理を行う。
    //       リスタート処理は JobOperator の Restart メソッドで行うべき責務。
    //       ここではシンプルに、既存の JobInstance が見つかった場合でも新しい JobExecution を作成して実行を開始します。
    //       厳密な JSR352 では、同じ JobInstance で同時に複数の JobExecution が実行されることは通常ありません。
    //       既に実行中の JobExecution がある場合はエラーとするなどの制御が必要です。
  }


  // Step 2: JobExecution の作成 (まだ永続化されていない状態)
  // JobInstance の ID を渡して JobExecution を作成します。
  jobExecution := core.NewJobExecution(jobInstance.ID, jobName, params) // JobInstance.ID を追加
  // NewJobExecution 時点では Status は JobStatusStarting です

  logger.Infof("Job '%s' (Execution ID: %s, Job Instance ID: %s) の実行処理を開始します。", jobName, jobExecution.ID, jobInstance.ID)


  // Step 3: JobExecution を JobRepository に保存 (Initial Save)
  // JobExecution の作成直後に永続化します。ステータスは STARTING です。
  err = o.jobRepository.SaveJobExecution(ctx, jobExecution)
  if err != nil {
    // 保存に失敗した場合は、ジョブ実行を開始せずにエラーを返します。
    logger.Errorf("JobExecution (ID: %s) の初期永続化に失敗しました: %v", jobExecution.ID, err)
    return jobExecution, fmt.Errorf("起動処理エラー: JobExecution の初期保存に失敗しました: %w", err)
  }
  logger.Debugf("JobExecution (ID: %s) を JobRepository に初期保存しました。", jobExecution.ID)


  // Step 4: JobExecution の状態を Started に更新し、永続化
  jobExecution.MarkAsStarted() // StartTime, LastUpdated, Status を更新

  err = o.jobRepository.UpdateJobExecution(ctx, jobExecution)
  if err != nil {
    // 更新に失敗した場合、ジョブ実行を開始したものの、状態を正しく記録できなかったことになります。
    // これは深刻な問題ですが、ジョブ自体は実行を開始したとみなします。
    // エラーを記録し、ジョブ実行自体は進めますが、最終的な JobExecution を返す際にエラー情報を含めるべきです。
    logger.Errorf("JobExecution (ID: %s) の Started 状態への更新に失敗しました: %v", jobExecution.ID, err)
    // JobExecution に永続化エラーを追加することも検討
    jobExecution.AddFailureException(fmt.Errorf("JobExecution 状態更新エラー (Started): %w", err))
    // エラーはログ出力に留め、ジョブの Run 処理に進みます。
  } else {
    logger.Debugf("JobExecution (ID: %s) を JobRepository で Started に更新しました。", jobExecution.ID, jobExecution.Status)
  }

  // Step 5: JobFactory を使用して Job オブジェクトを取得
  batchJob, err := o.jobFactory.CreateJob(jobName)
  if err != nil {
    // Job オブジェクトの作成に失敗した場合
    logger.Errorf("Job '%s' の作成に失敗しました: %v", jobName, err)
    // JobExecution を FAILED としてマークし、永続化
    jobExecution.MarkAsFailed(fmt.Errorf("Job オブジェクトの作成に失敗しました: %w", err))
    // エラー発生時の JobExecution の最終状態を JobRepository で更新
    updateErr := o.jobRepository.UpdateJobExecution(ctx, jobExecution)
    if updateErr != nil {
      logger.Errorf("JobExecution (ID: %s) の最終状態更新に失敗しました (Job作成エラー後): %v", jobExecution.ID, updateErr)
      jobExecution.AddFailureException(fmt.Errorf("JobExecution 最終状態更新エラー (Job作成エラー後): %w", updateErr))
    }
    return jobExecution, fmt.Errorf("Job '%s' の作成に失敗しました: %w", jobName, err)
  }


  logger.Infof("Job '%s' (Execution ID: %s, Job Instance ID: %s) を実行します。", jobName, jobExecution.ID, jobInstance.ID)

  // Step 6: core.Job の Run メソッドを実行し、JobExecution を渡す
  // Run メソッド内で JobExecution の最終状態が設定されることを期待
  // Run メソッドはジョブ自体の実行エラーを返します
  runErr := batchJob.Run(ctx, jobExecution)

  // Step 7: ジョブ実行完了後の JobExecution の状態を永続化 (defer で行うべきだが、ここでは明示的に呼び出し)
  // Run メソッド内で JobExecution の最終状態 (Completed or Failed) は既に設定されています。
  // ここではその最終状態を JobRepository に保存します。
  // Note: Job.Run メソッドの defer で JobExecution の最終状態永続化が行われているため、
  // ここでの UpdateJobExecution は冗長になる可能性があります。
  // JobOperator の Start メソッドの責務として JobExecution の最終状態永続化を行う場合は、
  // Job.Run メソッドから最終状態永続化の defer を削除する必要があります。
  // JSR352ではJobOperatorが最終的なJobExecutionの状態を管理することが多いです。
  // ここでは、Job.Run の defer を残しつつ、JobOperator でも念のため更新を試みる形とします。
  // より厳密には、Job.Run はエラーのみを返し、JobOperator がそのエラーと JobExecution の状態を見て最終的な永続化を行うべきです。
  updateErr := o.jobRepository.UpdateJobExecution(ctx, jobExecution)
  if updateErr != nil {
    // 最終状態の永続化に失敗した場合
    logger.Errorf("JobExecution (ID: %s) の最終状態の更新に失敗しました: %v", jobExecution.ID, updateErr)
    // このエラーを JobExecution に追加
    jobExecution.AddFailureException(fmt.Errorf("JobExecution 最終状態更新エラー: %w", updateErr))
    // もし Run メソッドが成功していたとしても、永続化エラーがあれば JobOperator レベルではエラーとみなす
    if runErr == nil {
      runErr = fmt.Errorf("JobExecution 最終状態の永続化に失敗しました: %w", updateErr)
    } else {
      // Run エラーと永続化エラーをラップすることも検討
      runErr = fmt.Errorf("Job実行エラー (%w), 永続化エラー (%w)", runErr, updateErr)
    }
  } else {
    logger.Debugf("JobExecution (ID: %s) を JobRepository で最終状態 (%s) に更新しました。", jobExecution.ID, jobExecution.Status)
  }

  // JobOperator の Start メソッドは、ジョブ自体の実行エラー (runErr) と最終状態の永続化エラーを含めて返します。
  // JobExecution オブジェクトは常に返します。
  return jobExecution, runErr
}

// Restart は指定された JobExecution を再開します。
// JobOperator インターフェースの実装スタブです。
func (o *DefaultJobOperator) Restart(ctx context.Context, executionID string) (*core.JobExecution, error) {
  logger.Infof("JobOperator: Restart メソッドが呼び出されました。Execution ID: %s", executionID)
  // TODO: JobRepository から JobExecution をロードし、再開処理を実装 (フェーズ3)
  return nil, fmt.Errorf("Restart メソッドはまだ実装されていません")
}

// Stop は指定された JobExecution を停止します。
// JobOperator インターフェースの実装スタブです。
func (o *DefaultJobOperator) Stop(ctx context.Context, executionID string) error {
  logger.Infof("JobOperator: Stop メソッドが呼び出されました。Execution ID: %s", executionID)
  // TODO: 実行中のジョブに停止を通知するロジックを実装
  return fmt.Errorf("Stop メソッドはまだ実装されていません")
}

// Abandon は指定された JobExecution を放棄します。
// JobOperator インターフェースの実装スタブです。
func (o *DefaultJobOperator) Abandon(ctx context.Context, executionID string) error {
  logger.Infof("JobOperator: Abandon メソッドが呼び出されました。Execution ID: %s", executionID)
  // TODO: JobExecution を放棄状態に更新するロジックを実装
  return fmt.Errorf("Abandon メソッドはまだ実装されていません")
}

// GetJobExecution は指定された ID の JobExecution を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobExecution(ctx context.Context, executionID string) (*core.JobExecution, error) {
  logger.Infof("JobOperator: GetJobExecution メソッドが呼び出されました。Execution ID: %s", executionID)
  // JobRepository の FindJobExecutionByID を呼び出す
  jobExecution, err := o.jobRepository.FindJobExecutionByID(ctx, executionID)
  if err != nil {
    // JobRepository からのエラーをそのまま返す
    return nil, fmt.Errorf("JobExecution (ID: %s) の取得に失敗しました: %w", executionID, err)
  }
  logger.Debugf("JobExecution (ID: %s) を JobRepository から取得しました。", executionID)
  return jobExecution, nil
}

// GetJobExecutions は指定された JobInstance に関連する全ての JobExecution を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobExecutions(ctx context.Context, instanceID string) ([]*core.JobExecution, error) {
  logger.Infof("JobOperator: GetJobExecutions メソッドが呼び出されました。Instance ID: %s", instanceID)

  // まず JobInstance を取得
  jobInstance, err := o.jobRepository.FindJobInstanceByID(ctx, instanceID)
  if err != nil {
    // JobInstance が見つからない場合や取得エラーの場合
    return nil, fmt.Errorf("JobInstance (ID: %s) の取得に失敗しました: %w", instanceID, err)
  }
  if jobInstance == nil {
    // JobInstance が見つからなかった場合
    logger.Warnf("JobInstance (ID: %s) が見つかりませんでした。", instanceID)
    return []*core.JobExecution{}, nil // 空のスライスを返す
  }

  // JobInstance に関連する全ての JobExecution を取得
  jobExecutions, err := o.jobRepository.FindJobExecutionsByJobInstance(ctx, jobInstance)
  if err != nil {
    // JobRepository からのエラーをそのまま返す
    return nil, fmt.Errorf("JobInstance (ID: %s) に関連する JobExecution の取得に失敗しました: %w", instanceID, err)
  }

  logger.Debugf("JobInstance (ID: %s) に関連する %d 件の JobExecution を取得しました。", instanceID, len(jobExecutions))
  return jobExecutions, nil
}

// GetLastJobExecution は指定された JobInstance の最新の JobExecution を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetLastJobExecution(ctx context.Context, instanceID string) (*core.JobExecution, error) {
  logger.Infof("JobOperator: GetLastJobExecution メソッドが呼び出されました。Instance ID: %s", instanceID)

  // JobRepository の FindLatestJobExecution を呼び出す
  jobExecution, err := o.jobRepository.FindLatestJobExecution(ctx, instanceID)
  if err != nil {
    // JobRepository からのエラーをそのまま返す
    // JobExecution が見つからない場合も JobRepository が nil, sql.ErrNoRows を返す想定
    return nil, fmt.Errorf("JobInstance (ID: %s) の最新 JobExecution の取得に失敗しました: %w", instanceID, err)
  }
  // JobExecution が見つからなかった場合は nil が返される

  if jobExecution != nil {
    logger.Debugf("JobInstance (ID: %s) の最新 JobExecution (ID: %s) を JobRepository から取得しました。", instanceID, jobExecution.ID)
  } else {
    logger.Warnf("JobInstance (ID: %s) の最新 JobExecution が見つかりませんでした。", instanceID)
  }

  return jobExecution, nil
}

// GetJobInstance は指定された ID の JobInstance を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobInstance(ctx context.Context, instanceID string) (*core.JobInstance, error) {
  logger.Infof("JobOperator: GetJobInstance メソッドが呼び出されました。Instance ID: %s", instanceID)
  // JobRepository の FindJobInstanceByID を呼び出す
  jobInstance, err := o.jobRepository.FindJobInstanceByID(ctx, instanceID)
  if err != nil {
    // JobRepository からのエラーをそのまま返す
    // JobInstance が見つからない場合も JobRepository が nil, sql.ErrNoRows を返す想定
    return nil, fmt.Errorf("JobInstance (ID: %s) の取得に失敗しました: %w", instanceID, err)
  }
  // JobInstance が見つからなかった場合は nil が返される

  if jobInstance != nil {
    logger.Debugf("JobInstance (ID: %s) を JobRepository から取得しました。", instanceID)
  } else {
    logger.Warnf("JobInstance (ID: %s) が見つかりませんでした。", instanceID)
  }

  return jobInstance, nil
}

// GetJobInstances は指定されたジョブ名とパラメータに一致する JobInstance を検索します。
// JobOperator インターフェースの実装です。
// JSR352では複数返す場合があるため、リストを返します。
func (o *DefaultJobOperator) GetJobInstances(ctx context.Context, jobName string, params core.JobParameters) ([]*core.JobInstance, error) {
  logger.Infof("JobOperator: GetJobInstances メソッドが呼び出されました。Job Name: %s, Parameters: %+v", jobName, params)
  // JobRepository の FindJobInstanceByJobNameAndParameters は単一の JobInstance を返すため、
  // このメソッドのインターフェース定義 (複数返す) とは少し異なります。
  // JSR352 の GetJobInstances は、指定された JobName と JobParameters に一致する JobInstance のリストを返します。
  // SQLJobRepository の FindJobInstanceByJobNameAndParameters は JobParameters で一意に JobInstance を検索する想定の実装になっています。
  // ここでは、既存の FindJobInstanceByJobNameAndParameters を呼び出し、結果が nil でなければリストに入れて返すようにします。
  // TODO: 厳密な JSR352 に合わせる場合は、JobRepository に JobName と JobParameters で複数検索するメソッドを追加する必要があります。

  jobInstance, err := o.jobRepository.FindJobInstanceByJobNameAndParameters(ctx, jobName, params)
  if err != nil {
    // JobRepository からのエラーをそのまま返す
    return nil, fmt.Errorf("JobInstance (JobName: %s, Parameters: %+v) の検索に失敗しました: %w", jobName, params, err)
  }

  var jobInstances []*core.JobInstance
  if jobInstance != nil {
    jobInstances = append(jobInstances, jobInstance)
    logger.Debugf("JobName '%s' と Parameters に一致する JobInstance (ID: %s) を取得しました。", jobName, jobInstance.ID)
  } else {
    logger.Warnf("JobName '%s' と Parameters に一致する JobInstance は見つかりませんでした。", jobName)
    jobInstances = []*core.JobInstance{} // 見つからない場合は空のスライスを返す
  }


  return jobInstances, nil
}

// GetJobNames は登録されている全てのジョブ名を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetJobNames(ctx context.Context) ([]string, error) {
  logger.Infof("JobOperator: GetJobNames メソッドが呼び出されました。")
  // JobRepository の GetJobNames を呼び出す
  jobNames, err := o.jobRepository.GetJobNames(ctx)
  if err != nil {
    // JobRepository からのエラーをそのまま返す
    return nil, fmt.Errorf("登録されているジョブ名の取得に失敗しました: %w", err)
  }
  logger.Debugf("%d 件のジョブ名を取得しました。", len(jobNames))
  return jobNames, nil
}

// GetParameters は指定された JobExecution の JobParameters を取得します。
// JobOperator インターフェースの実装です。
func (o *DefaultJobOperator) GetParameters(ctx context.Context, executionID string) (core.JobParameters, error) {
  logger.Infof("JobOperator: GetParameters メソッドが呼び出されました。Execution ID: %s", executionID)

  // まず JobExecution を取得
  jobExecution, err := o.jobRepository.FindJobExecutionByID(ctx, executionID)
  if err != nil {
    // JobExecution が見つからない場合や取得エラーの場合
    return core.NewJobParameters(), fmt.Errorf("JobExecution (ID: %s) の取得に失敗しました: %w", executionID, err)
  }
  if jobExecution == nil {
    // JobExecution が見つからなかった場合
    logger.Warnf("JobExecution (ID: %s) が見つかりませんでした。", executionID)
    return core.NewJobParameters(), fmt.Errorf("JobExecution (ID: %s) が見つかりませんでした", executionID) // エラーとして返す
  }

  logger.Debugf("JobExecution (ID: %s) の JobParameters を取得しました。", executionID)
  return jobExecution.Parameters, nil
}
