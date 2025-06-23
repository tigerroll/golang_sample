package listener

import (
	"context"
	"fmt"

	"sample/src/main/go/batch/config"
	core "sample/src/main/go/batch/job/core" // core パッケージをインポート
	"sample/src/main/go/batch/util/logger"
)

type LoggingJobListener struct{
	config *config.LoggingConfig
}

// NewLoggingJobListener が LoggingConfig を受け取るように修正
func NewLoggingJobListener(cfg *config.LoggingConfig) *LoggingJobListener {
	return &LoggingJobListener{
		config: cfg,
	}
}

// BeforeJob メソッドシグネチャを変更し、JobExecution を受け取るようにします。
func (l *LoggingJobListener) BeforeJob(ctx context.Context, jobExecution *core.JobExecution) {
	// JobExecution から必要な情報を取得してログ出力
	logger.Infof("ジョブ実行開始: JobName='%s', JobInstanceID='%s', JobExecutionID='%s', Parameters='%+v'",
		jobExecution.JobName, jobExecution.JobInstanceID, jobExecution.ID, jobExecution.Parameters.Params)
}

// AfterJob メソッドシグネチャを変更し、JobExecution を受け取るようにします。
func (l *LoggingJobListener) AfterJob(ctx context.Context, jobExecution *core.JobExecution) {
	// JobExecution から最終状態やエラー情報を取得してログ出力
	status := jobExecution.Status
	exitStatus := jobExecution.ExitStatus
	duration := jobExecution.EndTime.Sub(jobExecution.StartTime)

	if status == core.BatchStatusFailed { // ★ 修正: core.BatchStatusFailed に変更
		logger.Errorf("ジョブ実行終了 (失敗): JobName='%s', JobExecutionID='%s', Status='%s', ExitStatus='%s', Duration='%.2f秒', Failures='%+v'",
			jobExecution.JobName, jobExecution.ID, status, exitStatus, duration.Seconds(), jobExecution.Failures)
	} else {
		logger.Infof("ジョブ実行終了: JobName='%s', JobExecutionID='%s', Status='%s', ExitStatus='%s', Duration='%.2f秒'", // ★ 修正: ログメッセージの「最終状態」を「最終ステータス」に統一
			jobExecution.JobName, jobExecution.ID, status, exitStatus, duration.Seconds())
	}

	// デバッグレベルで JobExecution の詳細情報を出力
	if l.config.Level == "DEBUG" {
		logger.Debugf("JobExecution 詳細: %+v", jobExecution)
		for _, se := range jobExecution.StepExecutions {
			logger.Debugf("  StepExecution 詳細 (Step: %s): %+v", se.StepName, se)
			logger.Debugf("    StepExecutionContext: %+v", se.ExecutionContext)
		}
		logger.Debugf("JobExecutionContext: %+v", jobExecution.ExecutionContext)
	}
}

// LoggingJobListener が JobExecutionListener インターフェースを満たすことを確認
var _ JobExecutionListener = (*LoggingJobListener)(nil)
