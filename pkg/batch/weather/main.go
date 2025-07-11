package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "embed"

	// godotenv is now imported in initializer, so it's not needed here.
	// If you need it for other purposes in main, keep it.
	// "github.com/joho/godotenv"

	config "sample/pkg/batch/config"
	initializer "sample/pkg/batch/initializer"
	exception "sample/pkg/batch/util/exception"
	logger "sample/pkg/batch/util/logger"
	core "sample/pkg/batch/job/core"
)

//go:embed resources/application.yaml
var embeddedConfig []byte // application.yaml の内容をバイトスライスとして埋め込む

//go:embed resources/job.yaml
var embeddedJSL []byte // JSL YAML ファイルを埋め込む

func main() {
	// .env ファイルのロード処理は initializer.Initialize に移動されました。
	// ここでは、環境変数 ENV_FILE_PATH から .env ファイルのパスを取得し、
	// initializer に渡します。パスが指定されていない場合はデフォルトの ".env" を使用します。

	// 設定の初期ロード (embeddedConfig を渡すため)
	initialCfg := &config.Config{
		EmbeddedConfig: embeddedConfig, // embeddedConfig を Config 構造体に設定
	}

	// Context の設定 (キャンセル可能にする)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // main 関数終了時にキャンセルを呼び出す

	// シグナルハンドリング (Ctrl+C などで安全に終了するため)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Warnf("シグナル '%v' を受信しました。ジョブの停止を試みます...", sig)
		cancel() // Context をキャンセルしてジョブ実行を中断
	}()

	// BatchInitializer の生成
	batchInitializer := initializer.NewBatchInitializer(initialCfg)
	// JSL定義のバイトスライスを BatchInitializer に設定
	batchInitializer.JSLDefinitionBytes = embeddedJSL

	// Initialize メソッドに .env ファイルのパスを渡す
	// 環境変数 ".ENV_FILE_PATH" またはデフォルトの ".env" を使用
	envFilePath := os.Getenv("ENV_FILE_PATH")
	if envFilePath == "" {
		envFilePath = ".env" // デフォルトのパス
	}

	// バッチアプリケーションの初期化処理を実行
	jobOperator, initErr := batchInitializer.Initialize(ctx, envFilePath)
	if initErr != nil {
		logger.Fatalf("バッチアプリケーションの初期化に失敗しました: %v", exception.NewBatchError("main", "バッチアプリケーションの初期化に失敗しました", initErr, false, false))
	}
	logger.Infof("バッチアプリケーションの初期化が完了しました。")

	// 初期化完了後、リソースのクローズ処理を defer で登録
	defer func() {
		if closeErr := batchInitializer.Close(); closeErr != nil {
			logger.Errorf("バッチアプリケーションのリソースクローズ中にエラーが発生しました: %v", closeErr)
		} else {
			logger.Infof("バッチアプリケーションのリソースを正常にクローズしました。")
		}
	}()

	// 実行するジョブ名を設定ファイルから取得
	jobName := batchInitializer.Config.Batch.JobName
	if jobName == "" {
		logger.Fatalf("設定ファイルにジョブ名が指定されていません。")
	}
	logger.Infof("実行する Job: '%s'", jobName)

	// JobParameters を作成 (必要に応じてパラメータを設定)
	jobParams := config.NewJobParameters() // config.NewJobParameters() を使用
	// 例: ジョブパラメータを追加
	jobParams.Put("input.file", "/path/to/input.csv")
	jobParams.Put("output.dir", "/path/to/output")
	jobParams.Put("process.date", time.Now().Format("2006-01-02"))

	// JobOperator を使用してジョブを起動
	jobExecution, startErr := jobOperator.Start(ctx, jobName, jobParams)

	// Start メソッドがエラーを返した場合のハンドリング
	if startErr != nil {
		if jobExecution != nil {
			logger.Errorf("Job '%s' (Execution ID: %s) の実行中にエラーが発生しました: %v",
				jobName, jobExecution.ID, startErr)

			logger.Errorf("Job '%s' (Execution ID: %s) の最終状態: %s, ExitStatus: %s",
				jobExecution.JobName, jobExecution.ID, jobExecution.Status, jobExecution.ExitStatus)

			if len(jobExecution.Failures) > 0 {
				for i, f := range jobExecution.Failures {
					logger.Errorf("  - 失敗 %d: %v", i+1, f)
				}
			}
		} else {
			logger.Errorf("Job '%s' の起動処理中にエラーが発生しました: %v", jobName, startErr)
		}

		// BatchError の場合は、その情報もログ出力
		if be, ok := startErr.(*exception.BatchError); ok {
			logger.Errorf("BatchError 詳細: Module=%s, Message=%s, OriginalErr=%v", be.Module, be.Message, be.OriginalErr)
			if be.StackTrace != "" {
				logger.Debugf("BatchError StackTrace:\n%s", be.StackTrace)
			}
		}

		os.Exit(1)
	}

	if jobExecution == nil {
		logger.Fatalf("JobOperator.Start がエラーなしで nil の JobExecution を返しました。", exception.NewBatchErrorf("main", "JobOperator.Start がエラーなしで nil の JobExecution を返しました。"))
	}

	logger.Infof("Job '%s' (Execution ID: %s) の最終状態: %s",
		jobExecution.JobName, jobExecution.ID, jobExecution.Status)

	// JobExecution の状態に基づいてアプリケーションの終了コードを制御
	if jobExecution.Status == core.BatchStatusFailed || jobExecution.Status == core.BatchStatusAbandoned {
		logger.Errorf(
			"Job '%s' は失敗しました。詳細は JobExecution (ID: %s) およびログを確認してください。",
			jobExecution.JobName,
			jobExecution.ID,
		)
		if len(jobExecution.Failures) > 0 {
			for i, f := range jobExecution.Failures {
				logger.Errorf("  - 失敗 %d: %v", i+1, f)
			}
		}

		os.Exit(1)
	}

	logger.Infof("アプリケーションを正常に完了しました。Job '%s' (Execution ID: %s) は %s で終了しました。",
		jobExecution.JobName, jobExecution.ID, jobExecution.Status)

	os.Exit(0)
}
