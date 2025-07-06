package main // アプリケーションのエントリポイントなので main パッケージのまま

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "embed"

	"github.com/joho/godotenv"

	config "sample/src/main/go/batch/config"
	initializer "sample/src/main/go/batch/initializer"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"
	core "sample/src/main/go/batch/job/core" // <-- Add this import
)

//go:embed resources/application.yaml
var embeddedConfig []byte // application.yaml の内容をバイトスライスとして埋め込む

//go:embed resources/job.yaml
var embeddedJSL []byte // JSL YAML ファイルを埋め込む

func main() {
	// .env ファイルの読み込み (開発環境用)
	if err := godotenv.Load(); err != nil {
		logger.Warnf(".env ファイルのロードに失敗しました (本番環境では環境変数を使用): %v", err)
	}

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

	// BatchInitializer の生成と初期化処理の実行
	batchInitializer := initializer.NewBatchInitializer(initialCfg)
	// JSL定義のバイトスライスを BatchInitializer に設定
	batchInitializer.JSLDefinitionBytes = embeddedJSL

	jobOperator, initErr := batchInitializer.Initialize(ctx)
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
