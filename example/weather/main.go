package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	_ "embed"

	"sample/example/weather/app" // 新しい app パッケージをインポート
	"sample/pkg/batch/util/logger" // logger をインポート
)

//go:embed resources/application.yaml
var embeddedConfig []byte // application.yaml の内容をバイトスライスとして埋め込む

//go:embed resources/job.yaml
var embeddedJSL []byte // JSL YAML ファイルを埋め込む

func main() {
	// Context の設定 (キャンセル可能にする)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // main 関数終了時にキャンセルを呼び出す

	// シグナルハンドリング (Ctrl+C などで安全に終了するため)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM) // シグナルを捕捉

	go func() {
		sig := <-sigChan
		logger.Warnf("シグナル '%v' を受信しました。ジョブの停止を試みます...", sig)
		cancel() // Context をキャンセルしてジョブ実行を中断
	}()

	envFilePath := os.Getenv("ENV_FILE_PATH")
	if envFilePath == "" {
		envFilePath = ".env" // デフォルトのパス
	}

	// アプリケーションのメインロジックを app パッケージに委譲
	exitCode := app.RunApplication(ctx, envFilePath, embeddedConfig, embeddedJSL)
	os.Exit(exitCode)
}
