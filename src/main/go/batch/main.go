package main

import (
	"context"
	"database/sql" // database/sql パッケージをインポート
	"fmt"          // fmt パッケージをインポート
	"os"
	"os/signal"
	"syscall"
	"time" // time パッケージをインポート

	"github.com/joho/godotenv" // .env ファイルを読み込むためにインポート

	config "sample/src/main/go/batch/config"
	job "sample/src/main/go/batch/job" // job パッケージをインポート
	core "sample/src/main/go/batch/job/core"
	factory "sample/src/main/go/batch/job/factory" // factory パッケージをインポート
	repository "sample/src/main/go/batch/repository" // repository パッケージをインポート
	exception "sample/src/main/go/batch/util/exception" // exception パッケージをインポート
	logger "sample/src/main/go/batch/util/logger"

	// JSLでコンポーネントを動的に解決するため、Reader/Processor/Writerのパッケージをインポート
	_ "sample/src/main/go/batch/step/processor" // NewWeatherProcessor が参照されるためインポート
	_ "sample/src/main/go/batch/step/reader"    // NewWeatherReader が参照されるためインポート
	_ "sample/src/main/go/batch/step/writer"    // NewWeatherWriter が参照されるためインポート
	_ "sample/src/main/go/batch/step/processor" // dummy_processor.go がこのパッケージに属する
	_ "sample/src/main/go/batch/step/reader"    // dummy_reader.go がこのパッケージに属する
	_ "sample/src/main/go/batch/step/writer"    // dummy_writer.go がこのパッケージに属する
	_ "sample/src/main/go/batch/step"           // JSLAdaptedStep が参照されるためインポート

	// ★ マイグレーション関連のインポートを追加 ★
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // PostgreSQL ドライバ
	_ "github.com/golang-migrate/migrate/v4/source/file"      // ファイルソース
	_ "github.com/go-sql-driver/mysql"                        // MySQL ドライバ
	_ "github.com/lib/pq"                                     // PostgreSQL/Redshift ドライバ
	_ "github.com/snowflakedb/gosnowflake"                    // Snowflake ドライバ (必要に応じて)
)

// connectWithRetry は指定されたデータベースにリトライ付きで接続を試みます。
func connectWithRetry(ctx context.Context, driverName, dataSourceName string, maxRetries int, delay time.Duration) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < maxRetries; i++ {
		logger.Debugf("データベース接続を試行中 (試行 %d/%d)...", i+1, maxRetries)
		db, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			logger.Warnf("データベース接続のオープンに失敗しました: %v", err)
			time.Sleep(delay)
			continue
		}

		err = db.PingContext(ctx)
		if err == nil {
			logger.Infof("データベース接続に成功しました。")
			return db, nil
		}

		// Pingに失敗した場合、接続を閉じてからリトライ
		db.Close()
		logger.Warnf("データベースへのPingに失敗しました: %v", err)
		time.Sleep(delay)
	}
	return nil, fmt.Errorf("データベースへの接続に最大試行回数 (%d) 失敗しました", maxRetries)
}

func main() {
	// .env ファイルの読み込み (開発環境用)
	if err := godotenv.Load(); err != nil {
		logger.Warnf(".env ファイルのロードに失敗しました (本番環境では環境変数を使用): %v", err)
	}

	// 設定のロード
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("設定のロードに失敗しました: %v", exception.NewBatchError("main", "設定のロードに失敗しました", err, false, false))
	}

	// ロギングレベルの設定
	logger.SetLogLevel(cfg.System.Logging.Level)
	logger.Infof("ロギングレベルを '%s' に設定しました。", cfg.System.Logging.Level)

	// 必要に応じて他の設定値もログ出力
	logger.Debugf("Database Type: %s", cfg.Database.Type)
	logger.Debugf("Batch API Endpoint: %s", cfg.Batch.APIEndpoint)
	logger.Debugf("Batch Job Name: %s", cfg.Batch.JobName)
	logger.Debugf("Batch Chunk Size: %d", cfg.Batch.ChunkSize)
	logger.Debugf("Retry Max Attempts: %d", cfg.Batch.Retry.MaxAttempts)

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

	// データベース接続文字列を構築
	dbURL := cfg.Database.ConnectionString()
	if dbURL == "" {
		logger.Fatalf("データベース接続文字列の構築に失敗しました。")
	}

	// データベースドライバ名の決定
	dbDriverName := ""
	switch cfg.Database.Type {
	case "postgres", "redshift":
		dbDriverName = "postgres"
	case "mysql":
		dbDriverName = "mysql"
	default:
		logger.Fatalf("未対応のデータベースタイプです: %s", cfg.Database.Type)
	}

	// ★ データベースマイグレーションの実行前に、DB接続をリトライ付きで確立 ★
	// マイグレーション用のDB接続を確立 (リトライ付き)
	// 10回リトライ、5秒間隔で最大50秒待機
	dbForMigrate, err := connectWithRetry(ctx, dbDriverName, dbURL, 10, 5*time.Second)
	if err != nil {
		logger.Fatalf("データベースへの接続に失敗しました: %v", exception.NewBatchError("main", "データベースへの接続に失敗しました", err, false, false))
	}
	// マイグレーション用DB接続をmain関数終了時にクローズ
	defer func() {
		if dbForMigrate != nil {
			if err := dbForMigrate.Close(); err != nil {
				logger.Errorf("マイグレーション用データベース接続のクローズに失敗しました: %v", err)
			} else {
				logger.Debugf("マイグレーション用データベース接続を閉じました。")
			}
		}
	}()

	// マイグレーションソースのパス
	// プロジェクトのルートからの相対パスを想定
	migrationsPath := "file://src/main/resources/migrations" // ★ マイグレーションファイルのパスを設定

	m, err := migrate.New(
		migrationsPath,
		dbURL, // golang-migrate/migrate は内部で新しい接続を開くため、dbForMigrate は直接渡さない
	)
	if err != nil {
		// ここで発生するエラーの詳細をログに出力するように変更
		batchErr := exception.NewBatchError("main", "マイグレーションインスタンスの作成に失敗しました", err, false, false)
		logger.Fatalf("マイグレーションインスタンスの作成に失敗しました: %v (Original Error: %v)", batchErr, batchErr.OriginalErr)
	}
	logger.Infof("データベースマイグレーションを開始します...")
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logger.Fatalf("データベースマイグレーションの実行に失敗しました: %v", exception.NewBatchError("main", "データベースマイグレーションの実行に失敗しました", err, false, false))
	}
	logger.Infof("データベースマイグレーションが完了しました。")

	// Step 1: Job Repository の生成
	// マイグレーション後にデータベース接続を確立
	jobRepository, err := repository.NewJobRepository(ctx, *cfg)
	if err != nil {
		logger.Fatalf("Job Repository の生成に失敗しました: %v", exception.NewBatchError("main", "Job Repository の生成に失敗しました", err, false, false))
	}
	// Step 2: アプリケーション終了時に Job Repository をクローズするように defer を設定
	defer func() {
		closeErr := jobRepository.Close()
		if closeErr != nil {
			logger.Errorf("Job Repository のクローズに失敗しました: %v", exception.NewBatchError("main", "Job Repository のクローズに失敗しました", closeErr, false, false))
		} else {
			logger.Infof("Job Repository を正常にクローズしました。")
		}
	}()
	logger.Infof("Job Repository を生成しました。")


	// Step 3: JobFactory の生成
	jobFactory := factory.NewJobFactory(cfg, jobRepository)
	logger.Debugf("JobFactory を Job Repository と共に作成しました。")


	// 実行するジョブ名を指定 (JSLファイルで定義されたID)
	jobName := cfg.Batch.JobName
	logger.Infof("実行する Job: '%s'", jobName)

	// JobParameters を作成 (必要に応じてパラメータを設定)
	jobParams := core.NewJobParameters()
	// 例: ジョブパラメータを追加
	jobParams.Put("input.file", "/path/to/input.csv")
	jobParams.Put("output.dir", "/path/to/output")
	jobParams.Put("process.date", time.Now().Format("2006-01-02"))


	// Step 4: JobOperator を作成し、Job Repository と JobFactory を引き渡す
	jobOperator := job.NewDefaultJobOperator(jobRepository, *jobFactory)
	logger.Debugf("DefaultJobOperator を Job Repository および JobFactory と共に作成しました。")


	// Step 5: JobOperator を使用してジョブを起動
	jobExecution, startErr := jobOperator.Start(ctx, jobName, jobParams)

	// Start メソッドがエラーを返した場合のハンドリングを修正
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
	if jobExecution.Status == core.BatchStatusFailed || jobExecution.Status == core.BatchStatusAbandoned { // ★ 修正: core.BatchStatusFailed と core.BatchStatusAbandoned に変更
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
