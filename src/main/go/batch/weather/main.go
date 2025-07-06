package main // アプリケーションのエントリポイントなので main パッケージのまま

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "embed"

	"github.com/joho/godotenv"

	config "sample/src/main/go/batch/config"
	batch_joboperator "sample/src/main/go/batch/job/joboperator"
	core "sample/src/main/go/batch/job/core"
	factory "sample/src/main/go/batch/job/factory"
	jsl "sample/src/main/go/batch/job/jsl"
	jobListener "sample/src/main/go/batch/job/listener"
	repository "sample/src/main/go/batch/repository"
	exception "sample/src/main/go/batch/util/exception"
	logger "sample/src/main/go/batch/util/logger"

	// JSLでコンポーネントを動的に解決するため、Reader/Processor/Writerのパッケージをインポート
	_ "sample/src/main/go/batch/weather/step/processor"
	_ "sample/src/main/go/batch/weather/step/reader"
	_ "sample/src/main/go/batch/weather/step/writer"
	_ "sample/src/main/go/batch/step/processor"
	dummyProcessor "sample/src/main/go/batch/step/processor"
	_ "sample/src/main/go/batch/step/reader"
	dummyReader "sample/src/main/go/batch/step/reader"
	_ "sample/src/main/go/batch/step/writer"
	dummyWriter "sample/src/main/go/batch/step/writer"
	_ "sample/src/main/go/batch/step"

	// マイグレーション関連のインポート
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/snowflakedb/gosnowflake"

	// weather 関連のパッケージをインポート (JobFactory への登録用)
	weather_config "sample/src/main/go/batch/weather/config"
	weather_repo "sample/src/main/go/batch/weather/repository"
	weather_processor "sample/src/main/go/batch/weather/step/processor"
	weather_reader "sample/src/main/go/batch/weather/step/reader"
	weather_writer "sample/src/main/go/batch/weather/step/writer"
	weather_job "sample/src/main/go/batch/weather/job"
	executionContextReader "sample/src/main/go/batch/step/reader"
	executionContextWriter "sample/src/main/go/batch/step/writer"
	dummyTasklet "sample/src/main/go/batch/step"
)

//go:embed resources/application.yaml
var embeddedConfig []byte // application.yaml の内容をバイトスライスとして埋め込む (main.go と resources は同じディレクトリ階層にあるため、相対パスで指定)

//go:embed resources/job.yaml
var embeddedJSL []byte // JSL YAML ファイルを埋め込む (単一の job.yaml ファイルを対象)

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

// applyMigrations はマイグレーションを実行するヘルパー関数
func applyMigrations(databaseURL string, migrationPath string, dbDriverName string) error {
	if migrationPath == "" {
		logger.Infof("マイグレーションパスが指定されていません。スキップします。")
		return nil
	}

	// アプリケーション側のマイグレーションはデフォルトの履歴テーブル (schema_migrations) を使用するため、
	// x-migrations-table パラメータは追加しません。
	var migrateURL string
	switch dbDriverName {
	case "postgres":
		migrateURL = databaseURL // postgres://... の形式を想定。デフォルトの schema_migrations を使用。
	case "mysql":
		migrateURL = fmt.Sprintf("mysql://%s", databaseURL) // mysql://user:pass@tcp(host:port)/db の形式を想定。デフォルトの schema_migrations を使用。
	default:
		return fmt.Errorf("未対応のデータベースドライバです: %s", dbDriverName)
	}

	logger.Infof("マイグレーションを実行中: パス '%s'", migrationPath)
	m, err := migrate.New(
		fmt.Sprintf("file://%s", migrationPath),
		migrateURL, // x-migrations-table を含まない URL を使用
	)
	if err != nil {
		return exception.NewBatchError("migration", fmt.Sprintf("マイグレーションインスタンスの作成に失敗しました: %s", migrationPath), err, false, false)
	}

	// 開発環境向け: 既存のマイグレーションを一度ダウンさせてからアップする (テーブルを再作成するため)
	// 本番環境ではこのロジックは使用しないでください。
	if os.Getenv("APP_ENV") == "development" {
		if err := m.Down(); err != nil && err != migrate.ErrNoChange {
			logger.Warnf("既存のマイグレーションのダウンに失敗しました (開発環境のみ): %v", err)
		} else if err == nil {
			logger.Debugf("既存のマイグレーションをダウンしました: %s", migrationPath)
		}
	}

	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		return exception.NewBatchError("migration", fmt.Sprintf("マイグレーションの適用に失敗しました: %s", migrationPath), err, false, false)
	}

	if err == migrate.ErrNoChange {
		logger.Infof("マイグレーションは不要です: %s", migrationPath)
	} else {
		logger.Infof("マイグレーションが正常に完了しました: %s", migrationPath)
	}
	return nil
}

func main() {
	// .env ファイルの読み込み (開発環境用)
	if err := godotenv.Load(); err != nil {
		logger.Warnf(".env ファイルのロードに失敗しました (本番環境では環境変数を使用): %v", err)
	}

	// 設定のロード
	// 汎用的な BytesConfigLoader を使用して埋め込み設定をロード
	bytesLoader := config.NewBytesConfigLoader(embeddedConfig)
	cfg, err := bytesLoader.Load()
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

	// データベース接続文字列を構築 (sql.Open 用の DSN)
	dbDSN := cfg.Database.ConnectionString()
	if dbDSN == "" {
		logger.Fatalf("データベース接続文字列の構築に失敗しました。")
	}

	// データベースドライバ名の決定と migrate 用 URL の構築
	dbDriverName := ""
	switch cfg.Database.Type {
	case "postgres", "redshift":
		dbDriverName = "postgres"
	case "mysql":
		dbDriverName = "mysql"
	default:
		logger.Fatalf("未対応のデータベースタイプです: %s", cfg.Database.Type)
	}

	// データベースマイグレーションの実行前に、DB接続をリトライ付きで確立
	// マイグレーション用のDB接続を確立 (リトライ付き)
	// 10回リトライ、5秒間隔で最大50秒待機
	dbForMigrate, err := connectWithRetry(ctx, dbDriverName, dbDSN, 10, 5*time.Second)
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

	// アプリケーション側のマイグレーションを実行 (設定されていれば)
	if cfg.Database.AppMigrationPath != "" {
		if err := applyMigrations(dbDSN, cfg.Database.AppMigrationPath, dbDriverName); err != nil {
			logger.Fatalf("アプリケーションのマイグレーションに失敗しました: %v", err)
		}
	}

	// Step 1: Job Repository の生成
	// JobRepository は独自の接続を開き、その中でバッチフレームワークのマイグレーションが実行されます。
	logger.Debugf("Job Repository 用DB接続文字列: %s", cfg.Database.ConnectionString())
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

	// JobRepository から基盤となる *sql.DB 接続を取得
	// この接続は JobFactory の ComponentBuilder に渡される
	sqlJobRepo, ok := jobRepository.(*repository.SQLJobRepository)
	if !ok {
		logger.Fatalf("JobRepository の実装が予期された型ではありません。*sql.DB 接続を取得できません。")
	}
	dbConnectionForComponents := sqlJobRepo.GetDB()
	if dbConnectionForComponents == nil {
		logger.Fatalf("JobRepository からデータベース接続を取得できませんでした。")
	}

	// Job Repository 生成後の 'job_instances' テーブルの存在確認
	logger.Infof("マイグレーション後の 'job_instances' テーブルの存在を確認します...")
	var exists int
	// JobRepository が持つDB接続を使用して確認
	err = dbConnectionForComponents.QueryRowContext(ctx, "SELECT 1 FROM job_instances LIMIT 1;").Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Warnf("'job_instances' テーブルは存在しますが、データがありません。")
		} else {
			logger.Fatalf("'job_instances' テーブルの存在確認に失敗しました: %v", exception.NewBatchError("main", "'job_instances' テーブルの存在確認に失敗しました", err, false, false))
		}
	} else {
		logger.Infof("'job_instances' テーブルが正常に存在することを確認しました。")
	}

	// JSL 定義のロード (JobFactory から移動)
	if err := jsl.LoadJSLDefinitionFromBytes(embeddedJSL); err != nil {
		logger.Fatalf("JSL 定義のロードに失敗しました: %v", err)
	}
	logger.Infof("JSL 定義のロードが完了しました。ロードされたジョブ数: %d", jsl.GetLoadedJobCount())


	// Step 3: JobFactory の生成
	jobFactory := factory.NewJobFactory(cfg, jobRepository)
	logger.Debugf("JobFactory を Job Repository と共に作成しました。")

	// ここからが新しい登録ロジック
	// コンポーネントビルダーの登録
	jobFactory.RegisterComponentBuilder("weatherReader", func(cfg *config.Config, db *sql.DB) (any, error) {
		weatherReaderCfg := &weather_config.WeatherReaderConfig{
			APIEndpoint: cfg.Batch.APIEndpoint,
			APIKey:      cfg.Batch.APIKey,
		}
		return weather_reader.NewWeatherReader(weatherReaderCfg), nil
	})
	jobFactory.RegisterComponentBuilder("weatherProcessor", func(cfg *config.Config, db *sql.DB) (any, error) {
		return weather_processor.NewWeatherProcessor(), nil
	})
	jobFactory.RegisterComponentBuilder("weatherWriter", func(cfg *config.Config, db *sql.DB) (any, error) {
		// weatherWriter は weather_repo.WeatherRepository を必要とするため、ここで作成し、db を渡す
		var weatherSpecificRepo weather_repo.WeatherRepository
		switch cfg.Database.Type {
		case "postgres", "redshift":
			weatherSpecificRepo = weather_repo.NewPostgresWeatherRepository(dbConnectionForComponents)
		case "mysql":
			weatherSpecificRepo = weather_repo.NewMySQLWeatherRepository(dbConnectionForComponents)
		default:
			return nil, fmt.Errorf("未対応のデータベースタイプです: %s", cfg.Database.Type)
		}
		// NewPostgresWeatherRepository/NewMySQLWeatherRepository はエラーを返さないため、errチェックは不要

		return weather_writer.NewWeatherWriter(weatherSpecificRepo), nil
	})
	jobFactory.RegisterComponentBuilder("dummyReader", func(cfg *config.Config, db *sql.DB) (any, error) {
		return dummyReader.NewDummyReader(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyProcessor", func(cfg *config.Config, db *sql.DB) (any, error) {
		return dummyProcessor.NewDummyProcessor(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyWriter", func(cfg *config.Config, db *sql.DB) (any, error) {
		return dummyWriter.NewDummyWriter(), nil
	})
	jobFactory.RegisterComponentBuilder("executionContextReader", func(cfg *config.Config, db *sql.DB) (any, error) {
		return executionContextReader.NewExecutionContextReader(), nil
	})
	jobFactory.RegisterComponentBuilder("executionContextWriter", func(cfg *config.Config, db *sql.DB) (any, error) {
		return executionContextWriter.NewExecutionContextWriter(), nil
	})
	jobFactory.RegisterComponentBuilder("dummyTasklet", func(cfg *config.Config, db *sql.DB) (any, error) {
		return dummyTasklet.NewDummyTasklet(), nil
	})

	// ジョブビルダーの登録
	jobFactory.RegisterJobBuilder("weather", func(
		jobRepository repository.JobRepository,
		cfg *config.Config,
		listeners []jobListener.JobExecutionListener,
		flow *core.FlowDefinition,
	) (core.Job, error) {
		return weather_job.NewWeatherJob(jobRepository, cfg, listeners, flow), nil
	})
	// ここまでが新しい登録ロジック


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
	jobOperator := batch_joboperator.NewDefaultJobOperator(jobRepository, *jobFactory)
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
