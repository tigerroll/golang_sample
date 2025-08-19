package writer // パッケージ名を 'weatherwriter' から 'writer' に変更

import (
	"context"
	_ "database/sql" // sql.Stmt のためにブランクインポート
	"fmt"
	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core"
	logger "sample/pkg/batch/util/logger" // そのまま
	"sample/pkg/batch/util/exception" // exception パッケージをインポート
	batchRepo "sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
	"sample/pkg/batch/database" // database パッケージをインポート
	sql_repo "sample/pkg/batch/repository/sql" // ★ 追加: SQLJobRepository にアクセスするため

	weather_entity "sample/example/weather/domain/entity"
	appRepo "sample/example/weather/repository" // repository パッケージをインポート (エイリアスを appRepo に変更)
)

// WeatherItemWriter は天気データをデータベースに書き込むためのItemWriter実装です。
type WeatherItemWriter struct {
	dbConn database.DBConnection // ★ 追加: データベース接続を保持
	repo appRepo.WeatherRepository // repository.WeatherRepository を使用
	// ExecutionContext はWriterの状態を保持するために使用できます
	executionContext core.ExecutionContext
}

// NewWeatherWriter は新しいWeatherItemWriterのインスタンスを作成します。
// ComponentBuilder のシグネチャに合わせ、cfg, repo, properties を受け取ります。
// データベースリポジリはここで生成します。
func NewWeatherWriter(cfg *config.Config, repo batchRepo.JobRepository, properties map[string]string) (*WeatherItemWriter, error) { // repo の型を job.JobRepository に変更
	// ★ 追加: JobRepository から基盤となる DBConnection を取得
	sqlRepo, ok := repo.(*sql_repo.SQLJobRepository)
	if !ok {
		return nil, exception.NewBatchErrorf("weather_writer", "JobRepository が予期しない型です。SQLJobRepository が必要です。")
	}
	dbConnection := sqlRepo.DBConnection // SQLJobRepository の dbConnection フィールドにアクセス

	_ = properties // 現時点では properties は使用しないが、シグネチャを合わせるために受け取る

	var weatherSpecificRepo appRepo.WeatherRepository

	switch cfg.Database.Type {
	case "postgres", "redshift":
		weatherSpecificRepo = appRepo.NewPostgresWeatherRepository(repo) // ★ 変更: repo を渡す
	case "mysql":
		weatherSpecificRepo = appRepo.NewMySQLWeatherRepository(repo) // ★ 変更: repo を渡す
	default:
		return nil, fmt.Errorf("未対応のデータベースタイプです: %s", cfg.Database.Type)
	}

	return &WeatherItemWriter{
		dbConn:           dbConnection, // ★ 追加: 取得した DBConnection を設定
		repo:             weatherSpecificRepo, // ここで生成したリポジトリを使用
		executionContext: core.NewExecutionContext(), // 初期化
	}, nil
}

// Open は ItemWriter インターフェースの実装です。
// WeatherItemWriter はリソースを開く必要がないため、SetExecutionContext を呼び出すだけです。
func (w *WeatherItemWriter) Open(ctx context.Context, ec core.ExecutionContext) error { // ★ 追加
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("WeatherItemWriter.Open が呼び出されました。")
	err := w.SetExecutionContext(ctx, ec)
	if err != nil {
		return err
	}

	// ★ この行がここにあることを確認
	logger.Debugf("WeatherItemWriter: TRUNCATE TABLE hourly_forecast を実行しようとしています。")
	// ★ 追加: hourly_forecast テーブルをトランケートする
	// トランザクションを開始
	tx, err := w.dbConn.BeginTx(ctx, nil)
	if err != nil {
		return exception.NewBatchError("weather_writer", "トランザクションの開始に失敗しました", err, false, false)
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r) // re-throw panic
		} else if err != nil {
			tx.Rollback()
		}
	}()

	truncateSQL := `TRUNCATE TABLE hourly_forecast RESTART IDENTITY CASCADE;`
	_, err = tx.ExecContext(ctx, truncateSQL)
	if err != nil {
		return exception.NewBatchError("weather_writer", "hourly_forecast テーブルのトランケートに失敗しました", err, false, false)
	}

	// トランケートは独立したトランザクションとしてコミット
	if err = tx.Commit(); err != nil {
		return exception.NewBatchError("weather_writer", "トランケートトランザクションのコミットに失敗しました", err, false, false)
	}

	logger.Infof("hourly_forecast テーブルをトランケートしました。")

	// ExecutionContext から状態を復元する必要がある場合はここにロジックを追加
	// 例: if val, ok := ec["writer_state"]; ok { w.state = val.(string) }
	return nil
}

// Write は加工済みの天気データアイテムのチャンクをデータベースに保存します。
// 引数を []weather_entity.WeatherDataToStore に変更し、トランザクションを受け取るように変更します。
func (w *WeatherItemWriter) Write(ctx context.Context, tx database.Tx, items []any) error { // tx を database.Tx に変更
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return ctx.Err() // Context が完了していたら即座に中断
	default:
	}
	if len(items) == 0 {
		logger.Debugf("書き込むアイテムがありません。")
		return nil
	}

	// items は []any なので、元の型に変換する必要がある
	// Processor が []*weather_entity.WeatherDataToStore を返しているため、
	// items の各要素は []*weather_entity.WeatherDataToStore 型のスライスです。
	// これらを結合し、最終的に []weather_entity.WeatherDataToStore に変換します。
	var dataToStorePointers []*weather_entity.WeatherDataToStore
	for _, item := range items {
		typedItem, ok := item.(*weather_entity.WeatherDataToStore) // ここを修正: item は個々のポインタ型
		if !ok {
			// 予期しない入力アイテムの型はスキップ可能、リトライ不可
			return exception.NewBatchError("weather_writer", fmt.Sprintf("予期しない入力アイテムの型です: %T, 期待される型: *weather_entity.WeatherDataToStore", item), nil, false, true) // エラーメッセージも修正
		}
		dataToStorePointers = append(dataToStorePointers, typedItem) // typedItem は既にポインタなので、そのまま追加
	}

	// []*weather_entity.WeatherDataToStore を []weather_entity.WeatherDataToStore に変換
	finalDataToStore := make([]weather_entity.WeatherDataToStore, 0, len(dataToStorePointers))
	for _, ptrItem := range dataToStorePointers {
		if ptrItem != nil {
			finalDataToStore = append(finalDataToStore, *ptrItem) // ポインタをデリファレンスして値を追加
		}
	}

	if len(finalDataToStore) == 0 {
		logger.Debugf("型変換後、書き込む有効なアイテムがありません。")
		return nil
	}

	// BulkInsertWeatherData にトランザクションを渡す
	err := w.repo.BulkInsertWeatherData(ctx, tx, finalDataToStore) // tx を database.Tx に変更
	if err != nil {
		// バルク挿入失敗はリトライ可能、スキップ不可 (チャンク全体が対象のため)
		return exception.NewBatchError("weather_writer", "天気データのバルク挿入に失敗しました", err, true, false)
	}

	logger.Debugf("天気データアイテムのチャンクをデータベースに保存しました。データ数: %d", len(finalDataToStore))
	return nil
}

// Close はリソースを解放します。
func (w *WeatherItemWriter) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("WeatherWriterをクローズします。")
	// WeatherItemWriter はリポジトリを保持しているため、リポジトリの Close を呼び出す
	return w.repo.Close()
}

// SetExecutionContext は ExecutionContext を設定します。
func (w *WeatherItemWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	w.executionContext = ec
	logger.Debugf("WeatherWriter.SetExecutionContext が呼び出されました。")
	return nil
}

// GetExecutionContext は ExecutionContext を取得します。
func (w *WeatherItemWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("WeatherWriter.GetExecutionContext が呼び出されました。")
	return w.executionContext, nil
}

// Writer インターフェースが実装されていることを確認
var _ core.ItemWriter[any] = (*WeatherItemWriter)(nil) // writer.ItemWriter[any] に変更
