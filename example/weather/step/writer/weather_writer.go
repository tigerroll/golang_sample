package writer

import (
	"context"
	"database/sql" // sql パッケージをインポート
	"fmt"
	core "sample/pkg/batch/job/core"
	writer "sample/pkg/batch/step/writer" // Writer インターフェースをインポート
	logger "sample/pkg/batch/util/logger"

	weather_entity "sample/example/weather/domain/entity"
	weather_repo "sample/example/weather/repository" // repository パッケージをインポート
)

// WeatherItemWriter は天気データをデータベースに書き込むためのItemWriter実装です。
type WeatherItemWriter struct {
	repo weather_repo.WeatherRepository // repository.WeatherRepository を使用
	// ExecutionContext はWriterの状態を保持するために使用できます
	executionContext core.ExecutionContext
}

// NewWeatherItemWriter は新しいWeatherItemWriterのインスタンスを作成します。
func NewWeatherWriter(repo weather_repo.WeatherRepository) *WeatherItemWriter { // weather_repo.WeatherRepository を使用
	return &WeatherItemWriter{
		repo:             repo,
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Write は加工済みの天気データアイテムのチャンクをデータベースに保存します。
// 引数を []any に変更し、トランザクションを受け取るように変更します。
func (w *WeatherItemWriter) Write(ctx context.Context, tx *sql.Tx, items []any) error {
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

	// []any を []entity.WeatherDataToStore に変換
	dataToStore := make([]weather_entity.WeatherDataToStore, 0, len(items))
	for i, item := range items {
		// ここで型アサーションを行う
		if singleItem, ok := item.(*weather_entity.WeatherDataToStore); ok {
			dataToStore = append(dataToStore, *singleItem)
		} else {
			// 予期しない型の場合、エラーを返すかログに記録してスキップするか選択
			logger.Errorf("WeatherWriter: 予期しないアイテムの型です: %T, 期待される型: *weather_entity.WeatherDataToStore (インデックス: %d)", item, i)
			return fmt.Errorf("WeatherWriter: 予期しないアイテムの型です: %T", item)
		}
	}

	if len(dataToStore) == 0 {
		logger.Debugf("型変換後、書き込む有効なアイテムがありません。")
		return nil
	}

	// BulkInsertWeatherData にトランザクションを渡す
	err := w.repo.BulkInsertWeatherData(ctx, tx, dataToStore)
	if err != nil {
		return fmt.Errorf("天気データのバルク挿入に失敗しました: %w", err)
	}

	logger.Debugf("天気データアイテムのチャンクをデータベースに保存しました。データ数: %d", len(dataToStore))
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
var _ writer.Writer[any] = (*WeatherItemWriter)(nil)
