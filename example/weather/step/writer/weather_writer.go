package writer

import (
	"context"
	"database/sql" // sql パッケージをインポート
	"fmt"
	core "sample/pkg/batch/job/core"
	writer "sample/pkg/batch/step/writer" // Writer インターフェースをインポート
	logger "sample/pkg/batch/util/logger"
	"sample/pkg/batch/util/exception" // exception パッケージをインポート

	weather_entity "sample/example/weather/domain/entity"
	appRepo "sample/example/weather/repository" // repository パッケージをインポート (エイリアスを appRepo に変更)
)

// WeatherItemWriter は天気データをデータベースに書き込むためのItemWriter実装です。
type WeatherItemWriter struct {
	repo appRepo.WeatherRepository // repository.WeatherRepository を使用
	// ExecutionContext はWriterの状態を保持するために使用できます
	executionContext core.ExecutionContext
}

// NewWeatherItemWriter は新しいWeatherItemWriterのインスタンスを作成します。
func NewWeatherWriter(repo appRepo.WeatherRepository) *WeatherItemWriter { // appRepo.WeatherRepository を使用
	return &WeatherItemWriter{
		repo:             repo,
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Write は加工済みの天気データアイテムのチャンクをデータベースに保存します。
// 引数を []weather_entity.WeatherDataToStore に変更し、トランザクションを受け取るように変更します。
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

	// items は []any なので、元の型に変換する必要がある
	// Processor が []*weather_entity.WeatherDataToStore を返しているため、
	// items の各要素は []*weather_entity.WeatherDataToStore 型のスライスです。
	// これらを結合し、最終的に []weather_entity.WeatherDataToStore に変換します。
	var dataToStorePointers []*weather_entity.WeatherDataToStore
	for _, item := range items {
		typedItem, ok := item.([]*weather_entity.WeatherDataToStore)
		if !ok {
			// 予期しない入力アイテムの型はスキップ可能、リトライ不可
			return exception.NewBatchError("weather_writer", fmt.Sprintf("予期しない入力アイテムの型です: %T, 期待される型: []*weather_entity.WeatherDataToStore", item), nil, false, true)
		}
		dataToStorePointers = append(dataToStorePointers, typedItem...)
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
	err := w.repo.BulkInsertWeatherData(ctx, tx, finalDataToStore) // finalDataToStore を渡す
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
var _ writer.Writer[any] = (*WeatherItemWriter)(nil) // Writer[any] に変更
