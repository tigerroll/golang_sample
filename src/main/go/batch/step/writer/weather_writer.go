// src/main/go/batch/step/writer/weather_writer.go
package writer

import (
	"context"
	"database/sql" // sql パッケージをインポート
	"fmt"
	"sample/src/main/go/batch/domain/entity"
	core "sample/src/main/go/batch/job/core"
	"sample/src/main/go/batch/repository" // repository パッケージをインポート
	"sample/src/main/go/batch/util/logger"
)

// WeatherWriter は天気データをデータベースに書き込むためのItemWriter実装です。
type WeatherWriter struct {
	repo repository.WeatherRepository // repository.WeatherRepository を使用
	// ExecutionContext はWriterの状態を保持するために使用できます
	executionContext core.ExecutionContext
}

// NewWeatherWriter は新しいWeatherWriterのインスタンスを作成します。
func NewWeatherWriter(repo repository.WeatherRepository) *WeatherWriter { // repository.WeatherRepository を使用
	return &WeatherWriter{
		repo:             repo,
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Write は加工済みの天気データアイテムのチャンクをデータベースに保存します。
// 引数を []any に変更し、トランザクションを受け取るように変更します。
func (w *WeatherWriter) Write(ctx context.Context, tx *sql.Tx, items []any) error { // ★ 修正: tx *sql.Tx を追加
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
	dataToStore := make([]entity.WeatherDataToStore, 0, len(items))
	for i, item := range items {
		// ここで型アサーションを行う
		if singleItem, ok := item.(*entity.WeatherDataToStore); ok {
			dataToStore = append(dataToStore, *singleItem)
		} else {
			// 予期しない型の場合、エラーを返すかログに記録してスキップするか選択
			logger.Errorf("WeatherWriter: 予期しないアイテムの型です: %T, 期待される型: *entity.WeatherDataToStore (インデックス: %d)", item, i)
			return fmt.Errorf("WeatherWriter: 予期しないアイテムの型です: %T", item)
		}
	}

	if len(dataToStore) == 0 {
		logger.Debugf("型変換後、書き込む有効なアイテムがありません。")
		return nil
	}

	// BulkInsertWeatherData にトランザクションを渡す
	err := w.repo.BulkInsertWeatherData(ctx, tx, dataToStore) // ★ 修正: tx を渡す
	if err != nil {
		return fmt.Errorf("天気データのバルク挿入に失敗しました: %w", err)
	}

	logger.Debugf("天気データアイテムのチャンクをデータベースに保存しました。データ数: %d", len(dataToStore))
	return nil
}

// Close はリソースを解放します。
func (w *WeatherWriter) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("WeatherWriterをクローズします。")
	// WeatherWriter はリポジトリを保持しているため、リポジトリの Close を呼び出す
	return w.repo.Close()
}

// SetExecutionContext は ExecutionContext を設定します。
func (w *WeatherWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
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
func (w *WeatherWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("WeatherWriter.GetExecutionContext が呼び出されました。")
	return w.executionContext, nil
}

// Writer インターフェースが実装されていることを確認
var _ Writer[any] = (*WeatherWriter)(nil)
