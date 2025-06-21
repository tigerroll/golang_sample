// src/main/go/batch/step/writer/weather_item_writer.go
package writer

import (
	"context"
	"fmt"
	"sample/src/main/go/batch/domain/entity"
	core "sample/src/main/go/batch/job/core"
	"sample/src/main/go/batch/repository" // repository パッケージをインポート
	"sample/src/main/go/batch/util/logger"
)

// WeatherRepository は天気データを保存するためのリポジトリインターフェースです。
// このインターフェースは、PostgresRepositoryやMySQLRepositoryが実装することを想定しています。
// 既存のWeatherRepositoryインターフェースがこのシグネチャを持つことを前提とします。
type WeatherRepository interface {
	BulkInsertWeatherData(ctx context.Context, items []entity.WeatherDataToStore) error
	Close() error
}

// WeatherItemWriter は天気データをデータベースに書き込むためのItemWriter実装です。
type WeatherItemWriter struct {
	repo WeatherRepository
	// ExecutionContext はWriterの状態を保持するために使用できます
	executionContext core.ExecutionContext
}

// NewWeatherItemWriter は新しいWeatherItemWriterのインスタンスを作成します。
func NewWeatherItemWriter(repo WeatherRepository) *WeatherItemWriter {
	return &WeatherItemWriter{
		repo:             repo,
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Write は加工済みの天気データアイテムのチャンクをデータベースに保存します。
func (w *WeatherItemWriter) Write(ctx context.Context, items []entity.WeatherDataToStore) error {
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

	err := w.repo.BulkInsertWeatherData(ctx, items)
	if err != nil {
		return fmt.Errorf("天気データのバルク挿入に失敗しました: %w", err)
	}

	logger.Debugf("天気データアイテムのチャンクをデータベースに保存しました。データ数: %d", len(items))
	return nil
}

// Close はリソースを解放します。
func (w *WeatherItemWriter) Close(ctx context.Context) error {
	logger.Debugf("WeatherItemWriterをクローズします。")
	return w.repo.Close()
}

// SetExecutionContext は ExecutionContext を設定します。
func (w *WeatherItemWriter) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
	w.executionContext = ec
	return nil
}

// GetExecutionContext は ExecutionContext を取得します。
func (w *WeatherItemWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	return w.executionContext, nil
}

// Writer インターフェースが実装されていることを確認
var _ Writer[entity.WeatherDataToStore] = (*WeatherItemWriter)(nil)

