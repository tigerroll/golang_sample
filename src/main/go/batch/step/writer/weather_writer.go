package writer

import (
	"context"
	"fmt"
	// "time" // ★ 不要なインポートを削除

	entity "sample/src/main/go/batch/domain/entity"
	core "sample/src/main/go/batch/job/core" // core パッケージをインポート
	repository "sample/src/main/go/batch/repository"
	logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

type WeatherWriter struct {
	repo repository.WeatherRepository
	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewWeatherWriter が Repository を受け取るように修正 (ここでは修正なし)
func NewWeatherWriter(repo repository.WeatherRepository) *WeatherWriter {
	return &WeatherWriter{
		repo: repo,
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Write メソッドが Writer[any] インターフェースを満たすように修正
// このメソッドは、JSLAdaptedStep から渡されたアイテムのチャンクを書き込みます。
func (w *WeatherWriter) Write(ctx context.Context, items []any) error { // ★ シグネチャを []any に変更
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(items) == 0 {
		logger.Debugf("WeatherWriter: 書き込むアイテムがありません。")
		return nil
	}

	// []any から []entity.WeatherDataToStore に変換
	dataToStoreSlice := make([]entity.WeatherDataToStore, 0, len(items))
	for i, item := range items {
		dataToStore, ok := item.(entity.WeatherDataToStore) // ポインタではなく値型で受け取る
		if !ok {
			// エラーをログに記録し、スキップするか、エラーを返すか選択
			logger.Warnf("WeatherWriter: アイテム %d の型が予期せぬものです: %T, 期待される型: entity.WeatherDataToStore。このアイテムはスキップされます。", i, item)
			continue // このアイテムをスキップして次のアイテムへ
		}
		dataToStoreSlice = append(dataToStoreSlice, dataToStore)
	}

	if len(dataToStoreSlice) == 0 {
		logger.Debugf("WeatherWriter: 有効な天気データアイテムがありませんでした。")
		return nil
	}

	logger.Debugf("WeatherWriter: %d 件のアイテムをリポジトリに書き込みます。", len(dataToStoreSlice))

	// リポジトリの BulkInsertWeatherData メソッドを呼び出す
	if err := w.repo.BulkInsertWeatherData(ctx, dataToStoreSlice); err != nil { // ★ BulkInsertWeatherData を呼び出すように修正
		return fmt.Errorf("天気データのバルク保存に失敗しました: %w", err)
	}

	logger.Debugf("WeatherWriter: %d 件のアイテムをリポジトリに書き込み完了しました。", len(dataToStoreSlice))
	return nil
}

// Close は Writer インターフェースの実装です。
// WeatherWriter は閉じるリソースがないため、何もしません。
func (w *WeatherWriter) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	logger.Debugf("WeatherWriter.Close が呼び出されました。")
	return nil
}

// SetExecutionContext は Writer インターフェースの実装です。
// 渡された ExecutionContext を内部に設定します。
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

// GetExecutionContext は Writer インターフェースの実装です。
// 現在の ExecutionContext を返します。
func (w *WeatherWriter) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	logger.Debugf("WeatherWriter.GetExecutionContext が呼び出されました。")
	return w.executionContext, nil
}

// WeatherWriter が Writer[any] インターフェースを満たすことを確認
var _ Writer[any] = (*WeatherWriter)(nil)
