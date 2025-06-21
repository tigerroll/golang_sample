package writer

import (
	"context"
	"fmt"
	"time"

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

// Write メソッドが Writer インターフェースを満たすように修正
// このステップでは、JSLAdaptedStep から渡された単一のアイテムを書き込みます。
func (w *WeatherWriter) Write(ctx context.Context, item interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 渡されたアイテムを []*entity.WeatherDataToStore に型アサーション
	dataToStore, ok := item.(*entity.WeatherDataToStore)
	if !ok {
		return fmt.Errorf("WeatherWriter: 予期しない入力型です: %T, 期待される型: *entity.WeatherDataToStore", item)
	}

	if dataToStore == nil {
		logger.Debugf("WeatherWriter: 書き込むデータが nil です。")
		return nil // 書き込むデータが nil の場合は何もしない
	}

	logger.Debugf("WeatherWriter: 1 件のアイテムをリポジトリに書き込みます。")

	// WeatherDataToStore から OpenMeteoForecast 形式に変換して保存
	// Repository は OpenMeteoForecast 形式を期待しているため
	forecast := entity.OpenMeteoForecast{
		Latitude:  dataToStore.Latitude,
		Longitude: dataToStore.Longitude,
		Hourly: entity.Hourly{
			Time:          []string{dataToStore.Time.Format(time.RFC3339)}, // time.Time から string に変換
			WeatherCode:   []int{dataToStore.WeatherCode},
			Temperature2M: []float64{dataToStore.Temperature2M},
		},
	}
	// リポジトリのメソッドに Context を渡す
	if err := w.repo.SaveWeatherData(ctx, forecast); err != nil {
		// 個別アイテムの保存エラーは、Writer エラーとして返す
		return fmt.Errorf("データの保存に失敗しました (アイテム: %+v): %w", dataToStore, err)
	}

	logger.Debugf("WeatherWriter: 1 件のアイテムをリポジトリに書き込み完了しました。")
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

// WeatherWriter が Writer インターフェースを満たすことを確認
var _ Writer = (*WeatherWriter)(nil)
