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
// このステップでは、ChunkOrientedStep から渡されたアイテムを書き込みます。
func (w *WeatherWriter) Write(ctx context.Context, items interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// JSLAdaptedStep から渡される items は []interface{} 型なので、まずそれに型アサーション
	rawItems, ok := items.([]interface{})
	if !ok {
		return fmt.Errorf("Writer: 予期しない入力型です: %T, 期待される型: []interface{}", items)
	}

	// []interface{} の各要素を []*entity.WeatherDataToStore に変換
	dataToStore := make([]*entity.WeatherDataToStore, 0, len(rawItems))
	for i, item := range rawItems {
		typedItem, ok := item.(*entity.WeatherDataToStore)
		if !ok {
			return fmt.Errorf("Writer: スライス内の要素の型が予期しません: インデックス %d, 型 %T, 期待される型: *entity.WeatherDataToStore", i, item)
		}
		dataToStore = append(dataToStore, typedItem)
	}

	if len(dataToStore) == 0 {
		logger.Debugf("WeatherWriter: 書き込むデータがありません。")
		return nil // 書き込むデータがない場合は何もしない
	}

	logger.Infof("WeatherWriter: %d 件のアイテムをリポジトリに書き込みます。", len(dataToStore))

	// リポジトリを使用してデータを保存
	// SaveWeatherData は単一の OpenMeteoForecast を受け取るため、アイテムをループして個別に保存します。
	// バッチ保存が必要な場合は Repository のメソッドを変更する必要があります。
	for _, item := range dataToStore {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// WeatherDataToStore から OpenMeteoForecast 形式に変換して保存
		// Repository は OpenMeteoForecast 形式を期待しているため
		forecast := entity.OpenMeteoForecast{
			Latitude:  item.Latitude,
			Longitude: item.Longitude,
			Hourly: entity.Hourly{
				Time:          []string{item.Time.Format(time.RFC3339)}, // time.Time から string に変換
				WeatherCode:   []int{item.WeatherCode},
				Temperature2M: []float64{item.Temperature2M},
			},
		}
		// リポジトリのメソッドに Context を渡す
		if err := w.repo.SaveWeatherData(ctx, forecast); err != nil {
			// 個別アイテムの保存エラーは、Writer エラーとして返す
			return fmt.Errorf("データの保存に失敗しました (アイテム: %+v): %w", item, err)
		}
	}

	logger.Infof("WeatherWriter: %d 件のアイテムをリポジトリに書き込み完了しました。", len(dataToStore))
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
