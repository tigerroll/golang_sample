package writer

import (
  "context"
  "fmt"
  "time"

  entity      "sample/src/main/go/batch/domain/entity"
  repository  "sample/src/main/go/batch/repository"
)

type WeatherWriter struct {
  repo repository.WeatherRepository
  // config *config.WeatherWriterConfig // 必要に応じて追加
}

// NewWeatherWriter が Repository を受け取るように修正 (ここでは修正なし)
func NewWeatherWriter(repo repository.WeatherRepository) *WeatherWriter {
  return &WeatherWriter{
    repo: repo,
    // config: cfg, // 必要に応じて初期化
  }
}

// Write メソッドが Writer インターフェースを満たすように修正
func (w *WeatherWriter) Write(ctx context.Context, items interface{}) error {
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }

  dataToStore, ok := items.([]*entity.WeatherDataToStore)
  if !ok {
    return fmt.Errorf("予期しない入力型です: %T", items)
  }

  for _, item := range dataToStore {
    select {
    case <-ctx.Done():
      return ctx.Err()
    default:
    }

    forecast := entity.OpenMeteoForecast{
      Latitude:  item.Latitude,
      Longitude: item.Longitude,
      Hourly: entity.Hourly{
        Time:          []string{item.Time.Format(time.RFC3339)},
        WeatherCode:   []int{item.WeatherCode},
        Temperature2M: []float64{item.Temperature2M},
      },
    }
    // リポジトリのメソッドに Context を渡す (Repository は既に Config を受け取っている)
    if err := w.repo.SaveWeatherData(ctx, forecast); err != nil {
      return fmt.Errorf("データの保存に失敗しました: %w", err)
    }
  }
  return nil
}

// WeatherWriter が Writer インターフェースを満たすことを確認
var _ Writer = (*WeatherWriter)(nil)