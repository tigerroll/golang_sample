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
}

func NewWeatherWriter(repo repository.WeatherRepository) *WeatherWriter {
  return &WeatherWriter{
    repo: repo,
  }
}

func (w *WeatherWriter) Write(ctx context.Context, items []*entity.WeatherDataToStore) error {
  for _, item := range items {
    forecast := entity.OpenMeteoForecast{
      Latitude:  item.Latitude,
      Longitude: item.Longitude,
      Hourly: entity.Hourly{
        Time:          []string{item.Time.Format(time.RFC3339)}, // time.Time を文字列に変換
        WeatherCode:   []int{item.WeatherCode},
        Temperature2M: []float64{item.Temperature2M},
      },
    }
    if err := w.repo.SaveWeatherData(ctx, forecast); err != nil {
      return fmt.Errorf("データの保存に失敗しました: %w", err)
    }
  }
  return nil
}
