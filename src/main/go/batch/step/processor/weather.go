package processor

import (
  "context"
  "fmt"
  "sample/src/main/go/batch/domain/entity"
  "time"
)

type WeatherProcessor struct {
  // 設定などの依存があれば
}

func NewWeatherProcessor(/* 依存 */) *WeatherProcessor {
  return &WeatherProcessor{
    // 初期化
  }
}

func (p *WeatherProcessor) Process(ctx context.Context, forecast *entity.OpenMeteoForecast) ([]*entity.WeatherDataToStore, error) {
  var dataToStore []*entity.WeatherDataToStore
  collectedAt := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60)) // 収集時刻を日本時間に設定

  for i := range forecast.Hourly.Time {
    parsedTime, err := time.Parse("2006-01-02T15:04", forecast.Hourly.Time[i])
    if err != nil {
      return nil, fmt.Errorf("時間のパースに失敗しました: %w", err)
    }
    data := &entity.WeatherDataToStore{
      Time:        parsedTime,
      WeatherCode: forecast.Hourly.WeatherCode[i],
      Temperature2M: forecast.Hourly.Temperature2M[i],
      Latitude:    forecast.Latitude,
      Longitude:   forecast.Longitude,
      CollectedAt: collectedAt,
    }
    dataToStore = append(dataToStore, data)
  }

  return dataToStore, nil
}
