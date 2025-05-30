package processor

import (
  "context"
  "fmt"
  "time"

  entity  "sample/src/main/go/batch/domain/entity"
)

type WeatherProcessor struct {
  // 設定などの依存があれば
  // config *config.WeatherProcessorConfig // 必要に応じて追加
}

// NewWeatherProcessor が引数なし、または WeatherProcessorConfig を受け取るように修正 (ここでは引数なしのまま)
func NewWeatherProcessor(/* cfg *config.WeatherProcessorConfig */) *WeatherProcessor {
  return &WeatherProcessor{
    // 初期化
    // config: cfg, // 必要に応じて初期化
  }
}

// Process メソッドが Processor インターフェースを満たすように修正
func (p *WeatherProcessor) Process(ctx context.Context, item interface{}) (interface{}, error) {
  select {
  case <-ctx.Done():
    return nil, ctx.Err()
  default:
  }

  forecast, ok := item.(*entity.OpenMeteoForecast)
  if !ok {
    return nil, fmt.Errorf("予期しない入力型です: %T", item)
  }

  var dataToStore []*entity.WeatherDataToStore
  // CollectedAt は現在時刻を使用しているため、設定は不要
  collectedAt := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60))

  for i := range forecast.Hourly.Time {
    select {
    case <-ctx.Done():
      return nil, ctx.Err()
    default:
    }

    parsedTime, err := time.Parse("2006-01-02T15:04", forecast.Hourly.Time[i])
    if err != nil {
      parsedTime, err = time.Parse(time.RFC3339, forecast.Hourly.Time[i])
      if err != nil {
        return nil, fmt.Errorf("時間のパースに失敗しました: %w", err)
      }
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

// WeatherProcessor が Processor インターフェースを満たすことを確認
var _ Processor = (*WeatherProcessor)(nil)