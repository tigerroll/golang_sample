package processor

import (
  "context" // context パッケージをインポート
  "fmt"
  "time"

  entity  "sample/src/main/go/batch/domain/entity"
)

type WeatherProcessor struct {
  // 設定などの依存があれば
}

func NewWeatherProcessor(/* 依存 */) *WeatherProcessor {
  return &WeatherProcessor{
    // 初期化
  }
}

// Process メソッドに ctx context.Context を追加
func (p *WeatherProcessor) Process(ctx context.Context, forecast *entity.OpenMeteoForecast) ([]*entity.WeatherDataToStore, error) {
  var dataToStore []*entity.WeatherDataToStore
  // Context の完了をチェックしながら処理を進める例
  select {
  case <-ctx.Done():
    // Context が完了した場合、処理を中断してエラーを返す
    return nil, ctx.Err()
  default:
    // Context が完了していない場合は処理を続行
  }

  // 収集時刻を日本時間に設定
  // Context にタイムゾーン情報が含まれている場合はそちらを優先することも検討
  collectedAt := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60))

  for i := range forecast.Hourly.Time {
    // ループ内でも Context の完了を定期的にチェックすることが望ましい
    select {
    case <-ctx.Done():
      return nil, ctx.Err()
    default:
    }

    parsedTime, err := time.Parse("2006-01-02T15:04", forecast.Hourly.Time[i])
    if err != nil {
      // RFC3339 形式も試すなど、必要に応じてパースロジックを強化
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
