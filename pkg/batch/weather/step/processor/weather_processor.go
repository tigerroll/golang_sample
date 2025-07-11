package weather_processor

import (
	"context"
	"fmt"
	"time"

	weather_entity "sample/pkg/batch/weather/domain/entity" // weather_entity パッケージをインポート
	processor "sample/pkg/batch/step/processor" // Processor インターフェースをインポート
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

// Process メソッドが Processor[*entity.OpenMeteoForecast, []*entity.WeatherDataToStore] インターフェースを満たすように修正
func (p *WeatherProcessor) Process(ctx context.Context, item any) (any, error) { // I は any, O は any
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	forecast, ok := item.(*weather_entity.OpenMeteoForecast)
	if !ok {
		return nil, fmt.Errorf("WeatherProcessor: 予期しない入力型です: %T, 期待される型: *weather_entity.OpenMeteoForecast", item)
	}
 
	var dataToStore []*weather_entity.WeatherDataToStore
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
				return nil, fmt.Errorf("時間のパースに失敗しました: %w", forecast.Hourly.Time[i], err) // エラーメッセージに元の時刻文字列を追加
			}
		}
		data := &weather_entity.WeatherDataToStore{
			Time:          parsedTime,
			WeatherCode:   forecast.Hourly.WeatherCode[i],
			Temperature2M: forecast.Hourly.Temperature2M[i],
			Latitude:      forecast.Latitude,
			Longitude:     forecast.Longitude,
			CollectedAt:   collectedAt,
		}
		dataToStore = append(dataToStore, data)
	}

	// ここで []*entity.WeatherDataToStore を any 型として返します。
	return any(dataToStore), nil // ★ any に明示的にキャスト
}

// WeatherProcessor が Processor[*entity.OpenMeteoForecast, []*entity.WeatherDataToStore] インターフェースを満たすことを確認
var _ processor.Processor[any, any] = (*WeatherProcessor)(nil)
