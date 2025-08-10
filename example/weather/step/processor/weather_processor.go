package weatherprocessor // パッケージ名を 'weatherprocessor' に変更

import (
	"context"
	"database/sql" // Add sql import for *sql.DB
	"fmt"
	"time"

	config "sample/pkg/batch/config" // config パッケージをインポート
	itemprocessor "sample/pkg/batch/step/processor" // Renamed import
	"sample/pkg/batch/util/exception" // exception パッケージをインポート

	weather_entity "sample/example/weather/domain/entity"
)

// tokyoLocation は Asia/Tokyo タイムゾーンを保持します。
// アプリケーション起動時に一度ロードされます。
var tokyoLocation *time.Location

func init() {
	var err error
	tokyoLocation, err = time.LoadLocation("Asia/Tokyo")
	if err != nil {
		// ロードに失敗した場合はUTCにフォールバック（通常は発生しないはず）
		tokyoLocation = time.UTC
	}
}

type WeatherProcessor struct {
	// 設定などの依存があれば
	// config *config.WeatherProcessorConfig // 必要に応じて追加
}

// NewWeatherProcessor が ComponentBuilder のシグネチャに合わせるように修正
func NewWeatherProcessor(cfg *config.Config, db *sql.DB, properties map[string]string) (*WeatherProcessor, error) { // ★ 変更: シグネチャを factory.ComponentBuilder に合わせる
	_ = cfg        // 未使用の引数を無視
	_ = db
	_ = properties
	return &WeatherProcessor{
		// 初期化
		// config: cfg, // 必要に応じて初期化
	}, nil
}

// Process メソッドが Processor[*entity.OpenMeteoForecast, []*entity.WeatherDataToStore] インターフェースを満たすように修正
func (p *WeatherProcessor) Process(ctx context.Context, item any) (any, error) { // 引数と戻り値を any に変更
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// item を元の型に型アサーション
	forecast, ok := item.(*weather_entity.OpenMeteoForecast)
	if !ok {
		// 予期しない入力アイテムの型はスキップ可能、リトライ不可
		return nil, exception.NewBatchError("weather_processor", fmt.Sprintf("予期しない入力アイテムの型です: %T", item), nil, false, true)
	}
 
	var dataToStore []*weather_entity.WeatherDataToStore
	// CollectedAt は現在時刻を使用しているため、設定は不要
	collectedAt := time.Now().In(tokyoLocation) // ロードしたタイムゾーンを使用

	for i := range forecast.Hourly.Time {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// タイムゾーンを指定してパース
		parsedTime, err := time.ParseInLocation("2006-01-02T15:04", forecast.Hourly.Time[i], tokyoLocation)
		if err != nil {
			parsedTime, err = time.ParseInLocation(time.RFC3339, forecast.Hourly.Time[i], tokyoLocation)
			if err != nil {
				// 時間のパース失敗はスキップ可能、リトライ不可
				return nil, exception.NewBatchError("weather_processor", fmt.Sprintf("時間のパースに失敗しました: %s", forecast.Hourly.Time[i]), err, false, true)
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
	return dataToStore, nil
}

// WeatherProcessor が Processor[*entity.OpenMeteoForecast, []*entity.WeatherDataToStore] インターフェースを満たすことを確認
var _ itemprocessor.ItemProcessor[any, any] = (*WeatherProcessor)(nil) // ItemProcessor[any, any] に変更
