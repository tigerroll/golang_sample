package repository

import (
  "context"
  "sample/src/main/go/batch/domain/entity"
)

type WeatherRepository interface {
  SaveWeatherData(ctx context.Context, forecast entity.OpenMeteoForecast) error
  Close() error
  // 他のデータアクセス操作 (GetWeatherData など) があれば定義
}
