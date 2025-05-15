package repository

import (
  "context" // context パッケージをインポート
  "sample/src/main/go/batch/domain/entity"
)

type WeatherRepository interface {
  // SaveWeatherData メソッドに ctx context.Context を追加
  SaveWeatherData(ctx context.Context, forecast entity.OpenMeteoForecast) error
  // Close メソッドも Context を受け取るように変更することも検討
  Close() error
  // 他のデータアクセス操作 (GetWeatherData など) があれば定義
}