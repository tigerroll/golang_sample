package repository

import (
  "context" // context パッケージをインポート
  "sample/src/main/go/batch/domain/entity"
)

type WeatherRepository interface {
  // SaveWeatherData メソッドは ItemWriter パターンに移行したため削除
  // SaveWeatherData(ctx context.Context, forecast entity.OpenMeteoForecast) error // ★ この行を削除
  // BulkInsertWeatherData を追加
  BulkInsertWeatherData(ctx context.Context, items []entity.WeatherDataToStore) error
  // Close メソッドも Context を受け取るように変更することも検討
  Close() error
  // 他のデータアクセス操作 (GetWeatherData など) があれば定義
}
