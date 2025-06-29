package weather_repository

import (
  "context" // context パッケージをインポート
  "database/sql" // sql パッケージをインポート
  weather_entity "sample/src/main/go/batch/weather/domain/entity"
)

type WeatherRepository interface {
  // BulkInsertWeatherData にトランザクションを渡すように変更
  BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []weather_entity.WeatherDataToStore) error
  // Close メソッドも Context を受け取るように変更することも検討
  Close() error
  // 他のデータアクセス操作 (GetWeatherData など) があれば定義
}
