package repository

import (
  "context" // context パッケージをインポート
  "database/sql" // sql パッケージをインポート
  "sample/src/main/go/batch/domain/entity"
)

type WeatherRepository interface {
  // BulkInsertWeatherData にトランザクションを渡すように変更
  BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, items []entity.WeatherDataToStore) error
  // Close メソッドも Context を受け取るように変更することも検討
  Close() error
  // 他のデータアクセス操作 (GetWeatherData など) があれば定義
}
