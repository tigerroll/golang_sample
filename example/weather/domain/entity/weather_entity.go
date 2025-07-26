package weather_entity

import "time"

// Hourly は Open Meteo API から取得する時間ごとの天気予報データを表す構造体です。
type Hourly struct {
  Time          []string  `json:"time"` // string 型に変更
  WeatherCode   []int     `json:"weather_code"`
  Temperature2M []float64 `json:"temperature_2m"`
}

// OpenMeteoForecast は Open Meteo API から取得する生の天気予報データを表す構造体です。
type OpenMeteoForecast struct {
  Latitude  float64 `json:"latitude"`
  Longitude float64 `json:"longitude"`
  Hourly    Hourly  `json:"hourly"`
}

// WeatherDataToStore は Redshift に保存する天気予報データを表す構造体です。
type WeatherDataToStore struct {
  Time          time.Time
  WeatherCode   int
  Temperature2M float64
  Latitude      float64
  Longitude     float64
  CollectedAt   time.Time
}
