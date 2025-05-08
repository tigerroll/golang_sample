package reader

import (
  "context"
  "fmt"
  "encoding/json"
  "net/http"

  config  "sample/src/main/go/batch/config" // config パッケージをインポート
  entity  "sample/src/main/go/batch/domain/entity"
  logger  "sample/src/main/go/batch/util/logger"
)

type WeatherReader struct {
  config *config.WeatherReaderConfig // 小さい設定構造体を使用
  client *http.Client
}

// NewWeatherReader が WeatherReaderConfig を受け取るように修正
func NewWeatherReader(cfg *config.WeatherReaderConfig) *WeatherReader {
  return &WeatherReader{
    config: cfg,
    client: &http.Client{},
  }
}

// Read メソッドが Reader インターフェースを満たすように修正
func (r *WeatherReader) Read(ctx context.Context) (interface{}, error) {
  // config フィールドから必要な設定にアクセス
  apiURL := fmt.Sprintf("%s?latitude=35.6895&longitude=139.6917&hourly=temperature_2m,weather_code", r.config.APIEndpoint)
  req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
  if err != nil {
    return nil, fmt.Errorf("HTTPリクエストの作成に失敗しました: %w", err)
  }
  // config フィールドから必要な設定にアクセス
  req.Header.Set("X-API-Key", r.config.APIKey)

  resp, err := r.client.Do(req)
  if err != nil {
    return nil, fmt.Errorf("APIへのリクエストに失敗しました: %w", err)
  }
  defer resp.Body.Close()

  if resp.StatusCode != http.StatusOK {
    return nil, fmt.Errorf("APIからエラーレスポンスが返されました: ステータスコード %d", resp.StatusCode)
  }

  var forecastData entity.OpenMeteoForecast
  if err := json.NewDecoder(resp.Body).Decode(&forecastData); err != nil {
    return nil, fmt.Errorf("APIレスポンスのデコードに失敗しました: %w", err)
  }

  logger.Debugf("APIから取得したデータ: %+v", forecastData)
  return &forecastData, nil // interface{} 型として返す
}

// WeatherReader が Reader インターフェースを満たすことを確認
var _ Reader = (*WeatherReader)(nil)
