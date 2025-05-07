package reader

import (
  "context"
  "fmt"
  "encoding/json"
  "net/http"

  config  "sample/src/main/go/batch/config"
  entity  "sample/src/main/go/batch/domain/entity"
  logger  "sample/src/main/go/batch/util/logger"
)

type WeatherReader struct {
  config *config.Config
  client *http.Client
}

func NewWeatherReader(cfg *config.Config) *WeatherReader {
  return &WeatherReader{
    config: cfg,
    client: &http.Client{},
  }
}

func (r *WeatherReader) Read(ctx context.Context) (*entity.OpenMeteoForecast, error) {
  apiURL := fmt.Sprintf("%s?latitude=35.6895&longitude=139.6917&hourly=temperature_2m,weather_code", r.config.Batch.APIEndpoint) // 例: 東京の緯度経度
  req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
  if err != nil {
    return nil, fmt.Errorf("HTTPリクエストの作成に失敗しました: %w", err)
  }
  req.Header.Set("X-API-Key", r.config.Batch.APIKey) // APIキーの設定 (もし必要なら)

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
  return &forecastData, nil
}
