package reader

import (
  "context"
  "encoding/json"
  "fmt"
  "net/http"
  "io" // io パッケージをインポート

  config  "sample/src/main/go/batch/config" // config パッケージをインポート
  entity  "sample/src/main/go/batch/domain/entity"
  logger  "sample/src/main/go/batch/util/logger"
)

type WeatherReader struct {
  config *config.WeatherReaderConfig // 小さい設定構造体を使用
  client *http.Client
  forecastData *entity.OpenMeteoForecast // ★ フェッチしたデータを保持
  currentIndex int // ★ 現在読み込み済みのインデックス
}

// NewWeatherReader が WeatherReaderConfig を受け取るように修正
func NewWeatherReader(cfg *config.WeatherReaderConfig) *WeatherReader {
  return &WeatherReader{
    config: cfg,
    client: &http.Client{},
    // forecastData と currentIndex は初期値 (nil, 0) でOK
  }
}

// Read メソッドが Reader インターフェースを満たすように修正
// アイテムを一つずつ返すように変更
func (r *WeatherReader) Read(ctx context.Context) (interface{}, error) {
  // Context の完了をチェック
  select {
  case <-ctx.Done():
    return nil, ctx.Err()
  default:
  }

  // 初回読み込み時にデータをフェッチ
  if r.forecastData == nil {
    logger.Debugf("Fetching weather data from API...")
    apiURL := fmt.Sprintf("%s?latitude=35.6895&longitude=139.6917&hourly=temperature_2m,weather_code", r.config.APIEndpoint)
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
    if err != nil {
      return nil, fmt.Errorf("HTTPリクエストの作成に失敗しました: %w", err)
    }
    req.Header.Set("X-API-Key", r.config.APIKey)

    resp, err := r.client.Do(req)
    if err != nil {
      return nil, fmt.Errorf("APIへのリクエストに失敗しました: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
      // レスポンスボディを読み取り、エラーメッセージに含めることを検討
      bodyBytes, readErr := io.ReadAll(resp.Body)
      if readErr != nil {
          return nil, fmt.Errorf("APIからエラーレスポンスが返されました: ステータスコード %d (ボディ読み込みエラー: %w)", resp.StatusCode, readErr)
      }
      return nil, fmt.Errorf("APIからエラーレスポンスが返されました: ステータスコード %d, ボディ: %s", resp.StatusCode, string(bodyBytes))
    }

    var forecastData entity.OpenMeteoForecast
    if err := json.NewDecoder(resp.Body).Decode(&forecastData); err != nil {
      return nil, fmt.Errorf("APIレスポンスのデコードに失敗しました: %w", err)
    }
    r.forecastData = &forecastData // データを保持
    r.currentIndex = 0 // インデックスをリセット
    logger.Debugf("Successfully fetched %d hourly weather records from API.", len(r.forecastData.Hourly.Time))
  }

  // 保持しているデータからアイテムを一つずつ返す
  if r.currentIndex >= len(r.forecastData.Hourly.Time) {
    // 全てのアイテムを読み込み終えた
    logger.Debugf("Finished reading all weather items.")
    r.forecastData = nil // 次回 Read 時に再度フェッチするためにリセット
    r.currentIndex = 0
    return nil, io.EOF // アイテムがないことを示す
  }

  // 現在のインデックスのアイテムを構築して返す
  itemToProcess := &entity.OpenMeteoForecast{
    Latitude:  r.forecastData.Latitude,
    Longitude: r.forecastData.Longitude,
    Hourly: entity.Hourly{
      Time:          []string{r.forecastData.Hourly.Time[r.currentIndex]},
      WeatherCode:   []int{r.forecastData.Hourly.WeatherCode[r.currentIndex]},
      Temperature2M: []float64{r.forecastData.Hourly.Temperature2M[r.currentIndex]},
    },
  }

  r.currentIndex++ // インデックスをインクリメント

  //logger.Debugf("Read item at index %d: %+v", r.currentIndex-1, itemToProcess)
  return itemToProcess, nil // interface{} 型として返す
}

// WeatherReader が Reader インターフェースを満たすことを確認
var _ Reader = (*WeatherReader)(nil)