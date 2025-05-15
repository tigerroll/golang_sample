package reader

import (
  "context"
  "encoding/json" // json パッケージをインポート
  "fmt"
  "net/http"
  "io" // io パッケージをインポート
  "time" // time パッケージをインポート // ★ time パッケージは使用されているため残す

  config  "sample/src/main/go/batch/config" // config パッケージをインポート
  entity  "sample/src/main/go/batch/domain/entity" // ★ インポートパスを修正
  logger  "sample/src/main/go/batch/util/logger"
)

// WeatherReader は天気予報データを外部APIから読み込む Reader です。
// step.ItemReader インターフェースを実装します。
type WeatherReader struct {
  config *config.WeatherReaderConfig // 小さい設定構造体を使用
  client *http.Client
  forecastData *entity.OpenMeteoForecast // ★ フェッチしたデータを保持
  currentIndex int // ★ 現在読み込み済みのインデックス

  // ★ エラーテスト用のフィールド (一時的) ★
  readCount int // Read メソッドが呼び出された回数をカウント
}

// NewWeatherReader が WeatherReaderConfig を受け取るように修正
func NewWeatherReader(cfg *config.WeatherReaderConfig) *WeatherReader {
  return &WeatherReader{
    config: cfg,
    client: &http.Client{},
    // forecastData と currentIndex は初期値 (nil, 0) でOK
    readCount: 0, // ★ エラーテスト用のフィールドを初期化
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

  // ★ エラーテスト用のコード (一時的) ★
  r.readCount++
  if r.readCount >= 1 { // 例: 最初の読み込み後にエラーを発生させる
    logger.Warnf("WeatherReader: テストのために意図的にエラーを発生させます (Read Count: %d)", r.readCount)
    // ここで StepExecution の ExitStatus を FAILED に設定する必要はありません。
    // Reader がエラーを返すと、ChunkOrientedStep がそれを捕捉し、
    // リトライ処理を経て最終的にステップの ExitStatus を FAILED に設定します。
    return nil, fmt.Errorf("WeatherReader: テスト用の人工的なエラー (Read Count: %d)", r.readCount)
  }
  // ★ エラーテスト用のコードここまで ★


  // TODO: 実際のAPI呼び出しロジックを実装
  // 現在はダミーデータを返すか、または実際のAPI呼び出しが実装されているかによる
  // 実際のAPI呼び出しが実装されている場合、そのエラーハンドリングを行う

  // ダミーデータを返す例 (実際のAPI呼び出しに置き換える必要があります)
  // if r.currentIndex >= len(r.items) {
  //   return nil, io.EOF // 読み込むデータがない
  // }
  // item := &r.items[r.currentIndex]
  // r.currentIndex++
  // return item, nil

  // 実際のAPI呼び出し例 (エラーハンドリングを含む)
  // 以下のコードはAPI呼び出しのスケルトンです。実際のAPI仕様に合わせて修正してください。
  apiURL := fmt.Sprintf("%s?latitude=35.6895&longitude=139.6917&hourly=temperature_2m,weather_code", r.config.APIEndpoint)
  req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
  if err != nil {
    return nil, fmt.Errorf("HTTPリクエストの作成に失敗しました: %w", err)
  }
  req.Header.Set("X-API-Key", r.config.APIKey)

  client := &http.Client{Timeout: 10 * time.Second} // タイムアウトを設定
  resp, err := client.Do(req)
  if err != nil {
    logger.Errorf("API 呼び出しに失敗しました: %v", err)
    return nil, fmt.Errorf("API 呼び出しエラー: %w", err)
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


  // TODO: 上記のテスト用コードを削除し、実際のAPI呼び出しとページネーション/状態管理ロジックを実装

}

// WeatherReader が Reader インターフェースを満たすことを確認
var _ Reader = (*WeatherReader)(nil)
