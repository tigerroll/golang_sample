package weather_reader

import (
	"context"
	"encoding/json" // json パッケージをインポート
	"fmt"
	"io" // io パッケージをインポート
	"net/http"
	"time" // time パッケージをインポート

	weather_config "sample/pkg/batch/weather/config" // weather_config パッケージをインポート
	weather_entity "sample/pkg/batch/weather/domain/entity" // weather_entity パッケージをインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	reader "sample/pkg/batch/step/reader" // Reader インターフェースをインポート
	logger "sample/pkg/batch/util/logger"
)

// WeatherReader は天気予報データを外部APIから読み込む Reader です。
// Reader[*entity.OpenMeteoForecast] インターフェースを実装します。
type WeatherReader struct {
	config *weather_config.WeatherReaderConfig // 小さい設定構造体を使用
	client *http.Client
	forecastData *weather_entity.OpenMeteoForecast // ★ フェッチしたデータを保持
	currentIndex int // ★ 現在読み込み済みのインデックス

	// ExecutionContext を保持するためのフィールド
	executionContext core.ExecutionContext
}

// NewWeatherReader が WeatherReaderConfig を受け取るように修正
func NewWeatherReader(cfg *weather_config.WeatherReaderConfig) *WeatherReader {
	return &WeatherReader{
		config: cfg,
		client: &http.Client{},
		executionContext: core.NewExecutionContext(), // 初期化
	}
}

// Read メソッドが Reader インターフェースを満たすように修正
// アイテムを一つずつ返すように変更
func (r *WeatherReader) Read(ctx context.Context) (any, error) { // O は any
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 初回読み込み時、またはリスタート後にデータがロードされていない場合にデータをフェッチ
	if r.forecastData == nil {
		logger.Debugf("Fetching weather data from API...")
		// API呼び出しのURLを構築
		// 例: 東京の緯度経度を使用
		apiURL := fmt.Sprintf("%s?latitude=35.6895&longitude=139.6917&hourly=temperature_2m,weather_code", r.config.APIEndpoint)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			return nil, fmt.Errorf("HTTPリクエストの作成に失敗しました: %w", err)
		}
		// APIキーが必要な場合、ヘッダーに追加などを検討
		// req.Header.Set("X-API-Key", r.config.APIKey) // 必要に応じてAPIキーを追加

		client := &http.Client{Timeout: 10 * time.Second} // タイムアウトを設定
		resp, err := client.Do(req)
		if err != nil {
			logger.Errorf("APIへのリクエストに失敗しました: %v", err)
			return nil, fmt.Errorf("API呼び出しエラー: %w", err)
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

		var forecastData weather_entity.OpenMeteoForecast
		if err := json.NewDecoder(resp.Body).Decode(&forecastData); err != nil {
			return nil, fmt.Errorf("APIレスポンスのデコードに失敗しました: %w", err)
		}
		r.forecastData = &forecastData // データを保持
		// currentIndex は SetExecutionContext で復元されるか、0に初期化される
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
	itemToProcess := &weather_entity.OpenMeteoForecast{
		Latitude:  r.forecastData.Latitude,
		Longitude: r.forecastData.Longitude,
		Hourly: weather_entity.Hourly{
			Time:          []string{r.forecastData.Hourly.Time[r.currentIndex]},
			WeatherCode:   []int{r.forecastData.Hourly.WeatherCode[r.currentIndex]},
			Temperature2M: []float64{r.forecastData.Hourly.Temperature2M[r.currentIndex]},
		},
	}

	r.currentIndex++ // インデックスをインクリメント

	//logger.Debugf("Read item at index %d: %+v", r.currentIndex-1, itemToProcess)
	return itemToProcess, nil // any 型として返す
}

// Close は Reader インターフェースの実装です。
// WeatherReader は閉じるリソースがないため、何もしません。
func (r *WeatherReader) Close(ctx context.Context) error {
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }
  logger.Debugf("WeatherReader.Close が呼び出されました。")
  return nil
}

// SetExecutionContext は Reader インターフェースの実装です。
// 渡された ExecutionContext から内部状態を復元します。
func (r *WeatherReader) SetExecutionContext(ctx context.Context, ec core.ExecutionContext) error {
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }
  r.executionContext = ec // まず全体をコピー

  // currentIndex の復元
  if idx, ok := ec.GetInt("currentIndex"); ok {
    r.currentIndex = idx
    logger.Debugf("WeatherReader: ExecutionContext から currentIndex を復元しました: %d", r.currentIndex)
  } else {
    r.currentIndex = 0 // 見つからない場合は初期値
    logger.Debugf("WeatherReader: ExecutionContext に currentIndex が見つかりませんでした。0 に初期化します。")
  }

  // forecastData の復元 (JSON文字列として保存されていると仮定)
  if forecastJSON, ok := ec.GetString("forecastData"); ok && forecastJSON != "" {
    var forecast weather_entity.OpenMeteoForecast
    if err := json.Unmarshal([]byte(forecastJSON), &forecast); err != nil {
      logger.Errorf("WeatherReader: ExecutionContext から forecastData のデコードに失敗しました: %v", err)
      return fmt.Errorf("WeatherReader: forecastData のデコードに失敗しました: %w", err)
    }
    r.forecastData = &forecast
    logger.Debugf("WeatherReader: ExecutionContext から forecastData を復元しました。レコード数: %d", len(r.forecastData.Hourly.Time))
  } else {
    r.forecastData = nil // 見つからない場合はnil
    logger.Debugf("WeatherReader: ExecutionContext に forecastData が見つかりませんでした。次回 Read 時にフェッチします。")
  }

  return nil
}

// GetExecutionContext は Reader インターフェースの実装です。
// 現在の内部状態を ExecutionContext に保存して返します。
func (r *WeatherReader) GetExecutionContext(ctx context.Context) (core.ExecutionContext, error) {
  select {
  case <-ctx.Done():
    return nil, ctx.Err()
  default:
  }
  // 新しい ExecutionContext を作成し、現在の状態を保存
  newEC := core.NewExecutionContext()
  newEC.Put("currentIndex", r.currentIndex)

  if r.forecastData != nil {
    forecastJSON, err := json.Marshal(r.forecastData)
    if err != nil {
      logger.Errorf("WeatherReader: forecastData のエンコードに失敗しました: %v", err)
      return nil, fmt.Errorf("WeatherReader: forecastData のエンコードに失敗しました: %w", err)
    }
    newEC.Put("forecastData", string(forecastJSON))
  }

  r.executionContext = newEC // 内部の ExecutionContext も更新
  return newEC, nil
}

// WeatherReader が Reader[*entity.OpenMeteoForecast] インターフェースを満たすことを確認
var _ reader.Reader[any] = (*WeatherReader)(nil)
