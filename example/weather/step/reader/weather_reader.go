package weatherreader // パッケージ名を 'weatherreader' に変更

import (
	"context"
	"database/sql" // sql パッケージをインポート
	"encoding/json" // json パッケージをインポート
	"fmt"
	"io" // io パッケージをインポート
	"net/http"
	"time" // time パッケージをインポート

	config "sample/pkg/batch/config" // config パッケージをインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	itemreader "sample/pkg/batch/step/reader" // ItemReader インターフェースをインポート
	logger "sample/pkg/batch/util/logger"
	"sample/pkg/batch/util/exception" // exception パッケージをインポート

	weather_config "sample/example/weather/config" // weather_config パッケージをインポート
	weather_entity "sample/example/weather/domain/entity" // weather_entity パッケージをインポート
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

// NewWeatherReader が ComponentBuilder のシグネチャに合わせるように修正
func NewWeatherReader(cfg *config.Config, db *sql.DB, properties map[string]string) (*WeatherReader, error) { // ★ 変更: シグネチャを factory.ComponentBuilder に合わせる
	_ = db // 未使用の引数を無視

	weatherReaderCfg := &weather_config.WeatherReaderConfig{
		APIEndpoint: cfg.Batch.APIEndpoint,
		APIKey:      cfg.Batch.APIKey,
	}

	// JSL properties があれば、config の値を上書きする
	if endpoint, ok := properties["apiEndpoint"]; ok && endpoint != "" {
		weatherReaderCfg.APIEndpoint = endpoint
	}
	if apiKey, ok := properties["apiKey"]; ok && apiKey != "" {
		weatherReaderCfg.APIKey = apiKey
	}

	return &WeatherReader{
		config: weatherReaderCfg,
		client: &http.Client{},
		executionContext: core.NewExecutionContext(), // 初期化
	}, nil
}

// Read メソッドが Reader インターフェースを満たすように修正
// アイテムを一つずつ返すように変更
func (r *WeatherReader) Read(ctx context.Context) (any, error) { // 戻り値を any に変更
	// Context の完了をチェック
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 初回読み込み時、またはリスタート後にデータがロードされていない場合にデータをフェッチ
	if r.forecastData == nil {
		logger.Debugf("WeatherReader: forecastData is nil, fetching from API. Current index: %d", r.currentIndex) // デバッグログを追加
		// API呼び出しのURLを構築
		// 例: 東京の緯度経度を使用
		apiURL := fmt.Sprintf("%s?latitude=35.6895&longitude=139.6917&hourly=temperature_2m,weather_code", r.config.APIEndpoint)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			// HTTPリクエストの作成失敗はリトライ不可、スキップ不可
			return nil, exception.NewBatchError("weather_reader", "HTTPリクエストの作成に失敗しました", err, false, false)
		}
		// APIキーが必要な場合、ヘッダーに追加などを検討
		if r.config.APIKey != "" {
			req.Header.Set("X-API-Key", r.config.APIKey) // 必要に応じてAPIキーを追加
		}

		client := &http.Client{Timeout: 10 * time.Second} // タイムアウトを設定
		resp, err := client.Do(req)
		if err != nil {
			logger.Errorf("APIへのリクエストに失敗しました: %v", err)
			// API呼び出しエラーは一時的なネットワーク問題の可能性があるのでリトライ可能、スキップ不可
			return nil, exception.NewBatchError("weather_reader", "API呼び出しエラー", err, true, false)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			// レスポンスボディを読み取り、エラーメッセージに含めることを検討
			bodyBytes, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				// ボディ読み込みエラーはリトライ可能、スキップ不可
				return nil, exception.NewBatchError("weather_reader", fmt.Sprintf("APIからエラーレスポンスが返されました: ステータスコード %d (ボディ読み込みエラー)", resp.StatusCode), readErr, true, false)
			}
			// APIからのエラーレスポンスはリトライ可能、スキップ不可
			return nil, exception.NewBatchError("weather_reader", fmt.Sprintf("APIからエラーレスポンスが返されました: ステータスコード %d, ボディ: %s", resp.StatusCode, string(bodyBytes)), nil, true, false)
		}

		var forecastData weather_entity.OpenMeteoForecast
		if err := json.NewDecoder(resp.Body).Decode(&forecastData); err != nil {
			// APIレスポンスのデコード失敗はリトライ不可、スキップ不可
			return nil, exception.NewBatchError("weather_reader", "APIレスポンスのデコードに失敗しました", err, false, false)
		}
		r.forecastData = &forecastData // データを保持
		// currentIndex は SetExecutionContext で復元されるか、0に初期化される
		logger.Debugf("Successfully fetched %d hourly weather records from API.", len(r.forecastData.Hourly.Time))
	}

	// 保持しているデータからアイテムを一つずつ返す
	if r.currentIndex >= len(r.forecastData.Hourly.Time) {
		// 全てのアイテムを読み込み終えた
		logger.Debugf("Finished reading all weather items.")
		// r.forecastData = nil // この行は以前削除済み
		// r.currentIndex = 0 // この行を削除: EOF時にインデックスをリセットしない
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
	return itemToProcess, nil
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
  if val, ok := ec.Get("currentIndex"); ok {
    if idx, ok := val.(int); ok {
    r.currentIndex = idx
    logger.Debugf("WeatherReader: ExecutionContext から currentIndex を復元しました: %d", r.currentIndex)
    } else { logger.Warnf("WeatherReader: ExecutionContext の currentIndex が予期しない型です。0 に初期化します。") }
  }
  // 見つからない場合や型が異なる場合は r.currentIndex はデフォルト値 (0) のまま
  
  // forecastData の復元 (JSON文字列として保存されていると仮定)
  if val, ok := ec.Get("forecastData"); ok {
    if forecastJSON, ok := val.(string); ok && forecastJSON != "" {
    var forecast weather_entity.OpenMeteoForecast
    if err := json.Unmarshal([]byte(forecastJSON), &forecast); err != nil {
      logger.Errorf("WeatherReader: ExecutionContext から forecastData のデコードに失敗しました: %v", err)
      // forecastData のデコード失敗はリトライ不可、スキップ不可
      return exception.NewBatchError("weather_reader", "forecastData のデコードに失敗しました", err, false, false)
    }
    r.forecastData = &forecast
    logger.Debugf("WeatherReader: ExecutionContext から forecastData を復元しました。レコード数: %d", len(r.forecastData.Hourly.Time)) 
    } else { logger.Warnf("WeatherReader: ExecutionContext の forecastData が予期しない型です。次回 Read 時にフェッチします。") }
  }
  // 見つからない場合や型が異なる場合は r.forecastData はデフォルト値 (nil) のまま

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
      // forecastData のエンコード失敗はリトライ不可、スキップ不可
      return nil, exception.NewBatchError("weather_reader", "forecastData のエンコードに失敗しました", err, false, false)
    }
    newEC.Put("forecastData", string(forecastJSON))
  }

  r.executionContext = newEC // 内部の ExecutionContext も更新
  return newEC, nil
}

// WeatherReader が Reader[*entity.OpenMeteoForecast] インターフェースを満たすことを確認
var _ itemreader.ItemReader[any] = (*WeatherReader)(nil) // ItemReader[any] に変更
