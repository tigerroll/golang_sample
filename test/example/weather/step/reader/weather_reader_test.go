package reader_test // パッケージ名を 'reader_test' に変更

import (
	"context"
	"encoding/json"
	"errors" // errors パッケージを再インポート
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url" // net/url パッケージを再インポート
	"testing"
	"time"

	weather_config "sample/example/weather/config"
	weather_entity "sample/example/weather/domain/entity"
	weatherreader "sample/example/weather/step/reader" // プロダクションコードのパッケージをインポート
	"sample/pkg/batch/util/exception"

	"github.com/stretchr/testify/assert"
)

// mockOpenMeteoResponse はテスト用のOpenMeteo APIのモックレスポンスを生成します。
func mockOpenMeteoResponse(numRecords int, includeInvalidTime bool) string {
	times := make([]string, numRecords)
	weatherCodes := make([]int, numRecords)
	temperatures := make([]float64, numRecords)

	for i := 0; i < numRecords; i++ {
		if includeInvalidTime && i == 1 { // 2番目のレコードに不正な時間を注入 (Processorでエラーになることを想定)
			times[i] = "INVALID_TIME_FORMAT"
		} else {
			times[i] = time.Now().Add(time.Duration(i) * time.Hour).Format("2006-01-02T15:04")
		}
		weatherCodes[i] = i
		temperatures[i] = float64(20 + i)
	}

	hourly := weather_entity.Hourly{
		Time:          times,
		WeatherCode:   weatherCodes,
		Temperature2M: temperatures,
	}
	forecast := weather_entity.OpenMeteoForecast{
		Latitude:  35.6895,
		Longitude: 139.6917,
		Hourly:    hourly,
	}

	data, _ := json.Marshal(forecast)
	return string(data)
}

// TestWeatherReader_ReadScenarios は WeatherReader の様々な読み込みシナリオをテストします。
func TestWeatherReader_ReadScenarios(t *testing.T) {
	tests := []struct {
		name           string
		handler        http.HandlerFunc
		expectedItems  int
		expectedError  error // 期待されるエラー (io.EOF, *exception.BatchError など)
		isRetryable    bool
		isSkippable    bool
		expectedErrMsg string
	}{
		{
			name: "Successful Read - Multiple Items",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, mockOpenMeteoResponse(5, false))
			}),
			expectedItems: 5,
			expectedError: nil,
		},
		{
			name: "API Error - 500 Internal Server Error (Retryable)",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintln(w, "Internal Server Error")
			}),
			expectedItems:  0, // 初回API呼び出しでエラー
			expectedError:  &exception.BatchError{}, // 型チェック用
			isRetryable:    true,
			isSkippable:    false,
			expectedErrMsg: "APIからエラーレスポンスが返されました: ステータスコード 500",
		},
		{
			name: "API Error - 400 Bad Request (Retryable)", // WeatherReaderはAPIエラーを全てリトライ可能とマークしているため
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintln(w, "Bad Request")
			}),
			expectedItems:  0,
			expectedError:  &exception.BatchError{},
			isRetryable:    true,
			isSkippable:    false,
			expectedErrMsg: "APIからエラーレスポンスが返されました: ステータスコード 400",
		},
		{
			name: "Malformed JSON Response (Non-Retryable, Non-Skippable)",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, `{"invalid_json"`) // 不正なJSON
			}),
			expectedItems:  0,
			expectedError:  &exception.BatchError{},
			isRetryable:    false,
			isSkippable:    false,
			expectedErrMsg: "APIレスポンスのデコードに失敗しました",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// モックAPIサーバーのセットアップ
			ts := httptest.NewServer(tt.handler)
			defer ts.Close()

			cfg := &weather_config.WeatherReaderConfig{
				APIEndpoint: ts.URL, // モックサーバーのURLを使用
				APIKey:      "dummy-key",
			}
			reader := weatherreader.NewWeatherReader(cfg)
			ctx := context.Background()

			readCount := 0
			var lastErr error
			for {
				item, err := reader.Read(ctx)
				if err == io.EOF {
					lastErr = err
					break
				}
				if err != nil {
					lastErr = err
					break
				}
				assert.NotNil(t, item, "Read item should not be nil on successful read")
				readCount++
			}

			if tt.expectedError == nil {
				assert.Equal(t, io.EOF, lastErr, "Expected EOF after successful reads")
				assert.Equal(t, tt.expectedItems, readCount, "Expected correct number of items read")
			} else {
				assert.Error(t, lastErr, "Expected an error")
				batchErr, ok := lastErr.(*exception.BatchError)
				assert.True(t, ok, "Expected error to be of type *exception.BatchError")
				assert.Equal(t, tt.isRetryable, batchErr.IsRetryable(), "Expected IsRetryable to match test case")
				assert.Equal(t, tt.isSkippable, batchErr.IsSkippable(), "Expected IsSkippable to match test case")
				assert.Contains(t, batchErr.Message, tt.expectedErrMsg, "Expected error message to contain specific text")
			}

			// Readerをクローズ
			assert.NoError(t, reader.Close(ctx))
		})
	}
}

// TestWeatherReader_ExecutionContextPersistence は ExecutionContext の状態保存と復元をテストします。
func TestWeatherReader_ExecutionContextPersistence(t *testing.T) {
	// モックAPIサーバーのセットアップ
	mockResponse := mockOpenMeteoResponse(10, false) // 10レコード
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, mockResponse)
	}))
	defer ts.Close()

	cfg := &weather_config.WeatherReaderConfig{
		APIEndpoint: ts.URL,
		APIKey:      "dummy-key",
	}
	reader := weatherreader.NewWeatherReader(cfg)
	ctx := context.Background()

	// 最初の5アイテムを読み込む
	for i := 0; i < 5; i++ {
		_, err := reader.Read(ctx)
		assert.NoError(t, err)
	}

	// ExecutionContext を取得
	ec, err := reader.GetExecutionContext(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, ec)

	// currentIndex が正しく保存されていることを確認 (ExecutionContext は map[string]interface{} なので直接アクセス)
	idx, ok := ec["currentIndex"].(int)
	assert.True(t, ok, "currentIndex should be an int in EC")
	assert.Equal(t, 5, idx, "Expected currentIndex to be 5 after reading 5 items")

	// forecastData が保存されていることを確認
	forecastJSON, ok := ec["forecastData"].(string)
	assert.True(t, ok, "forecastData should be a string in EC")
	assert.NotEmpty(t, forecastJSON, "forecastData should not be empty")

	// 新しいReaderインスタンスを作成し、ExecutionContext を復元
	newReader := weatherreader.NewWeatherReader(cfg)
	err = newReader.SetExecutionContext(ctx, ec)
	assert.NoError(t, err)

	// 復元されたReaderで残りのアイテムを読み込む
	readCount := 0
	for {
		item, err := newReader.Read(ctx)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		assert.NotNil(t, item)
		readCount++
	}
	assert.Equal(t, 5, readCount, "Expected to read remaining 5 items after EC restoration") // 10 - 5 = 5

	assert.NoError(t, reader.Close(ctx))
	assert.NoError(t, newReader.Close(ctx))
}

// TestWeatherReader_ContextCancellation は Context のキャンセルが正しく処理されるかテストします。
func TestWeatherReader_ContextCancellation(t *testing.T) {
	// API呼び出しが遅延するモックサーバー
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond) // 意図的に遅延
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, mockOpenMeteoResponse(1, false))
	}))
	defer ts.Close()

	cfg := &weather_config.WeatherReaderConfig{
		APIEndpoint: ts.URL,
		APIKey:      "dummy-key",
	}
	reader := weatherreader.NewWeatherReader(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond) // 短いタイムアウト
	defer cancel()

	// Read が Context のキャンセルによって中断されることを期待
	_, err := reader.Read(ctx)
	assert.Error(t, err, "Expected an error due to context cancellation")

	// Explicitly check the error chain using errors.As and errors.Is
	var batchErr *exception.BatchError
	assert.True(t, errors.As(err, &batchErr), "Expected error to be *exception.BatchError")
	
	var urlErr *url.Error
	assert.True(t, errors.As(batchErr.OriginalErr, &urlErr), "Expected BatchError.OriginalErr to be *url.Error")

	assert.True(t, errors.Is(urlErr.Err, context.DeadlineExceeded), "Expected underlying error of url.Error to be context.DeadlineExceeded")

	assert.ErrorIs(t, reader.Close(ctx), context.DeadlineExceeded, "Expected Close to return context.DeadlineExceeded when context is done")
}
