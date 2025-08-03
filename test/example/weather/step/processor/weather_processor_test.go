package processor_test // パッケージ名を 'processor_test' に変更

import (
	"context"
	"testing"
	"time"

	weather_entity "sample/example/weather/domain/entity"
	weatherprocessor "sample/example/weather/step/processor" // プロダクションコードのパッケージをインポート
	"sample/pkg/batch/util/exception"

	"github.com/stretchr/testify/assert"
)

// tokyoLocation はテスト用の Asia/Tokyo タイムゾーンを保持します。
var testTokyoLocation *time.Location

func init() {
	var err error
	testTokyoLocation, err = time.LoadLocation("Asia/Tokyo")
	if err != nil {
		testTokyoLocation = time.UTC // ロード失敗時はUTCにフォールバック
	}
}

func TestWeatherProcessor_ProcessScenarios(t *testing.T) {
	tests := []struct {
		name           string
		input          any
		expectedOutput []*weather_entity.WeatherDataToStore
		expectedError  error // 期待されるエラー
		isRetryable    bool
		isSkippable    bool
		expectedErrMsg string
	}{
		{
			name: "Successful Processing",
			input: &weather_entity.OpenMeteoForecast{
				Latitude:  35.6895,
				Longitude: 139.6917,
				Hourly: weather_entity.Hourly{
					Time:          []string{"2025-01-01T00:00", "2025-01-01T01:00"},
					WeatherCode:   []int{0, 1},
					Temperature2M: []float64{10.0, 11.0},
				},
			},
			expectedOutput: []*weather_entity.WeatherDataToStore{
				{Time: time.Date(2025, time.January, 1, 0, 0, 0, 0, testTokyoLocation), WeatherCode: 0, Temperature2M: 10.0, Latitude: 35.6895, Longitude: 139.6917},
				{Time: time.Date(2025, time.January, 1, 1, 0, 0, 0, testTokyoLocation), WeatherCode: 1, Temperature2M: 11.0, Latitude: 35.6895, Longitude: 139.6917},
			},
			expectedError: nil,
		},
		{
			name: "Invalid Input Type (Skippable)",
			input: "not a forecast", // 不正な入力型
			expectedOutput: nil,
			expectedError:  &exception.BatchError{},
			isRetryable:    false,
			isSkippable:    true,
			expectedErrMsg: "予期しない入力アイテムの型です: string",
		},
		{
			name: "Invalid Time Format (Skippable)",
			input: &weather_entity.OpenMeteoForecast{
				Latitude:  35.6895,
				Longitude: 139.6917,
				Hourly: weather_entity.Hourly{
					Time:          []string{"2025-01-01T00:00", "INVALID_TIME"}, // 不正な時間形式
					WeatherCode:   []int{0, 1},
					Temperature2M: []float64{10.0, 11.0},
				},
			},
			expectedOutput: nil,
			expectedError:  &exception.BatchError{},
			isRetryable:    false,
			isSkippable:    true,
			expectedErrMsg: "時間のパースに失敗しました: INVALID_TIME",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := weatherprocessor.NewWeatherProcessor()
			ctx := context.Background()

			output, err := processor.Process(ctx, tt.input)

			if tt.expectedError == nil {
				assert.NoError(t, err)
				// CollectedAt はテスト実行時に動的に設定されるため、比較から除外するか、別途検証する
				// ここでは、CollectedAt 以外のフィールドが一致することを確認
				actualOutput, ok := output.([]*weather_entity.WeatherDataToStore)
				assert.True(t, ok, "Output should be []*weather_entity.WeatherDataToStore")
				assert.Len(t, actualOutput, len(tt.expectedOutput))

				for i, expected := range tt.expectedOutput {
					assert.Equal(t, expected.Time, actualOutput[i].Time)
					assert.Equal(t, expected.WeatherCode, actualOutput[i].WeatherCode)
					assert.Equal(t, expected.Temperature2M, actualOutput[i].Temperature2M)
					assert.Equal(t, expected.Latitude, actualOutput[i].Latitude)
					assert.Equal(t, expected.Longitude, actualOutput[i].Longitude)
					// CollectedAt はおおよそ現在時刻であることを確認
					assert.WithinDuration(t, time.Now(), actualOutput[i].CollectedAt, 5*time.Second)
				}
			} else {
				assert.Error(t, err, "Expected an error")
				batchErr, ok := err.(*exception.BatchError)
				assert.True(t, ok, "Expected error to be of type *exception.BatchError")
				assert.Equal(t, tt.isRetryable, batchErr.IsRetryable(), "Expected IsRetryable to match test case")
				assert.Equal(t, tt.isSkippable, batchErr.IsSkippable(), "Expected IsSkippable to match test case")
				assert.Contains(t, batchErr.Message, tt.expectedErrMsg, "Expected error message to contain specific text")
				assert.Nil(t, output, "Expected nil output on error")
			}
		})
	}
}

// TestWeatherProcessor_ContextCancellation は Context のキャンセルが正しく処理されるかテストします。
func TestWeatherProcessor_ContextCancellation(t *testing.T) {
	processor := weatherprocessor.NewWeatherProcessor()
	ctx, cancel := context.WithCancel(context.Background())

	// 処理中にキャンセルをトリガー
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	// 大量のデータを処理するフリをして、キャンセルされることを期待
	input := &weather_entity.OpenMeteoForecast{
		Latitude:  35.6895,
		Longitude: 139.6917,
		Hourly: weather_entity.Hourly{
			Time:          make([]string, 10000), // 大量のデータ
			WeatherCode:   make([]int, 10000),
			Temperature2M: make([]float64, 10000),
		},
	}
	for i := 0; i < 10000; i++ {
		input.Hourly.Time[i] = time.Now().Add(time.Duration(i) * time.Hour).Format("2006-01-02T15:04")
		input.Hourly.WeatherCode[i] = i
		input.Hourly.Temperature2M[i] = float64(20 + i)
	}

	output, err := processor.Process(ctx, input)
	assert.Error(t, err, "Expected an error due to context cancellation")
	assert.Nil(t, output, "Expected nil output on context cancellation")
	assert.Equal(t, context.Canceled, err, "Expected context.Canceled error")
}
