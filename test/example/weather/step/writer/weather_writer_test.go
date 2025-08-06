package writer_test // パッケージ名を 'writer_test' に変更

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	weather_entity "sample/example/weather/domain/entity"
	weatherwriter "sample/example/weather/step/writer" // プロダクションコードのパッケージをインポート
	"sample/pkg/batch/util/exception"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockWeatherRepository は appRepo.WeatherRepository インターフェースのモック実装です。
type MockWeatherRepository struct {
	mock.Mock
}

func (m *MockWeatherRepository) BulkInsertWeatherData(ctx context.Context, tx *sql.Tx, data []weather_entity.WeatherDataToStore) error {
	args := m.Called(ctx, tx, data)
	return args.Error(0)
}

func (m *MockWeatherRepository) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestWeatherWriter_WriteScenarios(t *testing.T) {
	sampleData := []*weather_entity.WeatherDataToStore{
		{Time: time.Now(), WeatherCode: 0, Temperature2M: 10.0, Latitude: 35.0, Longitude: 139.0, CollectedAt: time.Now()},
		{Time: time.Now().Add(time.Hour), WeatherCode: 1, Temperature2M: 11.0, Latitude: 35.0, Longitude: 139.0, CollectedAt: time.Now()},
	}

	tests := []struct {
		name           string
		inputItems     []any
		mockSetup      func(*MockWeatherRepository)
		expectedError  error
		isRetryable    bool
		isSkippable    bool
		expectedErrMsg string
	}{
		{
			name: "Successful Write",
			inputItems: []any{
				sampleData[0],
				sampleData[1],
			},
			mockSetup: func(m *MockWeatherRepository) {
				m.On("BulkInsertWeatherData", mock.Anything, mock.Anything, mock.AnythingOfType("[]weather_entity.WeatherDataToStore")).Return(nil).Once()
				m.On("Close").Return(nil).Once()
			},
			expectedError: nil,
		},
		{
			name: "Database Error - Retryable",
			inputItems: []any{
				sampleData[0],
			},
			mockSetup: func(m *MockWeatherRepository) {
				m.On("BulkInsertWeatherData", mock.Anything, mock.Anything, mock.AnythingOfType("[]weather_entity.WeatherDataToStore")).Return(errors.New("db connection lost")).Once()
				m.On("Close").Return(nil).Once()
			},
			expectedError:  &exception.BatchError{},
			isRetryable:    true,
			isSkippable:    false,
			expectedErrMsg: "天気データのバルク挿入に失敗しました",
		},
		{
			name: "Invalid Input Item Type (Skippable)",
			inputItems: []any{
				sampleData[0],
				"invalid_item", // 不正な型のアイテム
			},
			mockSetup: func(m *MockWeatherRepository) {
				// 不正なアイテムが来た場合、BulkInsertWeatherData は呼ばれないはず
				m.On("Close").Return(nil).Once()
			},
			expectedError:  &exception.BatchError{},
			isRetryable:    false,
			isSkippable:    true,
			expectedErrMsg: "予期しない入力アイテムの型です: string",
		},
		{
			name:       "Empty Items",
			inputItems: []any{},
			mockSetup: func(m *MockWeatherRepository) {
				// 何も呼ばれないことを期待
				m.On("Close").Return(nil).Once()
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRepo := new(MockWeatherRepository)
			tt.mockSetup(mockRepo) // モックの設定を適用

			writer := weatherwriter.NewWeatherWriter(mockRepo)
			ctx := context.Background()
			tx := &sql.Tx{} // ダミーのトランザクション

			err := writer.Write(ctx, tx, tt.inputItems)

			if tt.expectedError == nil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err, "Expected an error")
				batchErr, ok := err.(*exception.BatchError)
				assert.True(t, ok, "Expected error to be of type *exception.BatchError")
				assert.Equal(t, tt.isRetryable, batchErr.IsRetryable(), "Expected IsRetryable to match test case")
				assert.Equal(t, tt.isSkippable, batchErr.IsSkippable(), "Expected IsSkippable to match test case")
				assert.Contains(t, batchErr.Message, tt.expectedErrMsg, "Expected error message to contain specific text")
			}

			assert.NoError(t, writer.Close(ctx))
			mockRepo.AssertExpectations(t) // モックの期待が満たされたことを検証
		})
	}
}

// TestWeatherWriter_ContextCancellation は Context のキャンセルが正しく処理されるかテストします。
func TestWeatherWriter_ContextCancellation(t *testing.T) {
	mockRepo := new(MockWeatherRepository)
	// Write メソッドが Context のキャンセルを検知して早期リターンするため、BulkInsertWeatherData は呼び出されない
	// そのため、BulkInsertWeatherData のモック期待は設定しない
	mockRepo.On("Close").Return(nil).Once()

	writer := weatherwriter.NewWeatherWriter(mockRepo)
	// Context を作成し、すぐにキャンセルする
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Context のキャンセルは defer で実行 (Write メソッドのテスト用)

	// Write 呼び出しの直前にキャンセルをトリガー
	cancel()

	sampleData := []any{
		&weather_entity.WeatherDataToStore{Time: time.Now(), WeatherCode: 0, Temperature2M: 10.0, Latitude: 35.0, Longitude: 139.0, CollectedAt: time.Now()},
	}
	tx := &sql.Tx{} // ダミーのトランザクション

	err := writer.Write(ctx, tx, sampleData)
	assert.Error(t, err, "Expected an error due to context cancellation") // エラーが発生したことを確認
	assert.True(t, errors.Is(err, context.Canceled), "Expected error to be context.Canceled") // errors.Is を使用してエラーの型を確認

	assert.NoError(t, writer.Close(context.Background())) // Close には新しいコンテキストを渡す
	mockRepo.AssertExpectations(t)
}
