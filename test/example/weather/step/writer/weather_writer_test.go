package writer_test // パッケージ名を 'writer_test' に変更

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	weather_entity "sample/example/weather/domain/entity"
	batch_config "sample/pkg/batch/config" // pkg/batch/config をインポート
	core "sample/pkg/batch/job/core" // core パッケージをインポート
	"sample/pkg/batch/database" // database パッケージをインポート
	weatherwriter "sample/example/weather/step/writer" // プロダクションコードのパッケージをインポート
	"sample/pkg/batch/util/exception"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockWeatherRepository は appRepo.WeatherRepository インターフェースのモック実装です。
type MockWeatherRepository struct {
	mock.Mock
}

func (m *MockWeatherRepository) BulkInsertWeatherData(ctx context.Context, tx database.Tx, data []weather_entity.WeatherDataToStore) error { // tx の型を database.Tx に変更
	args := m.Called(ctx, tx, data)
	return args.Error(0)
}

func (m *MockWeatherRepository) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockTx は database.Tx インターフェースのモック実装です。
type MockTx struct {
	mock.Mock
}

func (m *MockTx) Commit() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockTx) Rollback() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	args := m.Called(ctx, query)
	// モックの戻り値が *sql.Stmt の場合、nil を返す可能性があるため、安全にキャスト
	if stmt, ok := args.Get(0).(*sql.Stmt); ok {
		return stmt, args.Error(1)
	}
	return nil, args.Error(1)
}
func (m *MockTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	calledArgs := []any{ctx, query}
	calledArgs = append(calledArgs, args...)
	retArgs := m.Called(calledArgs...)
	// Mock の戻り値が sql.Result の場合、nil を返す可能性があるため、型アサーションを安全に行う
	if res, ok := retArgs.Get(0).(sql.Result); ok {
		return res, retArgs.Error(1)
	}
	return nil, retArgs.Error(1) // もし sql.Result でなければ nil を返す
}
func (m *MockTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	calledArgs := []any{ctx, query}
	calledArgs = append(calledArgs, args...)
	retArgs := m.Called(calledArgs...)
	if rows, ok := retArgs.Get(0).(*sql.Rows); ok {
		return rows, retArgs.Error(1)
	}
	return nil, retArgs.Error(1)
}
func (m *MockTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	calledArgs := []any{ctx, query}
	calledArgs = append(calledArgs, args...)
	retArgs := m.Called(calledArgs...)
	// QueryRowContext は *sql.Row のみを返すため、エラーは返さない
	if row, ok := retArgs.Get(0).(*sql.Row); ok {
		return row
	}
	return nil
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
			mockTx := new(MockTx) // MockTx を作成
			// PrepareContext と ExecContext のモックを設定
			mockTx.On("PrepareContext", mock.Anything, mock.Anything).Return(&sql.Stmt{}, nil)
			mockTx.On("ExecContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sql.Result(nil), nil)

			// NewWeatherWriter のシグネチャに合わせて引数を渡す
			dummyConfig := batch_config.NewConfig()
			dummyConfig.Database.Type = "postgres" // ★ 追加: データベースタイプを設定
			dummyJobRepo := &MockJobRepository{} // モックまたはnil
			writer, err := weatherwriter.NewWeatherWriter(dummyConfig, dummyJobRepo, map[string]string{}) // 引数を追加
			assert.NoError(t, err, "NewWeatherWriter should not return an error") // コンストラクタのエラーチェック
			ctx := context.Background()
			
			err = writer.Write(ctx, mockTx, tt.inputItems) // MockTx を渡す

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

			assert.NoError(t, writer.Close(context.Background())) // Close には新しいコンテキストを渡す
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
	mockTx := new(MockTx) // MockTx を作成
	// PrepareContext と ExecContext のモックを設定
	mockTx.On("PrepareContext", mock.Anything, mock.Anything).Return(&sql.Stmt{}, nil)
	mockTx.On("ExecContext", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(sql.Result(nil), nil)

	dummyConfig := batch_config.NewConfig()
	dummyConfig.Database.Type = "postgres" // ★ 追加: データベースタイプを設定
	dummyJobRepo := &MockJobRepository{} // モックまたはnil
	writer, err := weatherwriter.NewWeatherWriter(dummyConfig, dummyJobRepo, map[string]string{}) // 引数を追加
	assert.NoError(t, err, "NewWeatherWriter should not return an error") // コンストラクタのエラーチェック
	// Context を作成し、すぐにキャンセルする
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Context のキャンセルは defer で実行 (Write メソッドのテスト用)

	// Write 呼び出しの直前にキャンセルをトリガー
	cancel()

	sampleData := []any{
		&weather_entity.WeatherDataToStore{Time: time.Now(), WeatherCode: 0, Temperature2M: 10.0, Latitude: 35.0, Longitude: 139.0, CollectedAt: time.Now()},
	}
	
	err = writer.Write(ctx, mockTx, sampleData) // MockTx を渡す
	assert.Error(t, err, "Expected an error due to context cancellation") // エラーが発生したことを確認
	assert.True(t, errors.Is(err, context.Canceled), "Expected error to be context.Canceled") // errors.Is を使用してエラーの型を確認

	assert.NoError(t, writer.Close(context.Background())) // Close には新しいコンテキストを渡す
	mockRepo.AssertExpectations(t)
}

// MockJobRepository は job.JobRepository インターフェースのダミー実装です。
// このテストではリポジトリの永続化機能は使用しないため、メソッドは空で問題ありません。
type MockJobRepository struct{}

func (m *MockJobRepository) SaveJobInstance(ctx context.Context, jobInstance *core.JobInstance) error {
	return nil
}
func (m *MockJobRepository) FindJobInstanceByJobNameAndParameters(ctx context.Context, jobName string, params core.JobParameters) (*core.JobInstance, error) {
	return nil, nil
}
func (m *MockJobRepository) FindJobInstanceByID(ctx context.Context, instanceID string) (*core.JobInstance, error) {
	return nil, nil
}
func (m *MockJobRepository) SaveJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
	return nil
}
func (m *MockJobRepository) UpdateJobExecution(ctx context.Context, jobExecution *core.JobExecution) error {
	return nil
}
func (m *MockJobRepository) FindJobExecutionByID(ctx context.Context, executionID string) (*core.JobExecution, error) {
	return nil, nil
}
func (m *MockJobRepository) Close() error { return nil }
func (m *MockJobRepository) GetJobNames(ctx context.Context) ([]string, error) { return nil, nil }
func (m *MockJobRepository) GetJobInstanceCount(ctx context.Context, jobName string) (int, error) { return 0, nil }
func (m *MockJobRepository) FindLatestJobExecution(ctx context.Context, jobInstanceID string) (*core.JobExecution, error) { return nil, nil }
func (m *MockJobRepository) FindJobExecutionsByJobInstance(ctx context.Context, jobInstance *core.JobInstance) ([]*core.JobExecution, error) { return nil, nil }
func (m *MockJobRepository) SaveStepExecution(ctx context.Context, stepExecution *core.StepExecution) error { return nil }
func (m *MockJobRepository) UpdateStepExecution(ctx context.Context, stepExecution *core.StepExecution) error { return nil }
func (m *MockJobRepository) FindStepExecutionByID(ctx context.Context, executionID string) (*core.StepExecution, error) { return nil, nil }
func (m *MockJobRepository) FindStepExecutionsByJobExecutionID(ctx context.Context, jobExecutionID string) ([]*core.StepExecution, error) { return nil, nil }
func (m *MockJobRepository) GetDBConnection() database.DBConnection { return nil }
