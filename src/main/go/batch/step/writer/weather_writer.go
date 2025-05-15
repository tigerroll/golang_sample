package writer

import (
  "context"
  "fmt"
  "time"

  entity      "sample/src/main/go/batch/domain/entity"
  repository  "sample/src/main/go/batch/repository"
  // core "sample/src/main/go/batch/job/core" // core パッケージは使用されていないため削除
  logger "sample/src/main/go/batch/util/logger" // logger パッケージをインポート
)

type WeatherWriter struct {
  repo repository.WeatherRepository
  // config *config.WeatherWriterConfig // 必要に応じて追加
}

// NewWeatherWriter が Repository を受け取るように修正 (ここでは修正なし)
func NewWeatherWriter(repo repository.WeatherRepository) *WeatherWriter {
  return &WeatherWriter{
    repo: repo,
    // config: cfg, // 必要に応じて初期化
  }
}

// Write メソッドが Writer インターフェースを満たすように修正
// このステップでは、ChunkOrientedStep から渡されたアイテムを書き込みます。
func (w *WeatherWriter) Write(ctx context.Context, items interface{}) error {
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }

  // ChunkOrientedStep.Execute (SaveDataStep の場合) から渡された items を使用します。
  // この items は ExecutionContext から取得されたデータです。
  dataToStore, ok := items.([]*entity.WeatherDataToStore)
  if !ok {
    // 予期しない型の場合
    return fmt.Errorf("Writer: 予期しない入力型です: %T, 期待される型: []*entity.WeatherDataToStore", items)
  }

  if len(dataToStore) == 0 {
    logger.Debugf("WeatherWriter: 書き込むデータがありません。")
    return nil // 書き込むデータがない場合は何もしない
  }

  logger.Infof("WeatherWriter: %d 件のアイテムをリポジトリに書き込みます。", len(dataToStore))

  // リポジトリを使用してデータを保存
  // SaveWeatherData は単一の OpenMeteoForecast を受け取るため、アイテムをループして個別に保存します。
  // バッチ保存が必要な場合は Repository のメソッドを変更する必要があります。
  for _, item := range dataToStore {
    select {
    case <-ctx.Done():
      return ctx.Err()
    default:
    }

    // WeatherDataToStore から OpenMeteoForecast 形式に変換して保存
    // Repository は OpenMeteoForecast 形式を期待しているため
    forecast := entity.OpenMeteoForecast{
      Latitude:  item.Latitude,
      Longitude: item.Longitude,
      Hourly: entity.Hourly{
        Time:          []string{item.Time.Format(time.RFC3339)}, // time.Time から string に変換
        WeatherCode:   []int{item.WeatherCode},
        Temperature2M: []float64{item.Temperature2M},
      },
    }
    // リポジトリのメソッドに Context を渡す
    if err := w.repo.SaveWeatherData(ctx, forecast); err != nil {
      // 個別アイテムの保存エラーは、Writer エラーとして返す
      return fmt.Errorf("データの保存に失敗しました (アイテム: %+v): %w", item, err)
    }
  }

  logger.Infof("WeatherWriter: %d 件のアイテムをリポジトリに書き込み完了しました。", len(dataToStore))
  return nil
}

// WeatherWriter が Writer インターフェースを満たすことを確認
var _ Writer = (*WeatherWriter)(nil)
