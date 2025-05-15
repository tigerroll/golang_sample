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
// このステップでは、ExecutionContext からデータを読み取って書き込みます。
func (w *WeatherWriter) Write(ctx context.Context, items interface{}) error {
  select {
  case <-ctx.Done():
    return ctx.Err()
  default:
  }

  // ★ ExecutionContext からデータを読み取る ★
  // Write メソッドの引数 items は、このステップ (SaveDataStep) では DummyProcessor から渡されるダミーデータになるため使用しません。
  // 代わりに、JobExecution の ExecutionContext から "processed_weather_data" を取得します。
  // JobExecution は StepExecution を通じてアクセスできますが、Write メソッドは StepExecution を直接受け取らないため、
  // ChunkOrientedStep の Execute メソッド内で JobExecution を Writer に渡すか、
  // Writer が ExecutionContext を直接受け取るように設計を変更する必要があります。
  // ここではシンプル化のため、Write メソッドの引数 items を使用せず、
  // ChunkOrientedStep の Execute メソッドが Writer を呼び出す際に、
  // ExecutionContext から取得したデータを Write メソッドに渡す、という設計とします。
  // ただし、ChunkOrientedStep.writeChunk は items []*entity.WeatherDataToStore を引数に取ります。
  // したがって、ChunkOrientedStep.Execute 内で ExecutionContext からデータを取得し、
  // そのデータを ChunkOrientedStep.writeChunk に渡し、さらにそれが WeatherWriter.Write に渡される、という流れになります。

  // ChunkOrientedStep.writeChunk から渡された items を使用します。
  // この items は、前のステップ (FetchAndProcessStep) が ExecutionContext に保存したデータが、
  // ChunkOrientedStep.Execute 内で ExecutionContext から取得され、writeChunk に渡されたものです。
  dataToStore, ok := items.([]*entity.WeatherDataToStore)
  if !ok {
    // この Writer が SaveDataStep で使用され、DummyProcessor から渡される items はダミーなので、
    // ここに到達することは想定していません。もし到達したらエラーとします。
    // 実際のデータは ExecutionContext から取得されるべきです。
    // 上記のコメントの通り、ChunkOrientedStep.Execute が ExecutionContext からデータを取得し、
    // writeChunk を通じてこの Write メソッドに渡す設計とします。
    // したがって、items は DummyProcessor の出力ではなく、ExecutionContext からのデータです。
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
