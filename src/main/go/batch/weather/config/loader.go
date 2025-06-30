// src/main/go/batch/weather/config/loader.go
package weather_config

import (
	"embed" // embed パッケージをインポート
	"fmt"

	batch_config "sample/src/main/go/batch/config" // 汎用 config パッケージをインポート
)

//go:embed ../resources/application.yaml
var embeddedConfig []byte // application.yaml の内容をバイトスライスとして埋め込む

// WeatherConfigLoader は weather アプリケーション固有の ConfigLoader 実装です。
// 埋め込みファイルから設定をロードします。
type WeatherConfigLoader struct{}

// NewWeatherConfigLoader は新しい WeatherConfigLoader のインスタンスを作成します。
func NewWeatherConfigLoader() *WeatherConfigLoader {
	return &WeatherConfigLoader{}
}

// Load は埋め込まれた application.yaml から設定をロードします。
func (l *WeatherConfigLoader) Load() (*batch_config.Config, error) {
	// 汎用的な BytesConfigLoader を使用して設定をロード
	bytesLoader := batch_config.NewBytesConfigLoader(embeddedConfig)
	cfg, err := bytesLoader.Load()
	if err != nil {
		return nil, fmt.Errorf("weather config のロードに失敗しました: %w", err)
	}
	return cfg, nil
}

// WeatherConfigLoader が batch_config.ConfigLoader インターフェースを満たすことを確認
var _ batch_config.ConfigLoader = (*WeatherConfigLoader)(nil)
