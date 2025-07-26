package weather_config

type WeatherReaderConfig struct {
	APIEndpoint string
	APIKey      string
}

// WeatherProcessorConfig は WeatherProcessor に必要な設定のみを持つ構造体です。
// 現時点では設定は不要ですが、将来的に追加される可能性があります。
type WeatherProcessorConfig struct {}

// WeatherWriterConfig は WeatherWriter に必要な設定のみを持つ構造体です。
// 現時点では設定は不要ですが、将来的に追加される可能性があります。
type WeatherWriterConfig struct {}
