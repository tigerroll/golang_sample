package component

import (
	config "sample/pkg/batch/config"
	repository "sample/pkg/batch/repository" // repository パッケージをインポート
)

// ComponentBuilder は、特定のコンポーネント（Reader, Processor, Writer, Tasklet）を生成するための関数型です。
// 依存関係 (config, repo, properties など) を受け取り、生成されたコンポーネントのインターフェースとエラーを返します。
// ジェネリックインターフェースを返すため、any を使用します。
type ComponentBuilder func(cfg *config.Config, repo repository.JobRepository, properties map[string]string) (any, error)
