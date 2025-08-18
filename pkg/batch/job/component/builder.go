package component

import (
	config "sample/pkg/batch/config"
	"sample/pkg/batch/repository/job" // job リポジトリインターフェースをインポート
)

// ComponentBuilder は、特定のコンポーネント（Reader, Processor, Writer, Tasklet）を生成するための関数型です。
// 依存関係 (config, repo, properties など) を受け取り、生成されたコンポーネントのインターフェースとエラーを返します。
// ジェネリックインターフェースを返すため、any を使用します。
type ComponentBuilder func(cfg *config.Config, repo job.JobRepository, properties map[string]string) (any, error) // repo の型を job.JobRepository に変更
