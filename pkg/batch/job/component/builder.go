package component

import (
	"database/sql"
	config "sample/pkg/batch/config"
)

// ComponentBuilder は、特定のコンポーネント（Reader, Processor, Writer, Tasklet）を生成するための関数型です。
// 依存関係 (config, db, properties など) を受け取り、生成されたコンポーネントのインターフェースとエラーを返します。
// ジェネリックインターフェースを返すため、any を使用します。
type ComponentBuilder func(cfg *config.Config, db *sql.DB, properties map[string]string) (any, error)
