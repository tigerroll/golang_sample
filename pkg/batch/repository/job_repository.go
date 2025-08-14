package repository

import (
	"sample/pkg/batch/database" // database パッケージをインポート
)

// JobRepository はバッチ実行に関するメタデータを永続化・管理するためのインターフェースです。
// Spring Batch の JobRepository に相当します。
// 複数のより小さなリポジトリインターフェースを埋め込むことで、責務を分割します。
type JobRepository interface {
	JobInstance
	JobExecution
	StepExecution

	// TODO: CheckpointData の永続化・復元に関するメソッドもここに追加

	// Close はリポジトリが使用するリソース (データベース接続など) を解放します。
	Close() error

	// GetDBConnection は、このリポジトリが使用するデータベース接続の抽象化を返します。
	// これは、ステップ内でトランザクションを開始するために使用されます。
	GetDBConnection() database.DBConnection
}
