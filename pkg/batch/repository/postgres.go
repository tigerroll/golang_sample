// pkg/batch/repository/postgres.go
package repository

import (
	"database/sql"

	_ "github.com/lib/pq" // PostgreSQL ドライバ (適切なドライバ)
	"github.com/tigerroll/go_sample/pkg/batch/config"
	"github.com/tigerroll/go_sample/pkg/batch/util/exception" // exception を直接インポート
	"github.com/tigerroll/go_sample/pkg/batch/util/logger"    // logger を直接インポート
)

// PostgresRepository は直接 *sql.DB を保持するのではなく、SQLClient を使用するように変更
// このファイルは、NewPostgresRepositoryFromConfig のような直接DB接続を確立する関数を
// 提供する代わりに、SQLClient を受け取るヘルパー関数を提供する形に変わる。
// ただし、JobRepositoryFactory が SQLClient を生成するため、このファイルはシンプルに
// SQLClient をラップする形にする。
type PostgresRepository struct {
	client SQLClient // ★ 変更: *sql.DB から SQLClient に変更
}

// NewPostgresRepository は新しい PostgresRepository のインスタンスを作成します。
func NewPostgresRepository(client SQLClient) *PostgresRepository { // ★ 変更: db *sql.DB から client SQLClient に変更
	return &PostgresRepository{client: client}
}

// NewPostgresRepositoryFromConfig はもはや直接DB接続を確立しないため、削除または変更
// この関数は initializer.go の connectWithRetry と NewSQLClient に置き換えられる
/*
func NewPostgresRepositoryFromConfig(cfg config.DatabaseConfig) (*PostgresRepository, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, exception.NewBatchError("repository", "PostgreSQL への接続に失敗しました", err, false, false)
	}

	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "PostgreSQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("PostgreSQL に正常に接続しました。")
	return &PostgresRepository{db: db}, nil
}
*/

// WeatherRepository インターフェースが実装されていることを確認
// この行は、WeatherRepositoryインターフェースがこのファイルと同じパッケージ、
// またはインポート可能なパッケージで定義されていることを前提としています。
// もしWeatherRepositoryが未定義であれば、別途定義が必要です。
/* var _ weather_repo.WeatherRepository = (*PostgresRepository)(nil) */
