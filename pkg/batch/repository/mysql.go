// pkg/batch/repository/mysql.go
package repository

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql" // MySQL ドライバ (適切なドライバ)
	"github.com/tigerroll/go_sample/pkg/batch/config"
	"github.com/tigerroll/go_sample/pkg/batch/util/logger" // logger を直接インポート
	"github.com/tigerroll/go_sample/pkg/batch/util/exception" // exception を直接インポート
)

// MySQLRepository は直接 *sql.DB を保持するのではなく、SQLClient を使用するように変更
// このファイルは、NewMySQLRepositoryFromConfig のような直接DB接続を確立する関数を
// 提供する代わりに、SQLClient を受け取るヘルパー関数を提供する形に変わる。
type MySQLRepository struct {
	client SQLClient // ★ 変更: *sql.DB から SQLClient に変更
}

// NewMySQLRepository は新しい MySQLRepository のインスタンスを作成します。
func NewMySQLRepository(client SQLClient) *MySQLRepository { // ★ 変更: db *sql.DB から client SQLClient に変更
	return &MySQLRepository{client: client}
}

// NewMySQLRepositoryFromConfig はもはや直接DB接続を確立しないため、削除または変更
/*
func NewMySQLRepositoryFromConfig(cfg config.DatabaseConfig) (*MySQLRepository, error) {
	connStr := cfg.ConnectionString()

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, exception.NewBatchError("repository", "MySQL への接続に失敗しました", err, false, false)
	}

	// Ping に Context を渡す場合は db.PingContext を使用
	err = db.Ping()
	if err != nil {
		return nil, exception.NewBatchError("repository", "MySQL への Ping に失敗しました", err, false, false)
	}

	logger.Debugf("MySQL に正常に接続しました。")
	return &MySQLRepository{db: db}, nil
}
*/

// WeatherRepository インターフェースが実装されていることを確認
// この行は、WeatherRepositoryインターフェースがこのファイルと同じパッケージ、
// またはインポート可能なパッケージで定義されていることを前提としています。
// もしWeatherRepositoryが未定義であれば、別途定義が必要です。
/* var _ weather_repo.WeatherRepository = (*MySQLRepository)(nil) */
