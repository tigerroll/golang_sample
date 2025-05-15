package repository

import (
  "context"
  "database/sql"
  "fmt"

  "sample/src/main/go/batch/config"

  _ "github.com/lib/pq"              // PostgreSQL/Redshift ドライバ
  _ "github.com/go-sql-driver/mysql" // MySQL ドライバ
  // "cloud.google.com/go/bigquery"
)

// NewWeatherRepository は WeatherRepository を生成する既存の関数です。
// ... (既存の NewWeatherRepository 関数は省略せずそのまま残してください) ...
func NewWeatherRepository(ctx context.Context, cfg config.Config) (WeatherRepository, error) {

  switch cfg.Database.Type {
  case "redshift":
    db, err := sql.Open("postgres", cfg.Database.ConnectionString())
    if err != nil {
      return nil, fmt.Errorf("failed to open redshift connection: %w", err)
    }
    // NewRedshiftRepository を正しく呼び出し
    return NewRedshiftRepository(db), nil
  case "postgres":
    db, err := sql.Open("postgres", cfg.Database.ConnectionString())
    if err != nil {
      return nil, fmt.Errorf("failed to open postgres connection: %w", err)
    }
    return NewPostgresRepository(db), nil
  case "mysql":
    db, err := sql.Open("mysql", cfg.Database.ConnectionString())
    if err != nil {
      return nil, fmt.Errorf("failed to open mysql connection: %w", err)
    }
    return NewMySQLRepository(db), nil
  //case "bigquery":
  //  bqClient, err := bigquery.NewClient(ctx, cfg.Database.ProjectID)
  //  if err != nil {
  //    return nil, fmt.Errorf("failed to create bigquery client: %w", err)
  //  }
  //  bqConfig := config.BigQueryConfig{
  //    ProjectID: cfg.Database.ProjectID,
  //    DatasetID: cfg.Database.DatasetID,
  //    TableID:   cfg.Database.TableID,
  //  }
  //  return NewBigQueryRepository(bqClient, bqConfig), nil
  default:
    return nil, fmt.Errorf("unsupported database type: %s", cfg.Database.Type)
  }
}

// NewJobRepository は JobRepository のインスタンスを作成します。
// アプリケーションの設定を基にデータベース接続を確立します。
func NewJobRepository(ctx context.Context, cfg config.Config) (JobRepository, error) {
  // ここでデータベース接続を確立します。
  // 既存の DatabaseConfig と ConnectionString() メソッドを利用します。
  db, err := sql.Open(cfg.Database.Type, cfg.Database.ConnectionString()) // ドライバ名は config.Database.Type をそのまま使用
  if err != nil {
    return nil, fmt.Errorf("failed to open database connection for JobRepository: %w", err)
  }

  // データベースへの疎通確認 (Ping)
  if err = db.PingContext(ctx); err != nil {
    db.Close() // Ping に失敗したら接続を閉じる
    return nil, fmt.Errorf("failed to ping database for JobRepository: %w", err)
  }

  // SQLJobRepository の新しいインスタンスを作成し、確立した接続を渡します。
  // TODO: 他のデータベースタイプ (MySQL, BigQueryなど) に対応するための分岐を追加する必要があります。
  //       現時点では PostgreSQL/Redshift 互換を想定した SQLJobRepository を返します。
  //       厳密にはデータベースタイプに応じた JobRepository 実装を選択するロジックが必要です。
  //       例: switch cfg.Database.Type { ... case "postgres": return NewSQLJobRepository(db), nil ... }

  // Simplification: 一旦 SQLJobRepository を返す前提で進めます。
  // 実際のプロダクションコードでは、データベースタイプごとに異なる JobRepository 実装を用意し、
  // ここで適切な実装を選択する必要があります。
  return NewSQLJobRepository(db), nil
}

// TODO: Job Repository が使用するデータベース接続を閉じるための関数や、
//       アプリケーション終了時に適切に Close() が呼び出される仕組みが必要です。
//       main 関数で JobRepository を生成した後、defer で Close() を呼び出すなどが考えられます。
