package repository

import (
  "context"
  "fmt"
  "database/sql"

  "sample/src/main/go/batch/config"

  _ "github.com/lib/pq"              // PostgreSQL/Redshift ドライバ
  _ "github.com/go-sql-driver/mysql" // MySQL ドライバ
  //"cloud.google.com/go/bigquery"
)

func NewWeatherRepository(ctx context.Context, cfg config.Config) (WeatherRepository, error) {

  switch cfg.Database.Type {
  case "redshift":
    db, err := sql.Open("postgres", cfg.Database.ConnectionString())
    if err != nil {
      return nil, fmt.Errorf("failed to open redshift connection: %w", err)
    }
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
