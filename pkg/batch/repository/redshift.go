// pkg/batch/repository/redshift.go
package repository

import (
	"database/sql"
)

// RedshiftRepository 型を定義
type RedshiftRepository struct {
	db *sql.DB
}

// NewRedshiftRepository 関数を定義
func NewRedshiftRepository(db *sql.DB) *RedshiftRepository {
	return &RedshiftRepository{db: db}
}
