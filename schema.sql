-- job_executions テーブル
CREATE TABLE IF NOT EXISTS job_executions (
    id VARCHAR(255) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(50) NOT NULL,
    exit_status VARCHAR(50),
    exit_code INTEGER,
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    version INTEGER NOT NULL,
    job_parameters JSONB,
    failure_exceptions JSONB,
    execution_context JSONB
);

-- step_executions テーブル
CREATE TABLE IF NOT EXISTS step_executions (
    id VARCHAR(255) PRIMARY KEY,
    job_execution_id VARCHAR(255) NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_time TIMESTAMP WITHOUT TIME ZONE, -- NULLABLE
    status VARCHAR(50) NOT NULL,
    exit_status VARCHAR(50), -- NULLABLE
    read_count INTEGER NOT NULL DEFAULT 0,
    write_count INTEGER NOT NULL DEFAULT 0,
    commit_count INTEGER NOT NULL DEFAULT 0,
    rollback_count INTEGER NOT NULL DEFAULT 0,
    failure_exceptions JSONB,
    execution_context JSONB
);

-- インデックスの追加 (パフォーマンス向上のため、必要に応じて追加)
CREATE INDEX IF NOT EXISTS idx_job_executions_job_name ON job_executions (job_name);
CREATE INDEX IF NOT EXISTS idx_job_executions_create_time ON job_executions (create_time);
CREATE INDEX IF NOT EXISTS idx_step_executions_job_execution_id ON step_executions (job_execution_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_step_name ON step_executions (step_name);
