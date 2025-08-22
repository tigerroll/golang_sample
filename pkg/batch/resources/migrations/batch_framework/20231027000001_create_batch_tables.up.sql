-- 20231027000001_create_batch_tables.up.sql
CREATE TABLE IF NOT EXISTS job_instances (
    id VARCHAR(36) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    job_parameters JSONB, -- PostgreSQL/Redshift specific, use JSON for MySQL
    create_time TIMESTAMP NOT NULL,
    version INTEGER NOT NULL,
    parameters_hash VARCHAR(64) NOT NULL DEFAULT '' -- ★ 追加: JobParameters のハッシュ値
);

CREATE INDEX IF NOT EXISTS idx_job_instances_job_name ON job_instances (job_name);
CREATE UNIQUE INDEX IF NOT EXISTS uk_job_instances_job_name_params_hash ON job_instances (job_name, parameters_hash); -- ★ 追加: ユニークインデックス

CREATE TABLE IF NOT EXISTS job_executions (
    id VARCHAR(36) PRIMARY KEY,
    job_instance_id VARCHAR(36) NOT NULL,
    job_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    exit_status VARCHAR(50) NOT NULL,
    exit_code INTEGER,
    create_time TIMESTAMP NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    version INTEGER NOT NULL,
    job_parameters JSONB, -- PostgreSQL/Redshift specific, use JSON for MySQL
    failure_exceptions JSONB, -- PostgreSQL/Redshift specific, use JSON for MySQL
    execution_context JSONB, -- PostgreSQL/Redshift specific, use JSON for MySQL
    current_step_name VARCHAR(255),
    FOREIGN KEY (job_instance_id) REFERENCES job_instances(id)
);

CREATE INDEX IF NOT EXISTS idx_job_executions_job_instance_id ON job_executions (job_instance_id);
CREATE INDEX IF NOT EXISTS idx_job_executions_job_name ON job_executions (job_name);

CREATE TABLE IF NOT EXISTS step_executions (
    id VARCHAR(36) PRIMARY KEY,
    job_execution_id VARCHAR(36) NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    exit_status VARCHAR(50) NOT NULL,
    read_count INTEGER NOT NULL DEFAULT 0,
    write_count INTEGER NOT NULL DEFAULT 0,
    commit_count INTEGER NOT NULL DEFAULT 0,
    rollback_count INTEGER NOT NULL DEFAULT 0,
    filter_count INTEGER NOT NULL DEFAULT 0,
    skip_read_count INTEGER NOT NULL DEFAULT 0,
    skip_process_count INTEGER NOT NULL DEFAULT 0,
    skip_write_count INTEGER NOT NULL DEFAULT 0,
    failure_exceptions JSONB, -- PostgreSQL/Redshift specific, use JSON for MySQL
    execution_context JSONB, -- PostgreSQL/Redshift specific, use JSON for MySQL
    last_updated TIMESTAMP NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id)
);

CREATE INDEX IF NOT EXISTS idx_step_executions_job_execution_id ON step_executions (job_execution_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_step_name ON step_executions (step_name);
