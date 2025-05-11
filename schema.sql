-- schema.sql

-- job_executions テーブル
CREATE TABLE IF NOT EXISTS job_executions (
    id VARCHAR(36) PRIMARY KEY, -- JobExecution ID (UUIDなどを想定)
    job_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL, -- 例: STARTING, STARTED, COMPLETED, FAILED
    exit_status VARCHAR(255),   -- 例: COMPLETED, FAILED, STOPPED
    exit_code INTEGER,          -- オプション
    create_time TIMESTAMP WITH TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL,
    version INTEGER,            -- オプション: 楽観的ロックなどに使用
    job_parameters TEXT,        -- ジョブパラメータをJSONや他の形式で保存
    failure_exceptions TEXT     -- 失敗例外をJSONや他の形式で保存
    -- execution_context TEXT   -- Job Execution Context をJSONなどで保存
);

-- step_executions テーブル
CREATE TABLE IF NOT EXISTS step_executions (
    id VARCHAR(36) PRIMARY KEY, -- StepExecution ID (UUIDなどを想定)
    job_execution_id VARCHAR(36) NOT NULL REFERENCES job_executions(id), -- 所属する JobExecution への外部キー
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL, -- 例: STARTING, STARTED, COMPLETED, FAILED
    exit_status VARCHAR(255),   -- 例: COMPLETED, FAILED
    read_count INTEGER,
    write_count INTEGER,
    commit_count INTEGER,
    rollback_count INTEGER,
    failure_exceptions TEXT    -- 失敗例外をJSONや他の形式で保存
    -- execution_context TEXT   -- Step Execution Context をJSONなどで保存
);

-- インデックスの追加 (パフォーマンスのため)
CREATE INDEX IF NOT EXISTS idx_job_executions_job_name ON job_executions (job_name);
CREATE INDEX IF NOT EXISTS idx_step_executions_job_execution_id ON step_executions (job_execution_id);
