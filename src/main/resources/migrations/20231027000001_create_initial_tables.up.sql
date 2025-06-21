-- 20231027000001_create_initial_tables.up.sql
CREATE TABLE IF NOT EXISTS job_instances (
    id VARCHAR(36) PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    job_parameters JSONB,
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS job_executions (
    id VARCHAR(36) PRIMARY KEY,
    job_instance_id VARCHAR(36), -- Foreign key constraint moved to ALTER TABLE
    job_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(20) NOT NULL,
    exit_status VARCHAR(50),
    exit_code INTEGER,
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_updated TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    version INTEGER NOT NULL,
    job_parameters JSONB,
    failure_exceptions JSONB,
    execution_context JSONB,
    current_step_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS step_executions (
    id VARCHAR(36) PRIMARY KEY,
    job_execution_id VARCHAR(36) NOT NULL, -- Foreign key constraint moved to ALTER TABLE
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(20) NOT NULL,
    exit_status VARCHAR(50),
    read_count INTEGER NOT NULL DEFAULT 0,
    write_count INTEGER NOT NULL DEFAULT 0,
    commit_count INTEGER NOT NULL DEFAULT 0,
    rollback_count INTEGER NOT NULL DEFAULT 0,
    failure_exceptions JSONB,
    execution_context JSONB
);

-- Add foreign key constraints after table creation
ALTER TABLE job_executions
ADD CONSTRAINT fk_job_instance FOREIGN KEY (job_instance_id) REFERENCES job_instances(id);

ALTER TABLE step_executions
ADD CONSTRAINT fk_job_execution FOREIGN KEY (job_execution_id) REFERENCES job_executions(id);

-- Add indexes
CREATE INDEX IF NOT EXISTS idx_job_instances_job_name_params ON job_instances (job_name, job_parameters);
CREATE INDEX IF NOT EXISTS idx_job_executions_job_instance_id ON job_executions (job_instance_id);
CREATE INDEX IF NOT EXISTS idx_job_executions_job_name ON job_executions (job_name);
CREATE INDEX IF NOT EXISTS idx_job_executions_create_time ON job_executions (create_time);
CREATE INDEX IF NOT EXISTS idx_step_executions_job_execution_id ON step_executions (job_execution_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_step_name ON step_executions (step_name);
