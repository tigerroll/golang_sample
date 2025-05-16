-- バッチメタデータ用SQLスキーマ (PostgreSQL を想定)

-- job_instances テーブル
-- ジョブの論理的な実行単位 (JobInstance) を管理します。
-- 同じ JobParameters で複数回実行された JobExecution は、同じ JobInstance に属します。
CREATE TABLE IF NOT EXISTS job_instances (
    id VARCHAR(36) PRIMARY KEY, -- JobInstance を一意に識別するID (UUIDなどを想定)
    job_name VARCHAR(255) NOT NULL, -- この JobInstance に関連付けられているジョブの名前
    job_parameters JSONB, -- この JobInstance を識別するための JobParameters (JSONBまたはJSON型)
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- JobInstance が作成された時刻
    version INTEGER NOT NULL -- バージョン (楽観的ロックなどに使用)
    -- TODO: JobInstance の状態（完了したか、失敗したかなど）を表すフィールドを追加するか検討
    --       JSR352 では JobInstance 自体は状態を持ちませんが、関連する JobExecution の状態から判断します。
);

-- job_executions テーブル
-- ジョブの単一の実行インスタンス (JobExecution) を管理します。
CREATE TABLE IF NOT EXISTS job_executions (
    id VARCHAR(36) PRIMARY KEY, -- 実行を一意に識別するID (UUIDなどを想定)
    job_instance_id VARCHAR(36) REFERENCES job_instances(id), -- ★ 所属する JobInstance の ID (JobInstance への外部キー)
    job_name VARCHAR(255) NOT NULL, -- この実行に関連付けられているジョブの名前
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- 実行開始時刻
    end_time TIMESTAMP WITHOUT TIME ZONE, -- 実行終了時刻 (NULLABLE)
    status VARCHAR(20) NOT NULL, -- ジョブの現在の状態 (例: STARTED, COMPLETED, FAILED)
    exit_status VARCHAR(50), -- 終了時の詳細なステータス (例: COMPLETED, FAILED, STOPPED) (NULLABLE)
    exit_code INTEGER, -- 終了コード (ここでは単純化のため未使用、NULLABLE)
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- レコード作成時刻
    last_updated TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- レコード最終更新時刻
    version INTEGER NOT NULL, -- バージョン (楽観的ロックなどに使用)
    job_parameters JSONB, -- 実行時の JobParameters (JSONBまたはJSON型)
    failure_exceptions JSONB, -- 発生したエラー (エラーメッセージのリストなど、JSONBまたはJSON型)
    execution_context JSONB, -- ジョブレベルのコンテキスト (JSONBまたはJSON型)
    current_step_name VARCHAR(255), -- ★ 現在実行中のステップ名 (リスタート時に使用) (NULLABLE)
    CONSTRAINT fk_job_instance FOREIGN KEY (job_instance_id) REFERENCES job_instances(id) -- JobInstance への外部キー制約
);

-- step_executions テーブル
-- ステップの単一の実行インスタンス (StepExecution) を管理します。
CREATE TABLE IF NOT EXISTS step_executions (
    id VARCHAR(36) PRIMARY KEY, -- 実行を一意に識別するID (UUIDなどを想定)
    job_execution_id VARCHAR(36) NOT NULL, -- 所属する JobExecution の ID
    step_name VARCHAR(255) NOT NULL, -- この実行に関連付けられているステップの名前
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL, -- 実行開始時刻
    end_time TIMESTAMP WITHOUT TIME ZONE, -- 実行終了時刻 (NULLABLE)
    status VARCHAR(20) NOT NULL, -- ステップの現在の状態 (例: STARTED, COMPLETED, FAILED)
    exit_status VARCHAR(50), -- 終了時の詳細なステータス (例: COMPLETED, FAILED, STOPPED) (NULLABLE)
    read_count INTEGER NOT NULL DEFAULT 0, -- 読み込んだアイテム数
    write_count INTEGER NOT NULL DEFAULT 0, -- 書き込んだアイテム数
    commit_count INTEGER NOT NULL DEFAULT 0, -- コミット回数
    rollback_count INTEGER NOT NULL DEFAULT 0, -- ロールバック回数
    failure_exceptions JSONB, -- 発生したエラー (エラーメッセージのリストなど、JSONBまたはJSON型)
    execution_context JSONB, -- ステップレベルのコンテキスト (JSONBまたはJSON型)
    CONSTRAINT fk_job_execution FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) -- JobExecution への外部キー制約
);

-- インデックスの追加 (パフォーマンス向上のため、必要に応じて追加)
-- JobInstance の検索用インデックス
CREATE INDEX IF NOT EXISTS idx_job_instances_job_name_params ON job_instances (job_name, job_parameters); -- JobName と Parameters で検索する場合

-- JobExecution の検索用インデックス
CREATE INDEX IF NOT EXISTS idx_job_executions_job_instance_id ON job_executions (job_instance_id); -- JobInstance に紐づく実行を検索する場合
CREATE INDEX IF NOT EXISTS idx_job_executions_job_name ON job_executions (job_name); -- ジョブ名で検索する場合
CREATE INDEX IF NOT EXISTS idx_job_executions_create_time ON job_executions (create_time); -- 作成時刻でソートする場合 (最新の実行検索など)

-- StepExecution の検索用インデックス
CREATE INDEX IF NOT EXISTS idx_step_executions_job_execution_id ON step_executions (job_execution_id); -- JobExecution に紐づくステップ実行を検索する場合
CREATE INDEX IF NOT EXISTS idx_step_executions_step_name ON step_executions (step_name); -- ステップ名で検索する場合
