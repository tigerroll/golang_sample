-- V1__create_batch_tables.sql

-- job_instances テーブル
-- ジョブのユニークな定義を保持します。job_name と job_key の組み合わせで一意になります。
CREATE TABLE IF NOT EXISTS job_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- UUID を主キーとして使用
    job_name VARCHAR(255) NOT NULL,
    job_key TEXT NOT NULL, -- ジョブパラメータのハッシュなど、ジョブインスタンスを一意に識別するキー
    version INTEGER NOT NULL DEFAULT 0, -- 楽観的ロックのためのバージョン
    create_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_updated_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uk_job_instance_name_key UNIQUE (job_name, job_key)
);

-- job_executions テーブル
-- 各ジョブ実行のインスタンスを保持します。
CREATE TABLE IF NOT EXISTS job_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_instance_id UUID NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL, -- 例: STARTED, COMPLETED, FAILED, ABANDONED
    exit_code VARCHAR(255),
    exit_description TEXT,
    create_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_updated_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    version INTEGER NOT NULL DEFAULT 0,
    job_parameters TEXT, -- ジョブ実行時のパラメータをJSON文字列などで保存
    FOREIGN KEY (job_instance_id) REFERENCES job_instances(id) ON DELETE CASCADE
);

-- step_executions テーブル
-- 各ステップ実行のインスタンスを保持します。
CREATE TABLE IF NOT EXISTS step_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_execution_id UUID NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL, -- 例: STARTED, COMPLETED, FAILED, SKIPPED
    exit_code VARCHAR(255),
    exit_description TEXT,
    read_count INTEGER NOT NULL DEFAULT 0,
    write_count INTEGER NOT NULL DEFAULT 0,
    commit_count INTEGER NOT NULL DEFAULT 0,
    rollback_count INTEGER NOT NULL DEFAULT 0,
    read_skip_count INTEGER NOT NULL DEFAULT 0,
    process_skip_count INTEGER NOT NULL DEFAULT 0,
    write_skip_count INTEGER NOT NULL DEFAULT 0,
    filter_count INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 0,
    create_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_updated_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) ON DELETE CASCADE
);

-- execution_context テーブル
-- ジョブまたはステップの実行コンテキスト（状態）をキーバリュー形式で保持します。
CREATE TABLE IF NOT EXISTS execution_context (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_execution_id UUID,
    step_execution_id UUID,
    key_name VARCHAR(255) NOT NULL, -- 'key' は予約語の可能性があるため 'key_name' に変更
    value_data TEXT, -- 'value' は予約語の可能性があるため 'value_data' に変更
    value_type VARCHAR(50) NOT NULL, -- 例: STRING, INT, FLOAT, BOOL, JSON
    create_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_updated_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    -- ジョブまたはステップのどちらか一方に紐づくことを保証
    CONSTRAINT chk_job_or_step_execution CHECK (
        (job_execution_id IS NOT NULL AND step_execution_id IS NULL) OR
        (job_execution_id IS NULL AND step_execution_id IS NOT NULL)
    ),
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) ON DELETE CASCADE,
    FOREIGN KEY (step_execution_id) REFERENCES step_executions(id) ON DELETE CASCADE
);

-- 各スコープ内でキーが一意であることを保証 (部分ユニークインデックス)
-- job_execution_id が NULL でない場合に (job_execution_id, key_name) の組み合わせが一意であることを保証
CREATE UNIQUE INDEX IF NOT EXISTS uk_job_context_partial ON execution_context (job_execution_id, key_name) WHERE job_execution_id IS NOT NULL;
-- step_execution_id が NULL でない場合に (step_execution_id, key_name) の組み合わせが一意であることを保証
CREATE UNIQUE INDEX IF NOT EXISTS uk_step_context_partial ON execution_context (step_execution_id, key_name) WHERE step_execution_id IS NOT NULL;


-- インデックスの追加 (パフォーマンス向上のため)
CREATE INDEX IF NOT EXISTS idx_job_executions_job_instance_id ON job_executions (job_instance_id);
CREATE INDEX IF NOT EXISTS idx_step_executions_job_execution_id ON step_executions (job_execution_id);
-- execution_context のインデックスは部分ユニークインデックスでカバーされるため、一般的なインデックスは不要な場合が多い
-- 必要であれば追加
-- CREATE INDEX IF NOT EXISTS idx_execution_context_job_execution_id ON execution_context (job_execution_id);
-- CREATE INDEX IF NOT EXISTS idx_execution_context_step_execution_id ON execution_context (step_execution_id);

-- 更新時刻を自動更新するトリガー (PostgreSQLの場合)
-- job_instances
CREATE OR REPLACE FUNCTION update_last_updated_time()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated_time = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_job_instances_last_updated_time
BEFORE UPDATE ON job_instances
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- job_executions
CREATE TRIGGER update_job_executions_last_updated_time
BEFORE UPDATE ON job_executions
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- step_executions
CREATE TRIGGER update_step_executions_last_updated_time
BEFORE UPDATE ON step_executions
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- execution_context
CREATE TRIGGER update_execution_context_last_updated_time
BEFORE UPDATE ON execution_context
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();
