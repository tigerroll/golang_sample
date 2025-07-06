-- create_batch_tables.sql (PostgreSQL Compatible)

-- UUID生成のための拡張機能を有効化
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 関数: last_updated_time を自動更新するためのトリガー関数
CREATE OR REPLACE FUNCTION update_last_updated_time()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated_time = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- job_instances テーブル
-- ジョブのユニークな定義を保持します。job_name と job_key の組み合わせで一意になります。
CREATE TABLE IF NOT EXISTS job_instances (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), -- PostgreSQL: UUID 型と uuid_generate_v4() 関数で生成
    job_name VARCHAR(255) NOT NULL,
    job_key TEXT NOT NULL, -- ジョブパラメータのハッシュなど、ジョブインスタンスを一意に識別するキー
    version INTEGER NOT NULL DEFAULT 0, -- 楽観的ロックのためのバージョン
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_time TIMESTAMP NOT NULL, -- DEFAULT CURRENT_TIMESTAMP を削除
    CONSTRAINT uk_job_instance_name_key UNIQUE (job_name, job_key)
);

-- job_instances テーブルの last_updated_time を自動更新するトリガー
CREATE TRIGGER set_job_instances_last_updated_time
BEFORE INSERT OR UPDATE ON job_instances -- INSERT 時もトリガーを発火させる
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- job_executions テーブル
-- 各ジョブ実行のインスタンスを保持します。
CREATE TABLE IF NOT EXISTS job_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_instance_id UUID NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL, -- 例: STARTED, COMPLETED, FAILED, ABANDONED
    exit_code VARCHAR(255),
    exit_description TEXT,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_time TIMESTAMP NOT NULL, -- DEFAULT CURRENT_TIMESTAMP を削除
    version INTEGER NOT NULL DEFAULT 0,
    job_parameters TEXT, -- ジョブ実行時のパラメータをJSON文字列などで保存
    FOREIGN KEY (job_instance_id) REFERENCES job_instances(id) ON DELETE CASCADE
);

-- job_executions テーブルの last_updated_time を自動更新するトリガー
CREATE TRIGGER set_job_executions_last_updated_time
BEFORE INSERT OR UPDATE ON job_executions -- INSERT 時もトリガーを発火させる
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- step_executions テーブル
-- 各ステップ実行のインスタンスを保持します。
CREATE TABLE IF NOT EXISTS step_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_execution_id UUID NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
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
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_time TIMESTAMP NOT NULL, -- DEFAULT CURRENT_TIMESTAMP を削除
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) ON DELETE CASCADE
);

-- step_executions テーブルの last_updated_time を自動更新するトリガー
CREATE TRIGGER set_step_executions_last_updated_time
BEFORE INSERT OR UPDATE ON step_executions -- INSERT 時もトリガーを発火させる
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- execution_context テーブル
-- ジョブまたはステップの実行コンテキスト（状態）をキーバリュー形式で保持します。
CREATE TABLE IF NOT EXISTS execution_context (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_execution_id UUID,
    step_execution_id UUID,
    key_name VARCHAR(255) NOT NULL,
    value_data TEXT,
    value_type VARCHAR(50) NOT NULL, -- 例: STRING, INT, FLOAT, BOOL, JSON
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_time TIMESTAMP NOT NULL, -- DEFAULT CURRENT_TIMESTAMP を削除
    -- ジョブまたはステップのどちらか一方に紐づくことを保証
    CONSTRAINT chk_job_or_step_execution CHECK (
        (job_execution_id IS NOT NULL AND step_execution_id IS NULL) OR
        (job_execution_id IS NULL AND step_execution_id IS NOT NULL)
    ),
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) ON DELETE CASCADE,
    FOREIGN KEY (step_execution_id) REFERENCES step_executions(id) ON DELETE CASCADE
);

-- execution_context テーブルの last_updated_time を自動更新するトリガー
CREATE TRIGGER set_execution_context_last_updated_time
BEFORE INSERT OR UPDATE ON execution_context -- INSERT 時もトリガーを発火させる
FOR EACH ROW
EXECUTE FUNCTION update_last_updated_time();

-- 各スコープ内でキーが一意であることを保証 (PostgreSQL の部分インデックス)
CREATE UNIQUE INDEX uk_job_context_partial ON execution_context (job_execution_id, key_name) WHERE job_execution_id IS NOT NULL;
CREATE UNIQUE INDEX uk_step_context_partial ON execution_context (step_execution_id, key_name) WHERE step_execution_id IS NOT NULL;

-- インデックスの追加 (パフォーマンス向上のため)
CREATE INDEX idx_job_executions_job_instance_id ON job_executions (job_instance_id);
CREATE INDEX idx_step_executions_job_execution_id ON step_executions (job_execution_id);
