-- V1__create_batch_tables.sql (MySQL Compatible)

-- PostgreSQL specific extensions and languages are removed for MySQL compatibility.
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- CREATE LANGUAGE plpgsql;

-- job_instances テーブル
-- ジョブのユニークな定義を保持します。job_name と job_key の組み合わせで一意になります。
CREATE TABLE IF NOT EXISTS job_instances (
    id CHAR(36) PRIMARY KEY DEFAULT (UUID()), -- MySQL: UUID を CHAR(36) で保存し、UUID() 関数で生成
    job_name VARCHAR(255) NOT NULL,
    job_key TEXT NOT NULL, -- ジョブパラメータのハッシュなど、ジョブインスタンスを一意に識別するキー
    version INTEGER NOT NULL DEFAULT 0, -- 楽観的ロックのためのバージョン
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- MySQL: TIMESTAMP を使用 (DATETIME との衝突を避けるため)
    last_updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- MySQL: 自動更新
    CONSTRAINT uk_job_instance_name_key UNIQUE (job_name, job_key)
);

-- job_executions テーブル
-- 各ジョブ実行のインスタンスを保持します。
CREATE TABLE IF NOT EXISTS job_executions (
    id CHAR(36) PRIMARY KEY DEFAULT (UUID()),
    job_instance_id CHAR(36) NOT NULL, -- MySQL: UUID を CHAR(36) で保存
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(50) NOT NULL, -- 例: STARTED, COMPLETED, FAILED, ABANDONED
    exit_code VARCHAR(255),
    exit_description TEXT,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- MySQL: TIMESTAMP を使用 (DATETIME との衝突を避けるため)
    last_updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    version INTEGER NOT NULL DEFAULT 0,
    job_parameters TEXT, -- ジョブ実行時のパラメータをJSON文字列などで保存
    FOREIGN KEY (job_instance_id) REFERENCES job_instances(id) ON DELETE CASCADE
);

-- step_executions テーブル
-- 各ステップ実行のインスタンスを保持します。
CREATE TABLE IF NOT EXISTS step_executions (
    id CHAR(36) PRIMARY KEY DEFAULT (UUID()),
    job_execution_id CHAR(36) NOT NULL, -- MySQL: UUID を CHAR(36) で保存
    step_name VARCHAR(255) NOT NULL,
    start_time DATETIME,
    end_time DATETIME,
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
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- MySQL: TIMESTAMP を使用 (DATETIME との衝突を避けるため)
    last_updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) ON DELETE CASCADE
);

-- execution_context テーブル
-- ジョブまたはステップの実行コンテキスト（状態）をキーバリュー形式で保持します。
CREATE TABLE IF NOT EXISTS execution_context (
    id CHAR(36) PRIMARY KEY DEFAULT (UUID()),
    job_execution_id CHAR(36),
    step_execution_id CHAR(36),
    key_name VARCHAR(255) NOT NULL, -- 'key' は予約語の可能性があるため 'key_name' に変更
    value_data TEXT, -- 'value' は予約語の可能性があるため 'value_data' に変更
    value_type VARCHAR(50) NOT NULL, -- 例: STRING, INT, FLOAT, BOOL, JSON
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- MySQL: TIMESTAMP を使用 (DATETIME との衝突を避けるため)
    last_updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    -- ジョブまたはステップのどちらか一方に紐づくことを保証 (MySQL 8.0.16+ でサポート)
    CONSTRAINT chk_job_or_step_execution CHECK (
        (job_execution_id IS NOT NULL AND step_execution_id IS NULL) OR
        (job_execution_id IS NULL AND step_execution_id IS NOT NULL)
    ),
    FOREIGN KEY (job_execution_id) REFERENCES job_executions(id) ON DELETE CASCADE,
    FOREIGN KEY (step_execution_id) REFERENCES step_executions(id) ON DELETE CASCADE
);

-- 各スコープ内でキーが一意であることを保証 (MySQLでは部分ユニークインデックスは直接サポートされない)
-- MySQLのUNIQUE INDEXはNULL値を複数許容するため、以下の定義でPostgreSQLの部分インデックスと同様の効果が得られる
-- (job_execution_id が NULL でない場合に (job_execution_id, key_name) の組み合わせが一意であることを保証)
CREATE UNIQUE INDEX uk_job_context_partial ON execution_context (job_execution_id, key_name);
-- (step_execution_id が NULL でない場合に (step_execution_id, key_name) の組み合わせが一意であることを保証)
CREATE UNIQUE INDEX uk_step_context_partial ON execution_context (step_execution_id, key_name);


-- インデックスの追加 (パフォーマンス向上のため)
CREATE INDEX idx_job_executions_job_instance_id ON job_executions (job_instance_id);
CREATE INDEX idx_step_executions_job_execution_id ON step_executions (job_execution_id);

-- 更新時刻を自動更新するトリガーは、MySQL の ON UPDATE CURRENT_TIMESTAMP で代替されるため不要。
-- PostgreSQL のトリガー関数とトリガー定義は削除。
