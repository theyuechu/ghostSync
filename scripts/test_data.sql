-- ============================================================
-- GhostSync 速度测试 — 生成测试数据
-- ============================================================
-- 用法：
--   PostgreSQL: psql -U postgres -d test_db -f test_data.sql
--   MySQL:      mysql -u root -D test_db < test_data.sql
--
-- 默认生成 50 万行。改第一行的值即可控制总量。
-- ============================================================

-- ==================== 调这里 ====================
\set row_count 500000
-- ================================================

-- ==================== PostgreSQL ====================
-- 如果不需要两套，注释掉不用的那套

-- ---------- PostgreSQL 建表 ----------
DROP TABLE IF EXISTS sync_test_users;

CREATE TABLE sync_test_users (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100),
    phone       VARCHAR(20),
    email       VARCHAR(200),
    password    VARCHAR(255),
    id_card     VARCHAR(30),
    address     TEXT,
    score       INT DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- 生成随机中文名（不够真？自己改数组）
INSERT INTO sync_test_users (name, phone, email, password, id_card, address, score, created_at)
SELECT
    'user_' || n,
    -- 11 位手机号：1 开头，后 10 位随机
    '1' || LPAD(floor(random() * 10000000000)::text, 10, '0'),
    -- 邮箱
    'test' || n || '@example.com',
    -- 模拟密码
    'pass_' || md5(random()::text),
    -- 模拟身份证号（18 位）
    LPAD(floor(random() * 999999999999999999)::text, 18, '0'),
    -- 地址
    'address_' || n || ' street',
    -- 分数
    floor(random() * 1000)::int,
    -- 时间分布在一周内
    NOW() - (random() * 7 * interval '1 day')
FROM generate_series(1, :row_count) AS s(n);

CREATE INDEX idx_sync_test_users_phone ON sync_test_users(phone);
ANALYZE sync_test_users;

SELECT COUNT(*) AS "PG 行数" FROM sync_test_users;
-- ---------- /PostgreSQL ----------


-- ==================== MySQL ====================

-- ---------- MySQL 建表 ----------
/*
DROP TABLE IF EXISTS sync_test_users;

CREATE TABLE sync_test_users (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100),
    phone       VARCHAR(20),
    email       VARCHAR(200),
    password    VARCHAR(255),
    id_card     VARCHAR(30),
    address     TEXT,
    score       INT DEFAULT 0,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- MySQL 需要存储过程才能批量生成，或者用 Python/GhostSync 自己同步自己
-- 推荐方式：
--   1. 先在 PG 生成
--   2. 用 GhostSync 从 PG 同步到 MySQL 顺便测速

SELECT COUNT(*) AS "MySQL 行数" FROM sync_test_users;
*/
-- ---------- /MySQL ----------


-- ============================================================
-- 推荐 GhostSync 配置（放在 config.yaml 的 tasks 里）：
-- ============================================================
--
-- tasks:
--   - name: speed-test
--     source: source_db
--     target: target_db
--     tables:
--       - name: sync_test_users
--         rules:
--           - field: phone       # 1.3 亿手机号 / 秒（单机 Rust 脱敏）
--             rule: mask_phone
--           - field: email       # 按空格→. 分段掩码
--             rule: mask_email
--           - field: password
--             rule: hash         # SHA-256
--           - field: id_card
--             rule: ignore       # 跳过，不往目标写
--     chunk_size: 10000          # 每块 1 万行
--     batch_size: 2000           # 每批 2000 行 INSERT
--     max_workers: 8             # 8 个 Worker 并发脱敏
--     truncate_target: true      # 每次重跑前清空目标表
--     schedule: ""               # 手动执行
