# GhostSync 速度测试报告

## 测试环境

| 项目 | 值 |
|------|----|
| 数据库 | MariaDB 11.7.2 (MySQL 协议兼容) |
| GhostSync 版本 | v0.1.0 (Rust, sqlx AnyPool) |
| 测试表 | `sync_test_users` — 8 列 (id, name, phone, email, password, id_card, created_at) |
| 脱敏规则 | `mask_phone`, `mask_email`, `hash`(SHA-256), `ignore`(id_card) |
| 配置参数 | chunk=50,000, batch=50,000 |
| 网络 | localhost (同机，文件 I/O) |

## 速度测试结果

| 数据量 | 引擎模式 | 耗时 | 吞吐量 |
|--------|---------|------|--------|
| 1,000,000 行 | Release — OUTFILE+Rust+LOAD DATA | **13.7s** | **73,000 行/秒** |
| 1,000,000 行 | Release — Keyset+INSERT (旧版) | ~65s | ~15,300 行/秒 |
| 1,000,000 行 | Release — OFFSET (旧版) | ~730s | ~1,370 行/秒 |

## 引擎路径选择

GhostSync 自动判断采用哪种同步路径：

| 场景 | 使用的路径 | 说明 |
|------|-----------|------|
| 同机 MySQL → MySQL | SELECT INTO OUTFILE → Rust → LOAD DATA | 跳过 TCP，直接文件 I/O |
| 跨机 MySQL → MySQL | INSERT + multi-row VALUES | 通过 TCP |
| MySQL → PostgreSQL | INSERT + multi-row VALUES | 跨引擎 |
| PostgreSQL → 任何 | INSERT + multi-row VALUES | PG 不支持 OUTFILE |

## 进度快照（完整测试）

### Oracle 基准

- 源表 1,000,000 行，无二级索引
- 配置：`chunk_size: 50000`, `batch_size: 50000`
- Release build, strip

### v3 优化结果

```
SELECT INTO OUTFILE   → 2.4s
Rust 处理 (SHA256)    → 2.0s
LOAD DATA INFILE      → 9.3s
─────────────────────────────
合计                   → 13.7s  (73,000 rows/s)
```
