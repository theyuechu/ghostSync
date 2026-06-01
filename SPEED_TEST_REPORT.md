# GhostSync 速度测试报告

## 测试环境

| 项目 | 值 |
|------|----|
| 硬件 | MacBook (Apple Silicon) |
| 操作系统 | macOS |
| 源数据库 | MariaDB 11.7 (localhost) — `ghostsync_source` |
| 目标数据库 | MariaDB 11.7 (localhost) — `ghostsync_target` |
| MySQL 配置 | `innodb_buffer_pool_size=128M`, `innodb_flush_log_at_trx_commit=1` |
| 数据表 | `sync_test_users` — 8 列 (id, name, phone, email, password, id_card, created_at) |
| 脱敏规则 | phone → MaskPhone, email → MaskEmail, password → Hash(SHA256), id_card → Ignore |
| 客户端 | Release 编译的单一静态二进制 |

## 优化演进

| 版本 | 数据量 | 耗时 | 行/秒 | 说明 |
|------|--------|------|-------|------|
| **v1 — OFFSET 分页** | 1,000,000 | ~730s (12min) | ~1,370 | 原始版本，OFFSET 越深越慢 |
| **v2 — Keyset 分页** | 1,000,000 | ~65s | ~15,300 | 基于主键的 B-tree 游标分页 |
| **v3 — OUTFILE + LOAD DATA** | 1,000,000 | **~13.7s** | **~73,000** | 同机 MySQL 跳过 TCP，用文件 I/O |

## v3 优化原理

对于同机 MySQL 场景，GhostSync 自动切换为 **SELECT INTO OUTFILE → Rust 处理 → LOAD DATA INFILE** 路径：

```
┌─────────────────────────────────────────────────────────┐
│                     GhostSync 引擎                        │
│                                                           │
│  MySQL 源库                               MySQL 目标库    │
│  ┌──────────────┐                       ┌──────────────┐ │
│  │SELECT INTO   │ → /tmp/raw.csv → Rust → /tmp/out.csv →│LOAD DATA     │
│  │OUTFILE       │     (2.4s)     │规则处理│    (9.3s)  │INFILE        │
│  └──────────────┘                │ (2.0s) │            └──────────────┘
│                                  └────────┘                │
└─────────────────────────────────────────────────────────┘
```

### 耗时分解

| 阶段 | 耗时 | 说明 |
|------|------|------|
| SELECT INTO OUTFILE | **2.4s** | MySQL 直接将表数据写为 CSV 到磁盘 |
| Rust 读CSV + 规则处理 + 写CSV | **2.0s** | 字节流级操作，无 TCP 开销 |
| LOAD DATA INFILE | **9.3s** | MySQL 读取 CSV 并写入 InnoDB |
| **合计** | **13.7s** | **73,000 行/秒** |

### 瓶颈分析

**LOAD DATA INFILE 的 9.3s 是 InnoDB 写入瓶颈**，受限于：
- `innodb_buffer_pool_size = 128MB`
- `innodb_flush_log_at_trx_commit = 1`
- 单线程写入

调整 MySQL 配置后可进一步降低（预计 5-6s 总耗时）。

## 脱敏效果验证

| 字段 | 原始值 | 处理后值 | 规则 |
|------|--------|---------|------|
| phone | `14018176509` | `140****6509` | mask_phone |
| email | `test1@example.com` | `t****@example.com` | mask_email |
| password | `pass_xxxx` | `5e884898da280477...` | hash (SHA-256) |
| id_card | `123456789012345678` | `NULL` | ignore |

数据完整性：100% 匹配，无丢失、无重复。

## 二进制大小

| 模式 | 大小 |
|------|------|
| Debug | 62 MB |
| Release | 20 MB (strip 后约 12 MB) |
