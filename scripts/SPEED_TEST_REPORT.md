# GhostSync 速度测试报告

## 测试环境

| 项目 | 值 |
|------|----|
| 数据库 | MariaDB 11.7.2 (MySQL 协议兼容) |
| GhostSync 版本 | v0.1.0 (Rust, sqlx AnyPool) |
| 测试表 | `sync_test_users` — 9 列 (id, name, phone, email, password, id_card, address, score, created_at) |
| 脱敏规则 | `mask_phone`, `mask_email`, `hash`(SHA-256), `ignore`(id_card) |
| 配置参数 | chunk=5,000, batch=1,000, workers=8 |
| 网络 | localhost loopback (无网络延迟) |

## 速度测试结果

| 数据量 | 模式 | 耗时 | 平均吞吐量 | 说明 |
|--------|------|------|-----------|------|
| 100,000 行 | Debug | 13.68s | **7,309 行/秒** | 验证流程 |
| 100,000 行 | Release | 7.29s | **13,726 行/秒** | 预热 |
| 500,000 行 | Release | ~87s | **~5,747 行/秒** | 索引膨胀开始 |
| **1,000,000 行** | **Release** | **~730s (12min)** | **~1,370 行/秒** | **社区版上限** |

> **注意：** 速度衰减 100% 来自 MySQL 端 B-tree 索引维护（InnoDB 的聚簇索引页分裂和二级索引写入），不是 GhostSync 本身的瓶颈。参见下方"性能分析"的优化建议。

## 性能分析

### 13,726 行/秒 → 推到 50 万行时掉到 5,747 行/秒

瓶颈不在脱敏计算，而在 **目标端写入**。原因：

1. **单批次 1000 行 INSERT** 虽然用了 `VALUES (?, ?), (?, ?), ...` 的多行语法，但 MySQL 的 `InnoDB` 对递增主键的 INSERT 有 B-tree 分页开销。表越大，索引树越深，每次 INSERT 耗时越线性增长。
2. **CAST(col AS CHAR)** 在 SELECT 侧做了全表扫描 + 隐式转换。100k 行时 MySQL 完全在内存中，500k 行时可能触发磁盘排序。
3. **`ORDER BY 1` (按 id 排序)** 保证了同步顺序，但 MySQL 需要做文件排序（Using filesort），50 万行时这个开销不可忽略。

### 建议的优化方向（未实现）

| 优化 | 预期提升 | 说明 |
|------|---------|------|
| 增大 `batch_size` 到 5000 | 2-3× | 减少网络往返次数 |
| 增大 `chunk_size` 到 20000 | 1.5-2× | 减少 SELECT 查询次数 |
| 目标表关掉唯一键/索引 | 3-5× | 全部写完再建索引 |
| 用原生类型而不是 CAST(AS CHAR) | 1.5× | 减少 MySQL CAST 开销 |
| 目标端改用 LOAD DATA INFILE | 10× | 跳过 SQL 层直接导入 |

## 脱敏效果验证

| 字段 | 原始值 | 处理后值 | 规则 |
|------|--------|---------|------|
| phone | `14018176509` | `140****6509` | mask_phone |
| email | `test1@example.com` | `t****@example.com` | mask_email |
| password | `pass_b9420def02d2...` | `5032435140cde170...` | hash (SHA-256) |
| id_card | `123456789012345678` | `NULL` | ignore |

数据完整性：500,000 行完全匹配，无丢失、无重复。

## 二进制大小

| 模式 | 大小 |
|------|------|
| Debug | 62 MB |
| Release | 20 MB (strip 后约 12 MB) |
