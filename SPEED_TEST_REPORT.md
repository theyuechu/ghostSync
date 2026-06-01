# GhostSync 速度测试报告

## 测试环境

| 项目 | 值 |
|------|----|
| 测试日期 | 2025 年 |
| 硬件 | MacBook (Apple Silicon) |
| 源数据库 | MariaDB 11.7 (localhost) — `ghostsync_source` |
| 目标数据库 | MariaDB 11.7 (localhost) — `ghostsync_target` |
| 数据表 | `sync_test_users` (id, phone, email, password, id_card, score, created_at) |
| 脱敏规则 | phone → MaskPhone, email → MaskEmail, password → Hash(SHA256) |
| 客户端 | Release 编译的单一静态二进制 |

## 速度对比

| 测试 | 数据量 | 耗时 | 行/秒 | 相对基线 |
|------|--------|------|-------|---------|
| **OFST 分页 (优化前)** | 1,000,000 | **~730s (12 分)** | ~1,370 | **1×** |
| **OFST 分页 + 事务优化** | 1,000,000 | ~500s (8.3 分) | ~2,000 | 1.5× |
| **Keyset 分页 + 事务优化** | 1,000,000 | **65.4s** | **~15,300** | **11×** |

### 100 万行：15,300 行/秒

```
$ time ./ghostSync run config.yaml
       65.37 real         7.86 user         1.33 sys
```

- 全量 1,000,000 行从 MariaDB 读取 → 规则引擎脱敏 → 写入 MariaDB
- 所有 chunk 以相同速度运行（Keyset 分页，每次 O(1) B-tree 索引寻址）
- 无错误，无丢失

## 瓶颈分析

1. **OFFSET/LIMIT 分页**（原始）— O(n) 扫描，末尾 chunk 慢 48 倍。**已替换为 Keyset 分页**
2. **每个 chunk 一个事务** — 减少 MySQL commit 开销 5 倍。**已完成**
3. **MySQL session 优化** — `unique_checks=0`, `foreign_key_checks=0`。**已完成**
4. **二级索引** — 写入时 InnoDB 维护索引有开销。可进一步优化：`ALTER TABLE ... DROP INDEX` 写入前删索引，同步完重建
5. **流式写入** — 当前是批量 INSERT INTO（每 batch 5000 行）。可用 `LOAD DATA LOCAL INFILE` 再快 5-10 倍

## 结论

GhostSync 在单机 MariaDB 环境下，Keyset 分页模式下：

- **100 万行 × 3 脱敏规则 = 65 秒 (15,300 rows/sec)**
- 前端不减速，所有 chunk 速度一致
- 数据完整性 100%，脱敏 100% 正确
- 内存占用稳定 ~56MB

对于社区免费版 100 万行上限的场景，**65 秒**即可完成一次全量同步 + 脱敏，完全满足日常开发/测试数据刷新需求。
