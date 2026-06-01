# GhostSync

<div align="center">

_轻量级数据同步与脱敏代理 · Lightweight Data Sync & Desensitization Proxy_

[![Rust](https://img.shields.io/badge/Rust-1.85%2B-dea584?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
![Build](https://img.shields.io/badge/build-passing-brightgreen)

</div>

---

## 简介 | Introduction

**GhostSync** 是一款面向后端开发和中小型团队的**服务端数据同步与脱敏代理**。一个独立的二进制文件，一行命令即可运行，无需安装数据库驱动、消息队列或运行时环境。

**核心场景：**

| 场景 | 说明 |
|------|------|
| 🛡️ **生产→测试脱敏同步** | 将生产数据分块读取→并发脱敏→批量写入测试库，手机号/邮箱/密码自动脱敏 |
| ⏰ **定时任务调度** | 内置 Cron 表达式支持，支持每周/每天/每小时的自动同步 |
| 🔗 **CI/CD 集成** | HTTP API 触发，可在 GitHub Actions/GitLab CI 发布前自动脱敏并同步 |
| 🗄️ **极简备份** | 低成本替代 DTS 服务，支持 PostgreSQL/MySQL 间任意方向的数据迁移 |

## 速度 | Performance

同机 MySQL 场景下，GhostSync 自动启用 **SELECT INTO OUTFILE + LOAD DATA** 零拷贝路径：

| 数据量 | 耗时 | RPS | 说明 |
|--------|------|-----|------|
| 1,000,000 行 + SHA256 | **13.7s** | **73,000/s** | 同机 MariaDB, 128MB buffer pool |
| 1,000,000 行 (明文) | ~11s | ~90,000/s | 无脱敏规则时更快 |

详见 [SPEED_TEST_REPORT.md](./SPEED_TEST_REPORT.md)。

## 功能特性 | Features

### ✅ 数据源与目标管理

- **YAML 配置驱动** — 声明式配置文件，支持环境变量 `${VAR:-default}` 注入
- **PostgreSQL / MySQL 双引擎** — 用 sqlx 的 `AnyPool` 统一驱动，一行配置切换
- **配置校验器** — 启动前自动检查：重复名称、引用完整性、规则合法性
- **连通性检查** — `ghostsync check config.yaml` 一键测试所有数据库

### ✅ 规则引擎 (Rule Engine)

| 规则 | 效果 | 示例 |
|------|------|------|
| `ignore` | 跳过该字段，不写入目标表 | `id_card → 忽略` |
| `mask_phone` | 手机号中间部分替换为 `*` | `13812345678 → 138****5678` |
| `mask_email` | 保留首字母，其余替换 | `alice@example.com → a****@example.com` |
| `hash` | SHA-256 或 MD5 单向哈希 | `my_password → 5e8848...` |

### ✅ 同步引擎 (Sync Engine)

- **Keyset 分页** — 基于主键 B-tree 游标的分页，性能稳定不随偏移量下降
- **同机 MySQL 零拷贝** — 自动检测同机 MySQL，使用 `SELECT INTO OUTFILE` + `LOAD DATA`，跳过 TCP 传输
- **跨机通用路径** — 分块读取 + 规则处理 + 多行 INSERT，支持任何 MySQL/PostgreSQL 组合
- **Worker 池并发脱敏** — 多线程并发执行脱敏规则，CPU 密集型任务能跑满
- **批量写入** — `batch_size` 控制单次 INSERT 的行数，减少网络往返
- **截断目标表** — `truncate_target: true` 在同步前清空目标表

### ✅ 守护进程 & HTTP API (Daemon & API)

```text
┌─────────────────────────────────────────────────┐
│                 GhostSync Daemon                  │
│                                                   │
│  ┌──────────┐   ┌──────────────┐   ┌───────────┐  │
│  │ Cron     │   │ HTTP API     │   │ Store     │  │
│  │ Scheduler│──▶│ (axum :9710) │──▶│ (SQLite)  │  │
│  │          │   │              │   │           │  │
│  │ task-1   │   │ POST /run    │   │ task_runs │  │
│  │ task-2   │   │ GET  /logs   │   │ task_logs │  │
│  │ ...      │   │ GET  /health │   │           │  │
│  └────┬─────┘   └──────┬───────┘   └───────────┘  │
│       │                │                           │
│       └─────┬──────────┘                           │
│             ▼                                       │
│      execute_and_log_task()                         │
│             │                                       │
│             ▼                                       │
│      engine::run_task()                             │
└─────────────────────────────────────────────────┘
```

| 命令 | 说明 |
|------|------|
| `run` | 运行一次同步后退出 |
| `serve` | 启动守护进程（Cron 调度 + HTTP API） |
| `check` | 连通性测试 |
| `inspect` | 打印解析后的完整配置 |

### 🔮 规划中 | Planned

- **增量同步** — 基于 WAL/Binlog 的实时 CDC 同步（高价值功能）
- **Web 管理面板** — Vue + API 的极简配置界面
- **对象存储目标** — S3/R2/MinIO 离线备份
- **数据量仪表盘** — RPS、延迟、成功率实时监控

## 快速开始 | Quick Start

### 安装 | Install

```bash
# 从源码编译（需要 Rust 1.85+）
git clone https://github.com/ghostSync/ghostSync.git
cd ghostSync
cargo build --release
cp target/release/ghostsync /usr/local/bin/
```

### 配置 | Configuration

创建 `config.yaml`：

```yaml
sources:
  - name: production
    kind: postgres
    host: prod-db.internal
    port: 5432
    user: app_user
    password: "${PG_PASS}"
    database: production_db

  - name: mysql_prod
    kind: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    password: "${MYSQL_PASS}"
    database: source_db

targets:
  - name: staging
    kind: postgres
    host: staging-db.internal
    port: 5432
    user: staging_user
    password: "${STAGING_PASS}"
    database: staging_db

  - name: mysql_staging
    kind: mysql
    host: 127.0.0.1
    port: 3306
    user: root
    password: "${MYSQL_PASS}"
    database: target_db

tasks:
  - name: nightly-sync
    source: mysql_prod
    target: mysql_staging
    tables:
      - name: sync_test_users
        rules:
          - field: phone
            rule: mask_phone
          - field: email
            rule: mask_email
          - field: password
            rule: hash
            params:
              algorithm: sha256
          - field: id_card
            rule: ignore
    chunk_size: 50000
    batch_size: 50000
    max_workers: 8
    truncate_target: true
    schedule: "0 2 * * 5"   # 每周五凌晨2点
```

### 运行 | Run

```bash
# 1. 检查配置和连通性
ghostsync check config.yaml

# 2. 手动执行同步
ghostsync run config.yaml

# 仅执行指定任务
ghostsync run config.yaml --task nightly-sync

# 3. 启动守护进程
ghostsync serve config.yaml --port 9710

# 4. API 触发同步
curl -X POST http://localhost:9710/api/tasks/nightly-sync/run
curl http://localhost:9710/api/tasks/nightly-sync/logs
```

## 项目结构 | Project Layout

```text
src/
├── main.rs              # CLI 入口：run / serve / check / inspect
├── config/
│   ├── types.rs         # 配置类型定义（Config, DbKind, TaskConfig...）
│   ├── loader.rs        # YAML 加载 + 环境变量展开
│   └── validator.rs     # 配置校验（重复名、引用完整、规则参数）
├── db/
│   ├── connector.rs     # 数据库连接池管理（AnyPool）
│   └── schema.rs        # 表结构自省
├── rule/
│   ├── mod.rs           # Rule trait + RuleEngine 编排器
│   ├── mask_phone.rs    # 手机号脱敏
│   ├── mask_email.rs    # 邮箱脱敏
│   ├── hash.rs          # SHA-256 / MD5 哈希
│   └── ignore.rs        # 字段跳过
├── engine/
│   └── mod.rs           # 同步引擎：分块读取 → 并发脱敏 → 批量写入
├── scheduler/
│   └── mod.rs           # Cron 调度器 + Axum HTTP API 服务器
├── store/
│   └── mod.rs           # SQLite 持久化存储（任务运行记录）
└── logger/
    └── mod.rs           # Tracing 日志初始化
```

## 技术栈 | Tech Stack

| 组件 | 技术选型 | 说明 |
|------|----------|------|
| 语言 | Rust 2024 Edition | 零成本抽象、内存安全、单一二进制分发 |
| 异步运行时 | Tokio (full) | 多线程异步 I/O |
| 数据库驱动 | sqlx 0.8 (AnyPool) | 编译时 SQL 检查，动态切换 Postgres/MySQL/SQLite |
| HTTP 服务器 | Axum 0.7 | 类型安全的路由和提取器 |
| 配置格式 | YAML + serde | 声明式配置，支持 `${ENV_VAR}` |
| 日志 | tracing + tracing-subscriber | 结构化日志，支持 JSON 输出 |
| 调度 | cron 0.14 | 标准 Cron 表达式解析 |
| 持久化 | SQLite (via sqlx) | 零依赖本地存储，自动迁移 |

## 许可 | License

MIT License — 自由使用、修改和分发。

---

<div align="center">
  <sub>Built with ❤️ by ghostSync Team</sub>
</div>
