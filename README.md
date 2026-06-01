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

## 功能特性 | Features

### ✅ Phase 1 — 基础架构 (Foundation)

- **YAML 配置驱动** — 声明式配置文件，支持环境变量 `${VAR:-default}` 注入
- **PostgreSQL / MySQL 双引擎** — 用 sqlx 的 `AnyPool` 统一驱动，一行配置切换
- **配置校验器** — 启动前自动检查：重复名称、引用完整性、规则合法性
- **可视化检查命令** — `ghostsync check config.yaml` 一键测试所有数据库连通性

### ✅ Phase 2 — 规则引擎 (Rule Engine)

| 规则 | 效果 | 示例 |
|------|------|------|
| `ignore` | 跳过该字段，不写入目标表 | `id_card → 忽略` |
| `mask_phone` | 手机号中间部分替换为 `*` | `13812345678 → 138****5678` |
| `mask_email` | 保留首字母，其余替换 | `alice@example.com → a****@example.com` |
| `hash` | SHA-256 或 MD5 单向哈希 | `my_password → 5e8848...` |

### ✅ Phase 3 — 核心同步引擎 (Sync Engine)

- **分块读取** — 基于 PK 的 `ORDER BY ... LIMIT ... OFFSET` 流式读取，内存友好
- **Worker 池并发脱敏** — 多线程并发执行脱敏规则，CPU 密集型任务能跑满
- **批量写入** — `batch_size` 控制单次 INSERT 的行数，减少网络往返
- **类型安全** — `row_to_map` 自动处理 `String`/`i64`/`f64`/`bool` 类型转换
- **截断目标表** — `truncate_target: true` 在同步前清空目标表

### ✅ Phase 4 — 守护进程 & HTTP API (Daemon & API)

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

# 或者使用 Docker
docker pull ghostsync/ghostsync:latest
```

### 配置 | Configuration

创建 `config.yaml`：

```yaml
sources:
  - name: production
    kind: postgres
    url: "postgres://app_user:${PG_PASS}@prod-db.internal:5432/production_db"

targets:
  - name: staging
    kind: postgres
    url: "postgres://staging_user:${STAGING_PASS}@staging-db.internal:5432/staging_db"

tasks:
  - name: nightly-sync
    source: production
    target: staging
    tables:
      - name: public.users
        rules:
          - field: phone
            rule: mask_phone
          - field: email
            rule: mask_email
          - field: password_hash
            rule: hash
            params:
              algorithm: sha256
          - field: id_card
            rule: ignore
      - name: public.orders
        # 无规则 = 原样同步
      - name: public.audit_logs
        mode: ignore  # 跳过整张表
    chunk_size: 5000
    batch_size: 1000
    max_workers: 4
    schedule: "0 2 * * 5"  # 每周五凌晨2点
    truncate_target: true
```

完整示例见 [`config.example.yaml`](./config.example.yaml)。

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

# 指定 SQLite 存储路径（默认 ~/.ghostsync/store.db）
ghostsync serve config.yaml --store /data/ghostsync.db

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
│   └── schema.rs        # 表结构自省（待 Phase 5+ 接入）
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

## 测试 | Testing

```bash
# 运行所有测试（39 个）
cargo test

# 运行特定模块测试
cargo test rule::  # 规则引擎测试
cargo test engine::  # 同步引擎测试
cargo test config::  # 配置加载和校验测试
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
