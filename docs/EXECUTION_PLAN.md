# etl_server_cloudberry 持续执行计划

**生成时间:** 2026-07-21
**目标:** 完成 Phase 0-4 全部生产就绪代码

> ⚠️ 本文是初始的详细任务分解，其中的"完成状态/环境验证"是 2026-07-21 的快照，可能已过时。
> **实时进度、换机步骤与下一步以 [HANDOFF.md](HANDOFF.md) 为准。** 本文保留用于查阅各 Phase 的
> 细粒度子任务拆分。

---

## 环境验证结果

✅ **Git 推送能力:** 已验证可推送到 `origin/codex/phase1-durable-cdc`  
✅ **WSL Docker:** 可用（Docker 26.1.5，当前运行 Citus 14.1 测试容器）  
✅ **项目编译:** workspace 72 个 Rust 源文件，Clippy 编译通过  
✅ **现有基础:** control migration v2、reconciliation primitives、snapshot session、chunk ledger 已有

---

## Phase 0: 工程门禁与基础设施（预计 5-7 天）

### 目标
让后续变更有稳定的测试、配置和交付边界；补齐 CI、migration、版本校验、基础 UI。

### 已有资产审查
- ✅ `crates/metadata/src/migration.rs` 已有 control DB migration v1/v2（SHA-256 checksum 保护）
- ✅ `tests/integration/README.md` 定义了 PG18 opt-in 测试方式
- ❌ 缺少 GitHub Actions CI workflow
- ❌ 缺少 target metadata migration（`pg2cb_meta` schema）
- ❌ 缺少源/目标版本严格校验
- ❌ Web UI 是 Svelte 5，需要重建为 Vue 3 + Naive UI

### 工作分解

#### 0.1 GitHub Actions CI 接入（1 天）
**目标:** 默认分支每次 push 自动运行 `cargo test/clippy`、Web check/build。

**文件变更:**
```
.github/workflows/ci.yml          [新增] Linux CI workflow
.github/workflows/release.yml     [新增] Release build workflow（暂只定义，Phase 3 启用）
```

**交付标准:**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` 通过
- `cargo test --workspace --all-features` 单测通过（真实 DB 测试保持 opt-in）
- Web `npm run check && npm run build` 通过

**实施步骤:**
1. 创建 `.github/workflows/ci.yml`，使用 `ubuntu-latest` runner
2. 安装 Rust stable、Node 20、设置 cache
3. 并行运行 Rust workspace 检查和 Web 检查
4. 不安装真实 PG18/Cloudberry（集成测试保留本地环境）

---

#### 0.2 Target Metadata Migration（1 天）
**目标:** `pg2cb_meta` schema 有版本化 migration，与 control DB 独立管理。

**文件变更:**
```
crates/target-cloudberry/src/migration.rs  [修改] 增加 v1 migration（已有 stub）
```

**Schema 设计（v1）:**
```sql
CREATE SCHEMA IF NOT EXISTS pg2cb_meta;

CREATE TABLE pg2cb_meta.schema_version (
    version INT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pg2cb_meta.pipeline_state (
    pipeline_id UUID PRIMARY KEY,
    source_identity TEXT NOT NULL,
    topology_generation BIGINT NOT NULL DEFAULT 1,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE pg2cb_meta.managed_tables (
    pipeline_id UUID NOT NULL,
    target_schema TEXT NOT NULL,
    target_table TEXT NOT NULL,
    source_relation_id TEXT NOT NULL,
    table_generation BIGINT NOT NULL CHECK (table_generation > 0),
    schema_fingerprint TEXT NOT NULL,
    relation_oid OID,
    state TEXT NOT NULL CHECK (state IN ('loading', 'active', 'blocked', 'quarantined')),
    PRIMARY KEY (pipeline_id, target_schema, target_table)
);

CREATE TABLE pg2cb_meta.node_checkpoints (
    pipeline_id UUID NOT NULL,
    node_identity TEXT NOT NULL,
    applied_lsn BIGINT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, node_identity)
);

CREATE TABLE pg2cb_meta.target_chunks (
    pipeline_id UUID NOT NULL,
    target_schema TEXT NOT NULL,
    target_table TEXT NOT NULL,
    node_identity TEXT NOT NULL,
    transaction_lsn BIGINT NOT NULL,
    chunk_seq BIGINT NOT NULL,
    next_seq BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, target_schema, target_table, node_identity, transaction_lsn, chunk_seq)
);
```

**交付标准:**
- `migrate_target_metadata(client)` 函数在启动时执行
- 版本 checksum 不匹配时拒绝启动
- 单测覆盖重复执行幂等性、checksum 保护

---

#### 0.3 源/目标版本严格校验（0.5 天）
**目标:** 启动时校验 `SELECT version()`，只允许 PG 18.x 和 Cloudberry 2.1.x。

**文件变更:**
```
crates/source-postgres/src/connection.rs  [修改] 增加 verify_pg18_version()
crates/target-cloudberry/src/lib.rs       [修改] 增加 verify_cloudberry_21_version()
crates/engine/src/pipeline.rs             [修改] preflight 调用版本校验
```

**实施步骤:**
1. 正则匹配 `PostgreSQL 18.(\d+)` 和 `Apache Cloudberry 2.1.`
2. 不匹配时返回明确错误："This service only supports PostgreSQL 18.x source and Apache Cloudberry 2.1.x target"
3. 单测覆盖 PG 17/19、Cloudberry 2.0/2.2 的拒绝场景

---

#### 0.4 Web UI 重建为 Vue 3 + Naive UI（2 天）
**目标:** 替换 Svelte，实现登录 + Pipeline CRUD + SSE 日志流。

**文件变更:**
```
web/                               [全部重建]
  package.json                     [Vue 3 + Vite + Naive UI + TypeScript]
  src/
    main.ts                        [入口]
    App.vue                        [根组件]
    router/index.ts                [Vue Router]
    views/
      Login.vue                    [登录页]
      PipelineList.vue             [Pipeline 列表]
      PipelineForm.vue             [新增/编辑 Pipeline]
    api/client.ts                  [Axios 封装，自动附加 JWT]
    composables/useEventStream.ts  [SSE 封装]

crates/api/src/auth.rs             [修改] JWT 签发/验证（jsonwebtoken crate）
crates/api/src/routes.rs           [修改] 增加 POST /api/auth/login、GET /api/events SSE
crates/app/src/main.rs             [修改] 嵌入 web/dist/（cfg 条件编译）
```

**UI 功能范围:**
- 登录页：username/password → JWT token 存 localStorage
- Pipeline 列表：表格展示 name、source/target（DSN 脱敏）、状态、lag LSN、操作按钮
- 新增 Pipeline：表单填写 source/target DSN、database、prefix，校验后提交
- 启用/暂停/删除：带 Naive UI Dialog 确认
- SSE 日志流：连接 `/api/events?token=<jwt>`，滚动展示 pipeline events

**交付标准:**
- `npm run dev` 可本地开发（proxy 到 Rust backend）
- `npm run build` 产生 `dist/`，Rust 嵌入后访问 `http://localhost:8080/` 可用
- 登录失败提示、token 过期自动跳转登录页

---

#### 0.5 Docker Compose 测试环境（0.5 天）
**目标:** 提供幂等可复现的 PG18 + Cloudberry 2.1 容器定义。

**文件变更:**
```
tests/integration/docker-compose.yml      [新增] 定义全套测试容器
tests/integration/pg18/init.sql           [新增] PG18 初始化（创建 publication/slot helper）
tests/integration/cloudberry/init.sql     [新增] Cloudberry 初始化（创建目标 database）
```

**容器定义:**
```yaml
services:
  pg18-source:
    image: postgres:18-alpine@sha256:...  # 用 digest 锁定
    environment:
      POSTGRES_PASSWORD: pg2cb_test
      POSTGRES_DB: source
    command: >
      -c wal_level=logical
      -c max_replication_slots=16
      -c max_wal_senders=16
    ports:
      - "127.0.0.1:55432:5432"
    volumes:
      - ./pg18/init.sql:/docker-entrypoint-initdb.d/init.sql

  cloudberry-target:
    image: apache/cloudberry:2.1.0-incubating@sha256:...
    environment:
      POSTGRES_PASSWORD: pg2cb_test
      POSTGRES_DB: target
    ports:
      - "127.0.0.1:55433:5432"
    volumes:
      - ./cloudberry/init.sql:/docker-entrypoint-initdb.d/init.sql
```

**交付标准:**
- `docker compose up -d` 启动容器，health check 通过
- `docker compose down -v` 清理，幂等可重复
- README 更新启动命令

---

### Phase 0 退出条件
- [ ] GitHub Actions CI 绿灯（Rust Clippy/test + Web check/build）
- [ ] Control DB migration v2、target metadata migration v1 启动时自动执行
- [ ] PG 17/19、Cloudberry 2.0/2.2 启动时明确拒绝
- [ ] Web UI 可登录、可 CRUD Pipeline、可看 SSE 日志流
- [ ] `docker-compose.yml` 可一键启动 PG18 + Cloudberry 测试环境
- [ ] `docs/delivery-plan.md` 更新 Phase 0 完成状态

---

## Phase 1: Standalone Bounded Data Path（预计 10-12 天）

### 目标
完成 bounded snapshot、target progress、E2E kill-point 验证；取消事务大小失败。

### 已有资产审查
- ✅ `crates/source-postgres/src/spool.rs` 已有 versioned journal、透明 spill、ENOSPC 保护
- ✅ `crates/target-cloudberry/src/chunk.rs` 已有 ledger、receipt 同事务提交
- ✅ `crates/engine/src/completion.rs` 已有 per-node completion tracker
- ✅ `crates/engine/src/reconcile.rs` 已有 Page/DigestContext/bounded diff primitives
- ❌ source snapshot 仍是整表 COPY（无 keyset paging、无 PK range cursor）
- ❌ target 缺少 `snapshot_progress` 表和 bounded COPY
- ❌ 缺少 E2E kill-point 测试脚本

### 工作分解

#### 1.1 Source Keyset Paging（2 天）
**目标:** snapshot 改为 `LIMIT + 1` lookahead、typed PK row comparison、bounded cursor。

**文件变更:**
```
crates/core/src/snapshot.rs                [新增] Page trait、KeyRange、PageLimits
crates/source-postgres/src/snapshot.rs     [修改] 实现 keyset SQL、lookahead 逻辑
tests/source-postgres/snapshot_paging.rs   [新增] 单测（首页/中间/尾页/空表/复合 PK）
```

**SQL 模板（复合 PK）:**
```sql
SELECT "pk1"::text, "pk2"::text, "c1"::text, ...
  FROM "schema"."table"
 WHERE ROW("pk1", "pk2") > ROW($1::text, $2::text)  -- 有 start 时
 ORDER BY "pk1", "pk2"
 LIMIT $limit_plus_one
```

**交付标准:**
- 返回 `Page { rows, has_more, next_key }`
- lookahead 行不返回给调用方，只用于判断 `has_more`
- 单测覆盖 PG18 真实表（首页返回 LIMIT 行、中间页、尾页 has_more=false、空表）

---

#### 1.2 Target Snapshot Progress（2 天）
**目标:** target 增加 `snapshot_progress` 表，bounded COPY 与 cursor 同事务。

**文件变更:**
```
crates/target-cloudberry/src/migration.rs       [修改] 增加 v2 migration（snapshot_progress 表）
crates/target-cloudberry/src/snapshot/progress.rs [新增] CRUD snapshot progress
crates/target-cloudberry/src/snapshot/manifest.rs [修改] COPY 完成后更新 progress
```

**Schema（v2 migration）:**
```sql
CREATE TABLE pg2cb_meta.snapshot_progress (
    pipeline_id UUID NOT NULL,
    target_schema TEXT NOT NULL,
    target_table TEXT NOT NULL,
    snapshot_session_id UUID NOT NULL,
    table_generation BIGINT NOT NULL,
    last_key_text TEXT[],
    scanned_rows BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_id, target_schema, target_table)
);
```

**交付标准:**
- `begin_snapshot_chunk(...)` 返回 RAII session，包含当前 cursor
- 每个 COPY chunk 提交后更新 `last_key_text` 和 `scanned_rows`
- 同一 S0 存活时，commit ambiguity 可从 cursor 续传
- 单测覆盖 commit 成功/失败后的 cursor 状态

---

#### 1.3 Bounded Snapshot Runtime（3 天）
**目标:** 合流 source pager + target progress，进程崩溃后 fresh S1 重拉。

**文件变更:**
```
crates/engine/src/runtime/snapshot.rs      [新增] BoundedSnapshotRunner
crates/engine/src/runtime/job.rs           [修改] 启动时检查 S0 存活、fence 一致性
tests/integration/snapshot_bounded.rs      [新增] E2E 测试（真实 PG18 + Cloudberry）
```

**逻辑:**
1. 启动时检查 `snapshot_progress` 是否存在且 `snapshot_session_id` 仍对应存活 S0
2. 若 S0 已消失（进程崩溃），清理旧 progress、slot、shadow table，创建新 slot/S1/fence
3. 若 S0 存活且 fence 一致，从 `last_key_text` cursor 继续
4. 每个 page 先 source 读取，再 target bounded COPY，再更新 progress，最后 commit

**交付标准:**
- 真实 PG18 表（1M 行、复合 PK），snapshot 期间 kill 进程，重启后从头重拉（不能续传旧 cursor）
- 同一 S0 内 commit ambiguity（target commit 后 process hang），重启后跳过已提交 chunk

---

#### 1.4 Spool 自动清理（1 天）
**目标:** checkpoint 成功后，按时间窗口自动清理过期 journal。

**文件变更:**
```
crates/source-postgres/src/spool.rs        [修改] 增加 cleanup_expired_journals()
crates/config/src/bootstrap.rs             [修改] 增加 spool_directory、spool_retention_hours 配置
```

**配置:**
```toml
[engine]
spool_directory = "./spool"          # 默认相对路径
spool_retention_hours = 24           # checkpoint 后保留 24h 用于审计
```

**逻辑:**
- 每次 checkpoint 推进后，扫描 spool 目录，删除 `mtime > retention_hours` 且 LSN < checkpoint 的 journal
- 磁盘剩余空间 < 5% 时进入 RESOURCE_WAIT，不自动删除更多（等待人工扩容）

**交付标准:**
- 单测覆盖 journal 创建、checkpoint 推进、自动清理、ENOSPC 保护
- 真实环境写入大事务、checkpoint、验证旧 journal 被删除

---

#### 1.5 E2E Kill-Point 测试（3 天）
**目标:** 5 个关键 kill-point 半自动化测试脚本，验证崩溃后收敛。

**文件变更:**
```
tests/integration/phase1_e2e.sh            [新增] 主测试脚本
tests/integration/scenarios/              [新增] 5 个 kill-point 场景
  01_source_read_kill.sh
  02_spool_write_kill.sh
  03_target_commit_kill.sh
  04_checkpoint_ack_kill.sh
  05_final_chunk_ambiguity.sh
```

**场景 1: source read 后 kill**
```bash
# 启动 service，监控日志等待 "WAL decode batch X"
# docker kill etl-service
# 重启 service，验证从未 ACK 的 LSN 重新消费
# 检查 target 数据最终一致（PK count 和 checksum）
```

**交付标准:**
- 每个场景脚本独立运行，输出 PASS/FAIL
- `phase1_e2e.sh` 串行运行 5 个场景，全部通过输出 "All kill-point tests passed"
- README 更新测试执行步骤

---

### Phase 1 退出条件
- [ ] Source snapshot 使用 keyset paging，单测覆盖 PG18 真实表
- [ ] Target snapshot progress 持久化，commit ambiguity 可续传
- [ ] Bounded snapshot runtime 崩溃后从新 S1 重拉，不复用旧 cursor
- [ ] Spool journal 按时间窗口自动清理，ENOSPC 进入 RESOURCE_WAIT
- [ ] 5 个 E2E kill-point 测试脚本通过，数据最终一致
- [ ] `docs/delivery-plan.md` 更新 Phase 1 完成状态

---

## Phase 2-4 简要概览

### Phase 2: DDL 紧密跟随（5-6 周）
- DDL event v2 envelope、持久 schema event/version
- 在线白名单：ADD COLUMN nullable、DROP、RENAME、widening、enum append
- Shadow reload: PK/collation/类型收窄触发 table rebuild
- Dynamic binding registry、catalog snapshot

### Phase 3: 吞吐与并发（4-5 周）
- pgoutput protocol v2 streaming 直接写 spool
- Table/node applier 并发，completion tracker 管理连续前缀
- 24/72h soak、p95/p99 lag < 5s/30s、10K rows/s 持续吞吐

### Phase 4: Citus 支持（4 周）
- Per-worker publication/slot/reader/checkpoint
- Topology generation、stable endpoint、failover continuity
- Hash-distributed table 基础场景（rebalance 保持 gated）

---

## Git 提交策略

### Commit 规范
- 每个独立功能点立即 commit（不等 Phase 完成）
- Message 格式：`<verb> <what>\n\n<why and context>`
- 示例：`Add keyset paging to source snapshot\n\nReplaces full-table COPY with bounded PK cursor. Lookahead row determines has_more flag. Supports composite PK with typed row comparison.`

### Branch 策略
- Phase 0-1：继续在 `codex/phase1-durable-cdc`
- Phase 2：评估是否需要新分支 `codex/phase2-ddl`
- 每个 Phase 完成后创建 PR 到 `master`

### PR Description 模板
```markdown
## Phase X: <Title>

### 完成内容
- [x] Task 1
- [x] Task 2

### 测试证据
- Workspace 单测：235 passed
- 真实 PG18 集成测试：3 passed (snapshot, apply, chunk ledger)
- E2E kill-point：5/5 passed

### 已知限制
- XXX 功能保持 validation-gated，Phase Y 解锁

### 部署影响
- 新增配置字段 `spool_directory`，需要更新 toml
- Migration 自动执行，兼容旧数据
```

---

## 下一步

**请确认以上计划，回复"开始 Phase 0"，我将立即执行 Phase 0.1（GitHub Actions CI 接入）。**

每个子任务完成后我会 commit 并汇报进度，Phase 0 全部完成后创建 PR 并等待你 review。
