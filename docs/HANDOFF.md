# 开发交接文档

> 单一权威交接文档。取代早期的 PHASE0_PROGRESS / PHASE0_COMPLETE / PHASE1_PROGRESS
> （已删除）。每次换机或阶段推进后更新本文。

**最后更新:** 2026-07-22
**工作分支:** `codex/phase1-durable-cdc`（以 `git status --short --branch` 为准）

## 换机快速开始

```bash
git fetch origin
git switch --track origin/codex/phase1-durable-cdc   # 或已存在则 git switch + git pull
cargo build --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib
```

预期：编译通过，clippy 零 warning，**271 项单元测试全部通过**。真实数据库集成测试是 opt-in（见下"测试环境"），不在默认 `--lib` 范围内。source-postgres 的 PG18 集成测试已在真实 PostgreSQL 18.4 上全部通过（4/4，在 WSL 内运行）。

## 核心目标（不变）

源和目标数据最终一致；DDL 变更紧密跟随，无法安全跟随时重拉受影响表；在保证正确性前提下追求低延迟、高吞吐。允许中间过程短暂不一致，最终一致性有保证。源仅 PostgreSQL 18，目标仅 Apache Cloudberry 2.1（双向锁定）。完整方向见 [delivery-plan.md](delivery-plan.md) 与 [architecture.md](architecture.md)。

## 已拍板决策（摘要）

- **测试 CI:** GitHub Actions Linux CI 已接入（Phase 0）；跨 Windows/WSL 网络与压测保留本地。
- **版本锁定:** 只支持 PG18 ↔ Cloudberry 2.1，启动时 `SELECT version()` 严格校验，其它版本拒绝启动。
- **Spool:** 固定目录（`engine.spool_directory`，默认 `data/spool`），不设硬容量上限；按 topology generation 自动清理被取代的旧目录；磁盘水位到达时进入 `RESOURCE_WAIT` 背压，不提前 ACK、不丢数据。
- **Reconciliation:** Phase 1-2 差异一律触发 shadow rebuild；原地 repair 保持 capability-gated，直到 replay-compatible 协议对 PK move/delete/reuse/swap 的证明与故障矩阵成立。见 [reconciliation.md](reconciliation.md)。
- **Citus:** 真实数据面推到 Phase 4（per-worker slot/checkpoint/topology generation），当前 fail-closed（`DataPlane::Gated`）。**`PhysicalHa` 已启用**，复用 Standalone 数据面（`data_plane_for_topology`）：failover 改 timeline 由 checkpoint identity 校验捕获并安全 rebuild。
- **Migration:** control 与 target metadata 各自版本化、SHA-256 checksum 保护、启动时执行。已到 **target V8**（V8 = `schema_events` DDL transition ledger）。
- **Web UI:** **已重建为 Vue 3 + Naive UI + Pinia + Vue Router**（`9e48e29`），5 视图 + API 层 + 认证 store，构建通过。JWT/CSRF 认证待后端对接。
- **Cloudberry 镜像:** Apache Cloudberry 2.1 无官方即用 server 镜像（Docker Hub 只有 build/test 环境镜像）。**已解决**：`tests/integration/cloudberry/build-local-image.sh` 把官方 2.1.0 RPM 装进 rockylinux9 + `gpinitsystem` 起单节点 demo cluster（`init-singlenode.sh`），可复现地提供可运行 Cloudberry。CI 的 `integration-cloudberry` job 用它跑 target + E2E 测试。
- **CI 验证矩阵:** 4 个 job——rust-checks（fmt/clippy/build/单测）、integration-pg18（真实 PG18 source + control-store）、integration-cloudberry（真实 Cloudberry target + 跨库 E2E）、web-checks（Vue 构建）。真实两端集成测试均已进 PR 门禁并本地验证通过。

## 进度

### Phase 0 — 工程门禁与基础设施 ✅ 完成
- `.github/workflows/ci.yml`：fmt / clippy / build / lib+bins 测试。
- control migration（`crates/metadata/src/migration.rs`，到 V2）与 target metadata migration（`crates/target-cloudberry/src/migration.rs`，到 V8，含 `snapshot_table_progress`、`transaction_chunk_progress`、`transaction_committed_chunks`、`schema_events`）。
- 版本校验：`source-postgres` `verify_pg18_version`、`target-cloudberry::version::verify_cloudberry_21_version`。
- `tests/integration/docker-compose.yml`：PG18(55432) + Cloudberry 2.1(55433)，含 init 脚本与 healthcheck。

### Phase 1 — Standalone 连续数据面 🔶 进行中
已完成（构建块 + 单测）：
- **1.1 Source keyset paging** ✅ `crates/source-postgres/src/snapshot.rs`：`read_canonical_pk_page`（`LIMIT+1` lookahead、typed `ROW(...)>ROW(...)`、`SnapshotKeyPage{has_more,next_key}`）、`read_canonical_row_page`、`copy_text_pk_range`。含真实 PG18 集成测试 `tests/snapshot_page_pg18.rs`（opt-in）。
- **1.2 Target snapshot progress** ✅ `crates/target-cloudberry/src/snapshot/progress.rs`：`register_snapshot_table_progress`、`copy_snapshot_page`（COPY 与 cursor 同事务）、完整 CRUD SQL、V7 schema。
- **1.4 Spool 自动清理** ✅ `crates/source-postgres/src/spool.rs`：`remove_superseded_generations` 在 `open` 时回收更低 generation 的目录，best-effort 非致命。
- **故障注入边界** 🔶 source adapter 已暴露 fatal `AfterSourceRead` / `AfterSpoolCommit` observer；target ledger apply 已暴露 final/non-final chunk 与空事务的 commit 前后 observer。真实 Cloudberry ledger 测试会注入 final chunk 已提交但调用方收到错误，并验证 checkpoint fast path 不重放 DML。

- **1.3 runtime 接入 bounded snapshot** ✅ `crates/engine/src/runtime/job.rs`：`load_table_snapshot` 按 PK page 循环（`read_canonical_pk_page` → `copy_range` → `copy_text_pk_range` → target `SnapshotPageLoader::apply_page` 同事务 cursor）；无 PK 表 fallback 整表 COPY。target 侧 `begin_snapshot_pages`/`SnapshotPageLoader`（`crates/target-cloudberry/src/snapshot.rs`）。含集成测试 `cloudberry21_snapshot_paging_with_resume`。**源侧 PK 分页已在真实 PG18 验证通过**。

**未完成（下一位优先处理，按顺序）：**

1. **1.4 补充按时间的 spool 清理（可选增强）。** 现在只按 generation 回收。若要"checkpoint 后保留 N 小时再删当前 generation 内已 retire 的 journal"，需在 `SpoolLimits`/config 增加 `retention` 字段并在 checkpoint 推进后调用。当前 ENOSPC → `RESOURCE_WAIT` 背压已就绪。
2. **1.5 E2E kill-point 测试。** 将现有 typed observer 接入跨进程 harness，覆盖 5 个 kill 点：source read 后、spool write 后、target chunk commit 前、checkpoint commit 后/ACK 前、final chunk commit ambiguity。验证重启后数据最终一致（PK count + canonical digest）。target final chunk commit ambiguity 已有真实 Cloudberry 场景，剩余四个仍需跨进程证据。

**Phase 1 退出条件：** 最大测试事务显著大于进程内存预算且内存保持水位内；上述 5 个 kill 点均收敛；磁盘 high-water 进入 `RESOURCE_WAIT`，扩容后继续且不触发 rebuild。

### Phase 2 — DDL 紧密跟随 🔶 进行中（本轮 2026-07-22 打下基础）
已完成（构建块 + 单测，见 `docs/phase2-ddl-tight-follow-adr.md`）：
- **DDL 分类** ✅ `DdlMessage::replication_impact()`（`core/src/change.rs`）：CREATE INDEX/GRANT/ANALYZE 等证明无害命令不再触发 rebuild（fail-closed 白名单）。
- **DDL event v2 类型** ✅ `TableTransition`/`TransitionKind`（AddColumn/DropColumn/RenameColumn/AlterColumnType/AddTable/DropTable/Unknown），`is_online_safe()` 保守判定；`DdlMessage.transitions`（`#[serde(default)]` 向后兼容 v1）。
- **schema-diff 分类器** ✅ `core/src/schema_diff.rs::classify_table_diff`：按 attnum 对比 before/after schema → TransitionKind；PK 变化/narrowing/未知 → `Unknown`（rebuild）。engine 侧 `TableBindingRegistry::classify_relation_diff` 接入。
- **schema_events ledger** ✅ target V8 migration + `target-cloudberry/src/schema_event.rs`（record/load/list_unfinished/advance_state，forward-only 状态机）+ 集成测试。
- **dynamic binding registry** ✅ `TableBindingRegistry` insert/remove/swap（运行时可 swap binding，维持唯一性不变量）。
- **SchemaBarrier 结构化** ✅ 带 `command_tag`；online-safe 的 v2 DDL 在 barrier reason 中标注可跟随。

**未完成（下一位优先处理，按顺序）：**
1. **DDL event v2 的 source 端捕获。** `source-postgres/src/ddl.rs` 的 event trigger 目前只发 v1 envelope（command_tag + fingerprint）。需在 `ddl_command_end` 阶段用已有 `schema_snapshot(oid)` 捕获**受影响表的 after-schema**放入 payload（before-schema 由 engine 从 binding 提供，因 command_end 阶段拿不到 before）。需真实 PG18 验证 PL/pgSQL。
2. **shadow reload / catch-up / cutover 流程。** 用 `begin_snapshot_pages` 重载单表 shadow → 从 spool 回放 barrier 后 CDC → 原子 RENAME cutover + `registry.swap`。持久化用 `schema_events`（pending→in_transition→completed）。
3. **在线白名单 handler 接入 barrier 决策。** 当 `classify_relation_diff` 全 online-safe 时走 transition 而非 rebuild；否则保持 rebuild。
4. **DROP quarantine + 新表自动准入。**

**Phase 2 退出条件：** 并发 DML+DDL、同事务多次 DDL、rapid DDL、rename/drop/recreate、目标 commit ambiguity、进程重启和重复 WAL 矩阵通过；普通 DDL 不调用 `request_pipeline_rebuild`。

### Phase 3 / 4
吞吐延迟与 soak / Citus 真实多节点数据面。详见 [delivery-plan.md](delivery-plan.md)。Citus 当前 `DataPlane::Gated`；PhysicalHa 已复用 Standalone。

## 测试环境

```bash
# 基础 PG18 + Cloudberry
cd tests/integration && docker compose up -d && docker compose ps

# 真实 PG18 单元级集成（opt-in，需设 DSN）
# 见 tests/integration/README.md

# Citus 集群验证
cd tests/integration/citus && ./verify.sh
```

WSL Docker 可用。跨主机功能测试可用 `10.144.144.4/5` 可达地址。测试容器不属于交付状态。

## 代码结构

Cargo workspace，crate 边界见 [architecture.md](architecture.md) 代码边界一节：`core / config / metadata / source-postgres / target-cloudberry / engine / api / app`，前端 `web/`。依赖方向：适配器依赖 `core`，`core` 不依赖数据库驱动/HTTP/前端。

## 安全提醒

会话中出现过的 GitHub PAT 具备 repo 写权限，**务必立即在 GitHub 撤销并重新生成**，改用 credential helper 或环境变量提供，勿写入仓库或 Git 配置。
