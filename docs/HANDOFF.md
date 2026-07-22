# 开发交接文档

> 单一权威交接文档。取代早期的 PHASE0_PROGRESS / PHASE0_COMPLETE / PHASE1_PROGRESS
> （已删除）。每次换机或阶段推进后更新本文。

**最后更新:** 2026-07-22
**工作分支:** `codex/phase2-ddl`（以 `git status --short --branch` 为准）

## 换机快速开始

```bash
git fetch origin
git switch --track origin/codex/phase1-durable-cdc   # 或已存在则 git switch + git pull
cargo build --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib
```

预期：编译通过，clippy 零 warning，**293 项 library 单元测试全部通过**。真实数据库集成测试是 opt-in（见下"测试环境"），不在默认 `--lib` 范围内。source-postgres 的 PG18 集成测试已在真实 PostgreSQL 18.4 上全部通过（4/4，在 WSL 内运行）。

## 核心目标（不变）

源和目标数据最终一致；DDL 变更紧密跟随，无法安全跟随时重拉受影响表；在保证正确性前提下追求低延迟、高吞吐。允许中间过程短暂不一致，最终一致性有保证。源仅 PostgreSQL 18，目标仅 Apache Cloudberry 2.1（双向锁定）。完整方向见 [delivery-plan.md](delivery-plan.md) 与 [architecture.md](architecture.md)。

## 已拍板决策（摘要）

- **测试 CI:** GitHub Actions Linux CI 已接入（Phase 0）；跨 Windows/WSL 网络与压测保留本地。
- **版本锁定:** 只支持 PG18 ↔ Cloudberry 2.1，启动时 `SELECT version()` 严格校验，其它版本拒绝启动。
- **Spool:** 固定目录（`engine.spool_directory`，默认 `data/spool`），不设硬容量上限；按 topology generation 自动清理被取代的旧目录；磁盘水位到达时进入 `RESOURCE_WAIT` 背压，不提前 ACK、不丢数据。
- **Reconciliation:** Phase 1-2 差异一律触发 shadow rebuild；原地 repair 保持 capability-gated，直到 replay-compatible 协议对 PK move/delete/reuse/swap 的证明与故障矩阵成立。见 [reconciliation.md](reconciliation.md)。
- **Citus:** 真实数据面推到 Phase 4（per-worker slot/checkpoint/topology generation），当前 fail-closed（`DataPlane::Gated`）。**`PhysicalHa` 已启用**，复用 Standalone 数据面（`data_plane_for_topology`）：failover 改 timeline 由 checkpoint identity 校验捕获并安全 rebuild。
- **Migration:** control 与 target metadata 各自版本化、SHA-256 checksum 保护、启动时执行。已到 **target V9**（V8 = `schema_events`，V9 = per-table schema transition ledger）。
- **Web UI:** **已重建为 Vue 3 + Naive UI + Pinia + Vue Router**（`9e48e29`），5 视图 + API 层 + 认证 store，构建通过。JWT/CSRF 认证待后端对接。
- **Cloudberry 镜像:** Apache Cloudberry 2.1 无官方即用 server 镜像（Docker Hub 只有 build/test 环境镜像）。**已解决**：`tests/integration/cloudberry/build-local-image.sh` 把官方 2.1.0 RPM 装进 rockylinux9 + `gpinitsystem` 起单节点 demo cluster（`init-singlenode.sh`），可复现地提供可运行 Cloudberry。CI 的 `integration-cloudberry` job 用它跑 target + E2E 测试。
- **CI 验证矩阵:** 4 个 job——rust-checks（fmt/clippy/build/单测）、integration-pg18（真实 PG18 source + control-store）、integration-cloudberry（真实 Cloudberry target + 跨库 E2E）、web-checks（Vue 构建）。真实两端集成测试均已进 PR 门禁并本地验证通过。

## 进度

### Phase 0 — 工程门禁与基础设施 ✅ 完成
- `.github/workflows/ci.yml`：fmt / clippy / build / lib+bins 测试。
- control migration（`crates/metadata/src/migration.rs`，到 V2）与 target metadata migration（`crates/target-cloudberry/src/migration.rs`，到 V9，含 `snapshot_table_progress`、transaction chunk ledger、`schema_events`、`table_schema_transitions`）。
- 版本校验：`source-postgres` `verify_pg18_version`、`target-cloudberry::version::verify_cloudberry_21_version`。
- `tests/integration/docker-compose.yml`：PG18(55432) + Cloudberry 2.1(55433)，含 init 脚本与 healthcheck。

### Phase 1 — Standalone 连续数据面 ✅ 完成
已完成（构建块 + 单测）：
- **1.1 Source keyset paging** ✅ `crates/source-postgres/src/snapshot.rs`：`read_canonical_pk_page`（`LIMIT+1` lookahead、typed `ROW(...)>ROW(...)`、`SnapshotKeyPage{has_more,next_key}`）、`read_canonical_row_page`、`copy_text_pk_range`。含真实 PG18 集成测试 `tests/snapshot_page_pg18.rs`（opt-in）。
- **1.2 Target snapshot progress** ✅ `crates/target-cloudberry/src/snapshot/progress.rs`：`register_snapshot_table_progress`、`copy_snapshot_page`（COPY 与 cursor 同事务）、完整 CRUD SQL、V7 schema。
- **1.4 Spool 自动清理** ✅ target checkpoint 持久后通过 `ChangeSource::cleanup` 立即幂等 retire；verified WAL replay 启动路径清理同 identity 中断残留；`remove_superseded_generations` 在 `open` 时 best-effort 回收更低 generation 目录。
- **故障注入边界** ✅ source adapter 已暴露 fatal `AfterSourceRead` / `AfterSpoolCommit` observer；target ledger apply 已暴露 final/non-final chunk 与空事务的 commit 前后 observer；bounded snapshot 暴露 page commit 后 observer。生产 factory 默认不安装 observer，测试 factory 可将同一控制器贯穿完整 runtime 数据路径。
- **1.5 完整 runtime 恢复矩阵** ✅ `crates/engine/tests/phase1_recovery_e2e.rs` 在真实 PG18 + Cloudberry 2.1 上覆盖 source read 后、spool commit 后、非 final target chunk commit 前/后、final chunk commit 后五个边界。每次故障均销毁 job、释放/重获 lease、重建 factory/source/sink，再验证源目标有序行完全相同且 target ledger 清空。该矩阵同时发现并修复 shadow 名误用 fencing token 的问题；shadow 现在按 topology/snapshot generation 规划，跨 lease 保持稳定，fencing 仍独立阻止旧 owner 写入。
- **大事务内存水位** ✅ 同一真实矩阵用 32,768 行、约 32 MiB 的单事务压过 64 KiB memory high-water 512 倍，typed observer 证明事务走 durable spool；Linux `/proc/self/status` 实测 RSS 增量约 5.8 MiB，CI gate 上限 24 MiB（低于事务 payload），源目标最终逐行一致。
- **磁盘 high-water 自动恢复** ✅ 两个已 spill 事务在 96 KiB high-water 下稳定进入 `RESOURCE_WAIT`；batch deadline 会先 apply/retire 已完成 spool，再以保留的同一 WAL message 继续，resource wait 自动清除、job 不重启、snapshot generation 不变且不产生 rebuild operation。
- **长 target apply source 心跳** ✅ target apply 期间每 10 秒发送一次旧 durable LSN standby status，直到 apply 成功后才 cleanup/ACK；修复大事务 apply 超过 PostgreSQL `wal_sender_timeout` 时连接关闭的问题，暂停时间单测证明 25 秒 apply 中不提前 ACK。
- **Bounded snapshot 崩溃恢复** ✅ 完整 runtime 在第一页 target COPY+cursor 提交后注入 fatal error，确认 S0 slot 被删除但旧 loading group/progress/shadow 保留；源端再插入位于旧 cursor 前的 `id=0`，重获 lease 后由新 fence 清理旧 group 与物理 OID，创建新 slot/S1 从表头重拉。测试验证新 group/LSN/checkpoint、无 loading 残留、无 rebuild operation，且源目标逐行完全一致。

- **1.3 runtime 接入 bounded snapshot** ✅ `crates/engine/src/runtime/job.rs`：`load_table_snapshot` 按 PK page 循环（`read_canonical_pk_page` → `copy_range` → `copy_text_pk_range` → target `SnapshotPageLoader::apply_page` 同事务 cursor）；无 PK 表 fallback 整表 COPY。target 侧 `begin_snapshot_pages`/`SnapshotPageLoader`（`crates/target-cloudberry/src/snapshot.rs`）。含集成测试 `cloudberry21_snapshot_paging_with_resume`。**源侧 PK 分页已在真实 PG18 验证通过**。

**Phase 1 关闭说明：** transaction spool 在 target checkpoint 持久后立即幂等 retire；重启时先证明 slot 可从 checkpoint 重放，再清理同 identity 的中断残留；新 topology generation 清理旧目录。spool 不承担审计留存，避免额外 retention 配置和无谓磁盘占用。最大测试事务约 32 MiB，相对 64 KiB memory high-water 超过 512 倍，RSS 增量约 5.7 MiB；snapshot post-commit 与 5 个 CDC kill 点全部收敛；磁盘 high-water 可自动退出 `RESOURCE_WAIT` 且不触发 rebuild。真实组合 E2E 最近耗时约 105 秒。

### Phase 2 — DDL 紧密跟随 🔶 进行中（本轮 2026-07-22 打下基础）
已完成（构建块 + 单测，见 `docs/phase2-ddl-tight-follow-adr.md`）：
- **DDL 分类** ✅ `DdlMessage::replication_impact()`（`core/src/change.rs`）：CREATE INDEX/GRANT/ANALYZE 等证明无害命令不再触发 rebuild（fail-closed 白名单）。
- **DDL event v2 类型** ✅ `TableTransition`/`TransitionKind`（AddColumn/DropColumn/RenameColumn/AlterColumnType/AddTable/DropTable/Unknown），`is_online_safe()` 保守判定；`DdlMessage.transitions`（`#[serde(default)]` 向后兼容 v1）。
- **DDL event v2 source 捕获** ✅ prefix/version 升级为 `pg2cloudberry_ddl_v2`/2（v1/1 仍可解码）；v6 event trigger 在 `ddl_command_end` 捕获 typed after-schema，在 `sql_drop` 捕获 DropTable。真实 PG18 已验证同事务 ADD→RENAME→varchar widen→DROP 的四个有序中间快照，以及 CREATE/DROP table。
- **spool DDL 完整性与有界内存** ✅ spool format v2 显式保存 transition/after-schema，修复旧 wire 静默丢失 `DdlMessage.transitions`；typed snapshot 字节计入 memory high-water，spill 后相邻重复 capture message 仍以固定 SHA-256 identity 去重。旧格式 artifact 只在 WAL replay 证明后直接清除，不作为恢复权威。
- **schema-diff 分类器** ✅ `core/src/schema_diff.rs::classify_table_diff`：按 attnum 对比 before/after schema → TransitionKind；PK 变化/narrowing/未知 → `Unknown`（rebuild）。engine 侧 `TableBindingRegistry::classify_relation_diff` 接入。
- **schema_events ledger** ✅ target V8 migration + `target-cloudberry/src/schema_event.rs`（record/load/list_unfinished/advance_state，forward-only 状态机）+ 集成测试。
- **dynamic binding registry** ✅ `TableBindingRegistry` insert/remove/swap（运行时可 swap binding，维持唯一性不变量）。
- **SchemaBarrier 结构化** ✅ 带 `command_tag`；online-safe 的 v2 DDL 在 barrier reason 中标注可跟随。
- **事务级 schema planner** ✅ `engine/src/schema_transition.rs` 对 memory/spool change source 统一流式扫描，保留 transaction ordinal，只以每 relation terminal captured state 对齐一次提交后 source catalog；v1/TRUNCATE/unknown scope 和 rapid-advance/catalog drift 均 fail closed，不推进 checkpoint。
- **fenced schema-event 持久化** ✅ 一个 source transaction 对应一个确定性 UUIDv8/JSON payload；写入、exact replay、unfinished adoption 和状态推进均锁 active target pipeline fence。真实 PG18 -> Cloudberry 2.1 E2E 覆盖 catalog exact match、后续 DDL mismatch、重复 WAL 和新 lease 接管。
- **runtime durable schema barrier** ✅ batcher 在 DDL 前后都切 batch，schema transaction 不再与后续 DML 混批；standalone sink 使用独立 source SQL 连接在任何 target data/checkpoint 写入前校验 terminal catalog 并落 ledger。当前 table handler 尚未接入时，runtime 先原子推进 event 为 `failed`，再调用旧 pipeline rebuild fallback；真实完整 runtime E2E 证明 checkpoint 不越过 DDL、目标无半应用 schema、且只创建一次 rebuild operation。
- **bound capability plan** ✅ barrier 在同一个 source repeatable-read snapshot 内校验 terminal fingerprint 并按 relation OID 读取完整 `TableSchema`，以 active binding 为 before-schema，确定性生成 `Noop / Online / Reload / Drop / Add`；v1/TRUNCATE 保持表级 reload，未知范围和 rapid-DDL/catalog mismatch 继续 pipeline fail-closed。schema diff 已补齐 table identity、kind、replica identity、distribution/partition、nullability、generated/identity/collation 的保守判定，避免把未覆盖 shape 误当 `Noop`。
- **table transition ledger** ✅ target V9 `table_schema_transitions` 按 event/relation 持久化完整 capability action、barrier LSN 和 fenced 恢复状态，并为执行期 generation/shadow group 提供持久字段；动作特定状态机拒绝跳步。compact fingerprint 与完整 `TableSchema` 在同一个 source repeatable-read snapshot 中读取，schema event 与全部表 action 在同一个 target transaction 中提交。
- **Noop schema executor** ✅ 能证明最终不涉及 managed table 的 schema transaction（例如同事务 CREATE→DROP）不再触发 rebuild；table transitions、schema event、该 source transaction 的 DML/chunk receipt、checkpoint 与 ledger retirement 在 final target chunk 同一事务完成。managed table 的“模型内无 diff”仍保守 reload，因为当前完整 catalog 模型尚不包含 default/check/reloptions。
- **reload identity allocation** ✅ capability ledger 使用 binding 中独立于 pgoutput wire cache 的持久 table generation：Online/Reload 分配 `active+1`、Drop 保留 active、Add 从 1 开始；reload/add 可在 fenced target transaction 中幂等绑定唯一 snapshot group 并进入 `snapshotting`。实际 COPY/catch-up/cutover 尚未接入，不能视为 table reload 已可用。
- **table-local target cutover** ✅ 新 activation 只 quarantine/替换请求内表，不处理无关 active 表、不推进 node checkpoint，并提供 caller-owned transaction 入口以便和 transition/event ledger 原子收口。runtime resume 改为校验精确 active managed-table 集合并从 target 恢复各表持久 generation，不再要求所有表来自同一个 snapshot group。source snapshot、WAL catch-up、reconciliation 和 runtime executor 仍未完成。

**未完成（下一位优先处理，按顺序）：**
1. **table handler + dependency closure。** capability plan、V9 持久状态和 Noop executor 已完成；下一步实现 Online/Reload/Add/Drop executor 与依赖 closure，并把其余 `failed -> pipeline rebuild` fallback 替换为局部处理。未完成事件不得越过 checkpoint/ACK。
2. **table barrier + shadow reload / catch-up / cutover。** 用 `begin_snapshot_pages` 重载单表 shadow → 从 barrier 后 spool 回放该表 CDC → reconciliation → 原子 quarantine/activation + `registry.swap`。`schema_events` 状态随 target 变更同事务推进。
3. **在线白名单 handler 接入。** 逐项验证 ADD/DROP/RENAME/default/nullability/widening 的 Cloudberry 2.1 capability；任一前置条件失败自动转受影响 table/dependency closure reload，不升级整 pipeline。
4. **rapid DDL、DROP quarantine + 新表自动准入。** catalog 比 event 超前时合并连续 committed schema transactions 后重算 terminal plan；无法证明完整范围则局部 quarantine/reload 并阻断 checkpoint。

**Phase 2 退出条件：** 并发 DML+DDL、同事务多次 DDL、rapid DDL、rename/drop/recreate、目标 commit ambiguity、进程重启和重复 WAL 矩阵通过；普通 DDL 不调用 `request_pipeline_rebuild`。

### Cloudberry 业务表存储 ✅ 进行中（AOCO 默认）
- 业务表与 snapshot shadow 通过 `TargetStorage` 统一规划：默认 `ao_column`（zstd level 1）；`pax_experimental` 只允许显式按表评估；业务表禁止 heap，metadata/staging 保持 heap。完整约束见 [cloudberry-storage-profile.md](cloudberry-storage-profile.md)。
- storage profile 进入 managed-table fingerprint；恢复时从 `pg_am` 校验 access method，已有 heap 或其他格式与期望 AOCO/PAX 不一致会在 CDC 写入前请求新的 snapshot generation，避免混用物理格式。
- AOCO 承担完整 current-state/type 集成验证。PAX 只保留独立实验 smoke test，不声明完整 SQL、类型、并发和恢复支持；显式启用时启动探测 `pg_am`。

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
