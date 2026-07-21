# 开发交接文档

> 单一权威交接文档。取代早期的 PHASE0_PROGRESS / PHASE0_COMPLETE / PHASE1_PROGRESS
> （已删除）。每次换机或阶段推进后更新本文。

**最后更新:** 2026-07-22
**工作分支:** `origin/codex/phase1-durable-cdc`
**最新 commit:** `78f9a8c` Auto-reclaim superseded spool generations (Phase 1.4)

## 换机快速开始

```bash
git fetch origin
git switch --track origin/codex/phase1-durable-cdc   # 或已存在则 git switch + git pull
cargo build --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --lib
```

预期：编译通过，clippy 零 warning，**246 项单元测试全部通过**。真实数据库集成测试是 opt-in（见下"测试环境"），不在默认 `--lib` 范围内。

## 核心目标（不变）

源和目标数据最终一致；DDL 变更紧密跟随，无法安全跟随时重拉受影响表；在保证正确性前提下追求低延迟、高吞吐。允许中间过程短暂不一致，最终一致性有保证。源仅 PostgreSQL 18，目标仅 Apache Cloudberry 2.1（双向锁定）。完整方向见 [delivery-plan.md](delivery-plan.md) 与 [architecture.md](architecture.md)。

## 已拍板决策（摘要）

- **测试 CI:** GitHub Actions Linux CI 已接入（Phase 0）；跨 Windows/WSL 网络与压测保留本地。
- **版本锁定:** 只支持 PG18 ↔ Cloudberry 2.1，启动时 `SELECT version()` 严格校验，其它版本拒绝启动。
- **Spool:** 固定目录（`engine.spool_directory`，默认 `data/spool`），不设硬容量上限；按 topology generation 自动清理被取代的旧目录；磁盘水位到达时进入 `RESOURCE_WAIT` 背压，不提前 ACK、不丢数据。
- **Reconciliation:** Phase 1-2 差异一律触发 shadow rebuild；原地 repair 保持 capability-gated，直到 replay-compatible 协议对 PK move/delete/reuse/swap 的证明与故障矩阵成立。见 [reconciliation.md](reconciliation.md)。
- **Citus:** 数据面推到 Phase 4；Phase 2 末评估是否有真实需求提前到 Phase 3。discovery/catalog 已有，`Citus`/`PhysicalHa` 拓扑当前 fail-closed。
- **Migration:** control 与 target metadata 各自版本化、SHA-256 checksum 保护、启动时执行。已到 target V7。
- **Web UI:** 计划 Vue 3 + Naive UI + JWT（账号密码）。当前仍是占位 Svelte，重建推迟到数据面稳定后。

## 进度

### Phase 0 — 工程门禁与基础设施 ✅ 完成
- `.github/workflows/ci.yml`：fmt / clippy / build / lib+bins 测试。
- control migration（`crates/metadata/src/migration.rs`，到 V2）与 target metadata migration（`crates/target-cloudberry/src/migration.rs`，到 V7，含 `snapshot_table_progress`、`transaction_chunk_progress`、`transaction_committed_chunks`）。
- 版本校验：`source-postgres` `verify_pg18_version`、`target-cloudberry::version::verify_cloudberry_21_version`。
- `tests/integration/docker-compose.yml`：PG18(55432) + Cloudberry 2.1(55433)，含 init 脚本与 healthcheck。

### Phase 1 — Standalone 连续数据面 🔶 进行中
已完成（构建块 + 单测）：
- **1.1 Source keyset paging** ✅ `crates/source-postgres/src/snapshot.rs`：`read_canonical_pk_page`（`LIMIT+1` lookahead、typed `ROW(...)>ROW(...)`、`SnapshotKeyPage{has_more,next_key}`）、`read_canonical_row_page`、`copy_text_pk_range`。含真实 PG18 集成测试 `tests/snapshot_page_pg18.rs`（opt-in）。
- **1.2 Target snapshot progress** ✅ `crates/target-cloudberry/src/snapshot/progress.rs`：`register_snapshot_table_progress`、`copy_snapshot_page`（COPY 与 cursor 同事务）、完整 CRUD SQL、V7 schema。
- **1.4 Spool 自动清理** ✅ `crates/source-postgres/src/spool.rs`：`remove_superseded_generations` 在 `open` 时回收更低 generation 的目录，best-effort 非致命。

**未完成（下一位优先处理，按顺序）：**

1. **1.3 把 runtime 接到 bounded snapshot（最关键）。**
   `crates/engine/src/runtime/job.rs::prepare_initial_snapshot` 目前仍用整表
   `snapshot.copy_text_table(...)` + `apply.copy_from_stream`，**尚未**调用 1.1/1.2 的
   `read_canonical_pk_page` / `copy_snapshot_page`。需要改成按 PK page 循环：source 读一页边界 →
   target `copy_snapshot_page` 写同事务 cursor → 直到 `has_more=false`。并处理：
   - 同一存活 `SnapshotSession`/S0 内 target commit ambiguity 可按 cursor 续传；
   - 进程崩溃、S0 消失后**不得**沿旧 cursor 续传，必须新 slot/S1/L1 从表头重拉并清理旧 loading group
     （`snapshot::cleanup` 与 `open_after_wal_replay_verified` 已提供原语）。
2. **1.4 补充按时间的 spool 清理（可选增强）。** 现在只按 generation 回收。若要"checkpoint 后保留 N 小时再删当前 generation 内已 retire 的 journal"，需在 `SpoolLimits`/config 增加 `retention` 字段并在 checkpoint 推进后调用。当前 ENOSPC → `RESOURCE_WAIT` 背压已就绪。
3. **1.5 E2E kill-point 测试。** `tests/integration/` 下补半自动脚本，覆盖 5 个 kill 点：source read 后、spool write 后、target chunk commit 前、checkpoint commit 后/ACK 前、final chunk commit ambiguity。验证重启后数据最终一致（PK count + canonical digest）。

**Phase 1 退出条件：** 最大测试事务显著大于进程内存预算且内存保持水位内；上述 5 个 kill 点均收敛；磁盘 high-water 进入 `RESOURCE_WAIT`，扩容后继续且不触发 rebuild。

### Phase 2 / 3 / 4
DDL 紧密跟随 / 吞吐延迟与 soak / Citus 多节点。详见 [delivery-plan.md](delivery-plan.md)。

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
