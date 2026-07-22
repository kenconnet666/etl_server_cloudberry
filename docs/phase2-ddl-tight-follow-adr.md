# Phase 2: DDL 紧密跟随实施设计

> 状态：进行中。权威架构决策见 `adr/0005-online-schema-evolution.md`；本文记录当前代码、下一里程碑和验收顺序。

## 目标与不变量

普通 DDL 不再触发整条 pipeline rebuild。只有能够证明安全的变更才原地跟随；其他变更自动重拉受影响 table 或共享依赖 closure。slot/WAL、source identity 或 topology coverage 失真时才允许整 pipeline rebuild。

PostgreSQL 源事务是 schema event 的原子边界。一个事务内可以有多条 DDL 和 DML，target checkpoint/ACK 不能越过未完成的 schema event，但其他 table 可以继续进入有界 spool 或 apply。

## 已落地基础

- DDL 分类、schema diff、`TransitionKind` 和保守 online-safe 判定。
- target V8 `pg2cb_meta.schema_events` ledger，状态为 `pending -> in_transition -> completed|failed`。
- `TableBindingRegistry` 的 insert/remove/swap 与唯一 target/staging 不变量。
- v2 source capture、legacy v1 解码、spool format v2 typed transition round-trip。
- 真实 PG18 覆盖同事务 ADD、RENAME、varchar widening、DROP 的四个中间 after-schema，以及 CREATE/DROP TABLE。

## DDL Event v2

当前 wire prefix/version 为 `pg2cloudberry_ddl_v2`/2。payload 包含：

- `command_tag`、`relation_ids`、`affected_schemas`、event fingerprint；
- 每个持久 table-like relation 的 `relation_id`、after-fingerprint、typed after-schema；
- CREATE TABLE 标记 `AddTable`；DROP TABLE 标记 `DropTable` 且没有 after-schema；
- 其他 surviving table DDL 在 source 保持 `Unknown`，由 engine 用 bound before-schema 分类。

typed after-schema 覆盖 relation name/kind、replica identity、stable attnum、列名、类型 OID/name/kind/typmod、nullability、generated/identity、collation、default、PK 和 partition attnum。它刻意低于完整 `TableSchema`：domain/enum/array、Citus 属性、依赖 closure 和 target capability 由提交后的 catalog planner 解析。

v1 prefix/version 继续解码，但缺少 typed after-schema，必须进入受影响 scope 的 shadow reload，不能猜测在线能力。

## 事务级规划

`schema_events` 每个已提交 source transaction 只写一行，幂等键为 `(pipeline_id, source_lsn, source_xid)`；ordered DDL messages 及其 transitions 作为一个 JSON payload 持久化。这样与 PostgreSQL commit、target completion gap 和重复 WAL 的边界一致。

planner 按以下顺序工作：

1. 按 transaction change ordinal 收集全部 DDL/TRUNCATE，并保留中间 after-schema。
2. 对每个 relation 找到 terminal post-state；DROP 的 terminal state 是不存在。
3. 事务提交后读取一次权威 catalog，要求 terminal state/fingerprint 对齐。中间快照不能逐条与最终 catalog 比较。
4. 用 active binding 的 before-schema 与 terminal catalog schema 做 diff，并补齐类型、collation、partition、Citus 和依赖 closure。
5. 将完整计划先写入 `schema_events.pending`，再选择 online apply、table/closure reload、quarantine 或 blocked retry。

若 event terminal state 与 catalog 不一致，说明 rapid later DDL 已经越过当前事件。planner 必须按 LSN 顺序合并/重规划到可验证的最新 terminal state，不能把旧快照当当前 catalog，也不能提前 ACK。

## Binding 发布

Phase 2 的 standalone applier 仍是单序列数据路径，registry 只在 source transaction/table cutover 边界由 coordinator 可变更新，row hot path 不查 catalog。当前不引入每行 `RwLock`。

未来 Phase 3 并行 table applier 需要并发 binding 时，再通过基准和故障矩阵选择 `ArcSwap`/RCU 或短临界区锁；旧 binding 必须由 inflight batch 持有到 target commit 完成。

## Table Transition

保守默认流程：

1. 持久化 schema event 并建立该 table 的 completion barrier。
2. 创建 pending generation 和 typed shadow。
3. 复用 bounded `begin_snapshot_pages` 从新 source snapshot 重载。
4. 从 barrier 后 spool 按 WAL 顺序 catch up 该 table。
5. 执行 reconciliation；不一致继续重拉或 blocked retry，不能带差异 cutover。
6. 一个 Cloudberry transaction 内校验 fence，旧表进入 quarantine，shadow 激活，metadata/generation 切换。
7. target commit 成功后发布新 binding，完成 event 和 completion gap；随后才推进 checkpoint/ACK。

崩溃恢复按 source LSN 加载 `pending|in_transition`，检查 target metadata/物理对象后幂等继续。commit ambiguity 必须通过 ledger 和对象 identity 判定，不能盲目重做 RENAME。

## 在线候选与 fallback

在线候选逐项开放：nullable/defaulted ADD COLUMN、DROP/RENAME COLUMN、SET/DROP DEFAULT、经过数据验证的 nullability、明确白名单 widening、enum append。每项都需要真实 Cloudberry 2.1 capability test、target DDL 幂等证明和 crash matrix。

PK/distribution/collation 改变、不兼容类型、generated/partition/table kind、TRUNCATE、复杂同事务 shape、v1 event 或任何未知条件走 table/closure shadow reload。DROP TABLE 进入 quarantine；CREATE TABLE 走准入、snapshot、catch-up、activation。

## 里程碑

### M1：v2 捕获和传输（已完成）

- [x] source v2 typed after-schema 和 v1 compatibility
- [x] spool v2 完整 round-trip、格式升级恢复证明、typed DDL memory spill
- [x] 同事务多 DDL、CREATE/DROP 的真实 PG18 测试

### M2：事务 planner 和 ledger 接入（进行中）

- [x] memory/spool 统一流式 planner、terminal-state catalog validation、确定性事务 payload/event identity
- [x] 事务级 `schema_events` exact replay、active fence 校验、未完成事件新 lease 接管
- [x] DDL transaction 独占 batch，runtime 在 checkpoint 前持久化 event；handler 未接入期间先标记 failed 再走兼容 rebuild
- [x] bound before/after capability plan：完整 catalog `TableSchema` + active binding，输出 `Noop / Online / Reload / Drop / Add`
- [ ] rapid-DDL coalesce/replan 和依赖 closure
- [ ] table completion barrier 与 online/reload handler，替换普通 DDL 的 pipeline rebuild 分支

### M3：shadow reload 和 cutover

- [ ] bounded table snapshot、spool catch-up、reconciliation
- [ ] quarantine/activation/binding swap 的 fence 与 commit ambiguity 恢复
- [ ] DROP quarantine 和新表自动准入

### M4：在线白名单和生产矩阵

- [ ] 逐项 Cloudberry 2.1 online handler
- [ ] 并发 DML+DDL、同事务 DML/多 DDL、rapid DDL、rename/drop/recreate
- [ ] process kill、重复 WAL、target commit ambiguity、RESOURCE_WAIT、soak

## Phase 2 退出条件

- 上述真实 PG18 -> Cloudberry 2.1 矩阵最终逐行/schema 一致。
- 普通 DDL 不调用 `request_pipeline_rebuild`；不安全变更只重拉受影响 table/closure。
- checkpoint/ACK 永不越过未完成 schema event，重启能从 target ledger 幂等恢复。
- row hot path 保持无 catalog 查询且资源有界，旧整 pipeline DDL 路径只保留给全局正确性损坏。
