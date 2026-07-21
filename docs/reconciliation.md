# PostgreSQL 到 Cloudberry 的一致性校验与修复设计

> 状态：设计文档。本文不表示 reconciliation/repair 已经实现，也不解锁
> Physical HA 或 Citus 数据面。当前实现只提供 `crates/engine/src/reconcile.rs`
> 中 validation-gated 的 page/range、digest 和 bounded diff 纯原语；它不获取 fence、
> 不建立 source snapshot、不写 target，也不持久化 cursor。下面的 target/source session API
> 仍是后续实现契约。

## 目标与边界

本模块只处理已经完成 snapshot、并且有一个 active generation 的普通受管表。它的目标是
在不依赖历史时间旅行的前提下，发现并修复源 PostgreSQL 18 与 Cloudberry 当前表状态的
差异。允许校验期间存在脏读、跨表不一致和重复应用；在源停止写入、DDL 稳定且 WAL
连续时，数据必须收敛。

本阶段的硬前提如下：

- 表有普通 primary key，且 key 的 equality、排序和 collation 在两端已通过能力矩阵验证。
- 目标 namespace 不接受外部 DML。
- 运行中的 pipeline 使用同一个 source identity/timeline；failover 或 identity drift
  先走现有 rebuild/barrier 路径。
- 只为 `standalone` 拓扑创建 runner。Physical HA、Citus worker 多节点 reader 在各自的
  数据面完成前保持 validation-gated。

## 当前代码盘点

可以直接复用的部分：

- `engine::reconcile::Page`、source-derived `KeyRange`、`PageLimits` 以及 source/target reader
  契约；target 不能再独立选择自己的 LIMIT 区间。
- `DigestContext`、schema/type-domain SHA-256 编码、canonical row 校验和 bounded merge diff；
  `Binary`、`UnchangedToast`、NULL key、arity/顺序错误和预算超限都会 fail closed。
- `TableSchema::primary_key`、`runtime::planning::PlannedTable`、schema fingerprint。
- `target-cloudberry::apply::plan_apply` 生成的 `ApplyPlan`、`StageOperation`、
  `StagingRow`、COPY 编码和 staging SQL。
- `target-cloudberry::managed::lock_active_apply_table(s)`。CDC apply 已经先锁
  `pipeline_state`，再验证受管表 identity/fence/relation OID 并锁实际 relation；repair session
  应复用相同锁顺序，但还必须增加实际列/PK 结构校验。
- `source-postgres::snapshot` 的 repeatable-read/read-only 会话和固定文本输出设置。
- `PipelineTelemetryHandle`、job 的 `CancellationToken`、WAL retention monitor，以及
  `PostgresCloudberryJob::request_rebuild`。

不能直接复用的部分：

- 没有真实 source/target page reader。尤其缺少 PG typed keyset SQL、物化前字节预算、
  catalog admission 后的 typed PK comparator 和跨 PG18/Cloudberry canonical text golden matrix。
- 现有纯原语不携带 target fence、checkpoint、source identity/timeline、table generation 或
  snapshot lifetime，不能单独作为 authoritative repair API。
- `execute_apply` 必然推进 node checkpoint；repair 不能调用它，否则会产生 checkpoint
  超前于数据的不可恢复状态。
- target 没有“锁 fence 但不推进 checkpoint”的 RAII repair transaction；source 没有按
  PK page 查询的 canonical text reader；target 没有 canonical session settings。
- 没有持久化 cursor、repair 结果、重试次数和 source snapshot 观察点的 target metadata。

## 核心安全不变量

### Fence-first 时序

一个 authoritative page 必须严格按下面的顺序执行：

```text
target BEGIN
  -> SELECT pipeline_state ... FOR UPDATE (校验 generation/token)
  -> 锁定并校验 managed_tables/实际 relation
source BEGIN REPEATABLE READ READ ONLY
  -> 固定 canonical session settings
  -> 读取一个 source PK page
target 读取同一个 PK range
target staging COPY + delete/upsert
target 在同一事务内复读并校验 digest
source ROLLBACK/结束快照
target 写 reconciliation state 后 COMMIT
```

`execute_apply` 也先锁 `pipeline_state`，因此这个行锁同时阻塞当前进程、另一个 active
实例和新 owner 的 target apply。目标 checkpoint 在整个 repair transaction 内保持不变。

设锁取得时目标 checkpoint 为 `C`。source session 在锁之后建立，所以其 MVCC snapshot
至少能看到 source 上已经对应 `C` 的提交，并可能看到 `C` 之后的提交 `S`。repair 把目标
写到 `S` 不会造成永久回退：锁释放后，逻辑流仍从 `C` 重放，`U/D` 以 primary key
幂等应用，重复事件最终得到 source 的最新状态。若 source 在 snapshot 建立后继续提交，
这些事件同样留在 `C` 之后的 WAL 中。这个证明依赖 source identity 正确和目标没有外部写入。

不要为了周期性校验创建 logical slot；额外 slot 会保留 WAL，且并不能比 fence-first
顺序提供更好的 repair 安全性。审计可记录 `target_checkpoint_lsn`、事务中观察到的
`pg_current_wal_lsn()` 和事务 snapshot 标识（例如 `txid_current_snapshot()`，按服务器版本
选择等价函数）；观察到的 LSN 不是跨端 equality boundary，不能单独用来跳过上述顺序。

### DDL 与 generation

在拿 target fence 之前可以做便宜的 catalog preflight；拿到 fence 后必须在 source snapshot
中再次确认：relation OID/name、列顺序、PK、类型/typmod、collation、partition/Citus 属性
和 schema fingerprint 仍与 `PlannedTable` 相同。任一项变化、catalog 查询失败或 WAL relation
generation 不匹配，都只允许 rollback 并请求 rebuild，不能尝试“修一部分旧列”。

target 侧必须同时验证：

- `managed_tables.state = 'active'`；
- pipeline、source relation id、table generation、schema fingerprint 和当前 fencing token
  与本次 plan 一致；
- `relation_oid` 非空且仍对应目标表，实际列/PK 结构与 plan 一致。

legacy 没有 `relation_oid` 的 metadata 记录先升级/重建，不能由 repair 猜测对象身份。

## PK range paging

### Page 形状

reader 不应再返回单纯的 `Vec<CanonicalRow>`，而应返回：

```text
Page {
    rows: Vec<CanonicalRow>,
    has_more: bool,
    next_key: Option<Vec<Bytes>>, // rows 最后一行的 canonical PK text
}
```

source 查询取 `chunk_rows + 1` 行，用第 `chunk_rows + 1` 行判断 `has_more`，再丢弃 lookahead。
对复合 PK 使用 source 原生 typed row comparison 和原生 PK order：

```sql
SELECT "c1"::text, "c2"::text, ...
  FROM "schema"."table"
 WHERE ROW("pk1", "pk2") > ROW($1, $2)       -- 有 start 时
 ORDER BY "pk1", "pk2"
 LIMIT $limit_plus_one
```

target 查询使用同一个 source-derived boundary：

```sql
SELECT "c1"::text, "c2"::text, ...
  FROM "target_schema"."table"
 WHERE ROW("pk1", "pk2") > ROW($1, $2)       -- 有 start 时
   AND ROW("pk1", "pk2") <= ROW($3, $4)      -- 有 end 时
 ORDER BY "pk1", "pk2"
 LIMIT $repair_limit_plus_one
```

参数值是 source `::text` 输出，建议实现一个仅供内部使用的 text-format `ToSql` 参数类型，
让 PostgreSQL/Cloudberry 根据左侧列推断真实 key 类型。这样既不需要把 enum/domain/array
类型 renderer 重复到 engine，也保留 PK 索引；若 Cloudberry parser 对匿名 row 参数推断不
兼容，再退回按 `PgTypeKind` 生成显式 cast，并在集成测试中锁定 SQL。

source page 满且有 lookahead 时，target range 是 `(start, end]`；source 已到尾部时，range
是 `(start, +infinity)`；source 返回空 page 时也执行尾部 range，以发现并删除 target-only
rows。每个 reader 必须检查结果严格按 key 递增、没有重复 key、key 不为 NULL，发现违反即
进入 contract failure/rebuild。

不要让 source 和 target 各自独立 `LIMIT N` 后再比较；那是当前 `compare_chunk` 的主要
语义缺陷。PK ordering/collation 不可证明兼容的表在 catalog admission 阶段拒绝，而不是
改用 locale-dependent 的字符串排序。

### Cursor

cursor 只是扫描进度优化，不是数据正确性的依据。它至少包含 pipeline、topology generation、
source relation id、table generation、schema fingerprint 和最后一个 PK 的 canonical text
数组。任何 identity/fingerprint 变化都清空 cursor。只有 page repair/校验事务成功提交后
才推进 cursor；进程在 commit 前崩溃时重做同一 range 是安全的。尾部完成后将 cursor 置空
并递增 cycle，保证在整个周期中发生的“key 移到已扫描区间”会在下一周期再次检查。

## Canonical row 与 digest

canonical reader 统一执行 `column::text`，并在 source/target 两端固定：UTF-8、UTC、ISO
DateStyle、Postgres IntervalStyle、`extra_float_digits=3`、`bytea_output=hex`。结果只允许
`NULL` 或 UTF-8 text；reconciliation 不接受 pgoutput `UnchangedToast` 或 binary cell。

digest 建议升级为 `digest_rows(plan, rows)`，输入至少包括：

1. 版本域（例如 `pg2cb-reconcile-v1`）和 portable schema fingerprint；
2. key/value 列的 ordinal 与递归 type tag（OID 不参与，domain/array/enum 使用规范化身份）；
3. 每行的 key arity、每个 key 的长度前缀和值；
4. 每行的 value arity、NULL marker 或 type-tagged bytes；
5. row count 和固定长度编码的所有字段。

行顺序必须是 PK 顺序。`Cell::Null`、空字符串、文本 `\\N` 和不同 type tag 必须产生不同
编码；不依赖 locale、显示小数位或 JSON 输入字符串的偶然格式。digest 相等才走快速路径；
不相等时再按 key 建 bounded map，产生精确的 `Upsert(source row)` 与
`Delete(target-only key)`，而不是把整个表重写。

## Target repair transaction 契约

后续 `target-cloudberry::reconcile` 应提供类似 snapshot 模块的 RAII API（本文不实现）：

```text
begin_repair_chunk(client, fence, table_identity, limits) -> RepairSession
RepairSession::read_range(start, end, limit) -> Vec<CanonicalRow>
RepairSession::apply_rows(Vec<StagingRow>) -> ApplyStats
RepairSession::record_state(update)
RepairSession::commit()
RepairSession::rollback()
```

`begin_repair_chunk` 必须先开启 transaction、调用 `lock_pipeline_fence`，再锁/校验
`managed_tables` 和实际 catalog。`apply_rows` 复用 `ApplyPlan` 的 staging COPY、validation、
delete/move/update/insert SQL，但只允许 `Upsert`/`Delete`，不能调用
`advance_node_checkpoint`。所有数据、repair state 和统计信息在同一 target transaction
提交；事务 drop 或取消时全部 rollback。

target 读/写 session 都设置 bounded `statement_timeout`、`lock_timeout` 和必要的
`idle_in_transaction_session_timeout`。超时必须让 server transaction 结束并释放 fence，不能
把一个长期 query 留在后台。target transaction commit 结果不确定时不推进 cursor，重连后
重复该 range；PK 幂等性保证重试不会扩大差异。

## Runtime 数据流

`PostgresCloudberryJob::run_standalone` 在 prepared snapshot、publication、sink 建立后，
创建一个与 job cancellation 同生命周期的 `ConsistencyRunner`。runner 使用独立 source
SQL client 和独立 target client，不共用 replication connection；每次只处理一个 bounded page，
page 间在 fence 外 sleep/限速。

推荐循环：

1. 检查 cancellation、WAL hard limit、当前 source/target lag 和当前 generation；lag 太大时
   暂停 reconciliation，让 CDC 先追平。
2. 选择 round-robin 的 active table 和其 cursor。
3. 执行上面的 fence-first source snapshot/page/range/repair/verify。
4. 提交 state/cursor，更新 pipeline 级 telemetry，然后释放 fence。
5. 达到 `max_chunks_per_cycle` 或时间预算后退出本轮，按 interval+jitter 再继续。

runner 的结果分三类：`Cancelled` 是正常退出；catalog/fence/identity/重复不收敛等
consistency barrier 请求一次 `request_rebuild`；连接断开、超时、死锁等 transient error
保留 cursor、指数退避并继续。不能让一次普通校验失败直接结束整个 pipeline，也不能无限
自动 bump generation；同一 fingerprint 连续失败达到阈值后才升级 rebuild，重建再次失败则
保持 degraded 并等待人工处理。

## 持久状态、限流和观测

状态应放在 target metadata（而不是 control-plane 热路径），因为它要和 repair 数据、fence
一起提交。后续 target migration 可增加 `pg2cb_meta.reconciliation_state`，主键为
`(pipeline_id, target_schema, target_table)`，至少包含：generation/fingerprint/relation_oid、
cursor、cycle id、last source snapshot observed LSN、target checkpoint LSN、last result/time、
scanned/repair row counters、consecutive failures、fencing token 和 bounded error text。

建议的 pipeline 设置（所有字段都要有硬上限并拒绝 0）：

| 设置 | 初始建议 | 作用 |
| --- | ---: | --- |
| `reconciliation.enabled` | `true` | 是否执行周期校验 |
| `interval_seconds` | `300` | 周期之间的等待，叠加少量 jitter |
| `chunk_rows` | `4096` | 每个 fence 持有期间的最大 source rows |
| `chunk_bytes` | `16 MiB` | Rust materialization 的内存/网络预算 |
| `max_repair_rows` | `65536` | range diff 上限，超过则 rebuild |
| `max_chunks_per_cycle` | `16` | 防止一个大表独占 pipeline |
| `max_pause_ms` | `2000` | fence 持有时间预算，必须显著小于 lease renewal interval |
| `statement_timeout_ms` | `max_pause_ms` 的 2 倍以内 | server 侧 query 上限 |
| `max_lag_bytes` | `64 MiB` | lag 超过时暂缓校验 |
| `rebuild_after_failures` | `3` | 同一 range/fingerprint 连续不收敛后升级 |

`max_pause_ms` 不能覆盖 source snapshot、target COPY 和 commit 的总预算；应在 runtime
启动时与 lease renewal interval 交叉校验。任何 sleep、I/O token bucket 或 backoff 都必须
发生在 fence 释放后。已有 WAL retention monitor 进入 warning/hard 状态时，runner 只保留
状态并让 CDC/rebuild 优先，不能因 reconciliation 扫描继续制造 WAL backlog。

telemetry 只增加 pipeline 低基数汇总：`reconciliation_in_progress`、last success time、
last result、mismatch/repair counters、last error 和 overdue 标志；表名、key 和 digest 不
作为默认 metric label。详细 table state 通过受保护 API 查询，日志只记录 pipeline/table
身份和 digest 前缀，不记录实际 PK 值。

## 失败恢复语义

| 情况 | 动作 |
| --- | --- |
| cancellation、lease/fence stale | 立即停止读取，rollback target tx，不推进 cursor |
| source/target 网络、statement timeout、死锁 | rollback，保留 cursor，指数退避后重试 |
| catalog/schema/fingerprint/relation identity drift | 不写业务表，记录 barrier 并请求 shadow rebuild |
| source page/target range 有重复、NULL PK 或无法 canonicalize | contract failure；阻塞该 generation，不猜测修复 |
| diff 成功且复读 digest 相等 | 同一事务提交数据和 state，推进 cursor |
| diff 后仍不相等 | rollback；最多重试阈值次，随后请求 rebuild |
| target-only rows 超过 repair 上限 | 不批量盲删，直接请求 shadow rebuild |
| commit 结果不确定 | cursor 不前进，重连后幂等重做 range |
| source slot lost/WAL hard limit | 由现有 WAL monitor 触发 rebuild，runner 不绕过 checkpoint |

每次重试都重新取得 fence、重新建立 source snapshot；不能复用可能早于当前 checkpoint 的
旧 source rows。任何已提交的 repair 都不推进 WAL checkpoint，因此 CDC 重放仍是最终收敛
的权威路径。

## 测试与生产门槛

### 单元测试

- digest 的版本域、schema/type tag、NULL/空值、长度前缀、行顺序和 `UnchangedToast` 拒绝。
- 首页/中间页/尾页/空 source 尾部、复合 PK、quoted identifier、lookahead 和 cursor reset。
- source-only、target-only、值变化、相同 key、PK move 的 diff；重复 key 和超限保护。
- text-format parameter 的类型推断；若使用显式 cast，覆盖 enum/domain/array/typmod。
- repair state machine 的 backoff、取消、commit ambiguity 和 rebuild threshold。

### PG18 -> Cloudberry 真实集成

- source/target 端所有已支持类型、NULL、TOAST 后完整行的 canonical text 相等性。
- 复合 PK、确定性 collation、quoted schema/table/column；`EXPLAIN` 确认 keyset 使用 PK
  索引而不是每页全表排序。
- 人为制造 source-only、target-only、update mismatch、PK move，确认 repair 后 exact match，
  且 `node_checkpoints.applied_lsn` 没有推进。
- 在 source snapshot、target COPY、commit 前后分别注入 insert/update/delete；释放 fence 后
  WAL 重放必须收敛且不能被旧 snapshot 永久回退。
- DDL/DDL barrier、relation generation/fingerprint drift、target 外部 ALTER 和 relation OID
  变化均不得写业务表，并进入 rebuild/barrier。
- stale owner、lease loss、target restart、网络中断、statement timeout、commit ambiguity；
  新 token 能取得 fence，旧事务最终 rollback/结束。
- 大行、大 range、target extras 超限、WAL warning/hard limit、取消和重启后的 cursor resume。
- 校验周期不创建额外 logical slot；所有临时连接/事务/表在成功和失败路径均释放。

### Soak 与验收

在源停止写入、DDL/topology 稳定后，连续完成至少两个 full cycles：每个 active table 的
PK 集合、受支持列值、schema fingerprint 和最近一次 range digest 均相等。再运行持续写入
和故障注入 soak，确认 source WAL、fence pause、repair bytes、CPU 和目标 apply latency
都在配置上限内。只有这组证据齐全后，才把 automatic repair 从 validation-gated 改为
默认生产能力；在此之前，runner 即使实现也必须保持可关闭并清楚显示 degraded/overdue 状态。
