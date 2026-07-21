# 完整形态交付计划

## 开发交接（2026-07-21）

远程工作分支：`origin/codex/phase1-durable-cdc`。换机后执行：

```text
git fetch origin
git switch --track origin/codex/phase1-durable-cdc
```

本轮已经落地：

- versioned binary transaction spool；内存越过水位后透明 spill，checkpoint 成功后、ACK 前清理；
- spool 使用量 O(1) 计数，实际 append/rotate/manifest ENOSPC 保留原消息并进入 `RESOURCE_WAIT`，等待期间每 10 秒只发送 durable LSN heartbeat；
- target stable manifest、半开 chunk range/digest、持久 `next_seq`、receipt 与 DML 同事务；
- completion 后同一 target transaction 推进 checkpoint 并回收 receipts/progress；提交响应丢失时由严格 identity 的 checkpoint fast path 跳过重复 DML；
- 每 node 的 transaction end LSN 跨 batch 严格递增；相同 LSN fail closed；
- PK delete/reuse、move chain 和 temporary-key swap 不再依赖 chunk 大小；
- CI 覆盖 `master`、Web check/test/build，以及真实 PG18 metadata/source/snapshot 门禁。

已验证：workspace 全量测试通过，其中 source unit 44 项、target unit 56 项、engine 81 项；真实 PG18 source/snapshot 3 项；真实 Cloudberry 2.1 chunk ledger 回收/重放 1 项；PK graph 的 3 项 Cloudberry ignored tests；API/config 与 Web check/test/build。

下一位开发者首先处理：

1. 合并 target commit 快路径。当前普通事务仍是 manifest、chunk、completion 三阶段提交；目标是单 chunk 1 次、N chunk N 次、空事务 1 次。final 调用必须同时处理 `Applied`、`AlreadyCommitted` 和 `ResumeAt == record_count`，并原子完成 checkpoint/retirement。
2. 为快路径补单 chunk、多 chunk、空事务、final commit 响应丢失、改 chunk 大小恢复和并发双提交测试。
3. 执行完整 workspace test/Clippy、全部 Cloudberry opt-in tests，再按 Phase 1 kill-point 矩阵推进 snapshot/reconciliation。

测试容器不是交付状态的一部分；本轮结束时会停止 `pg2cb-it-pg18` 和 `pg2cb-it-cb21`，不会操作 `ducklake-*`。用户曾在会话中暴露 GitHub PAT，该令牌没有写入仓库或 Git 配置，仍应在 GitHub 立即撤销并重新生成。

## 目标约束

本计划从最终产品反推阶段边界，不为 Standalone、Physical HA 和 Citus 维护三套数据面。

- source 仅 PostgreSQL 18，target 仅 Apache Cloudberry。
- 支持 Standalone、带物理 HA 的单逻辑节点和 Citus coordinator/worker。
- 当前状态最终一致；不提供历史、时间旅行或跨表/Citus 跨节点原子可见。
- 正常大事务不按事务字节数失败，内存有界并透明使用磁盘 spool。
- DDL 安全时在线跟随，无法证明安全时重拉受影响 table/依赖闭包。
- checkpoint/ACK、generation、identity 和 schema correctness 优先于可用性与吞吐。
- 阶段能力使用 capability gate；最终接口稳定后删除旧实现，不长期双轨。

## 最终运行模型

```text
SourceTopology
  -> NodeSet (1 standalone/HA node, N Citus nodes)
     -> NodeStream
        -> SpoolJournal
        -> CommittedTransaction/SchemaEvent
        -> Scheduler
           -> TableRuntime + SchemaCoordinator
           -> ChunkAppliers
        -> CompletionTracker
        -> target node checkpoint
        -> source ACK

ConsistencyRunner
  -> table PK-range digest
  -> bounded repair through the same table fence/generation
```

核心接口只有一套：

- `NodeSet/NodeStream` 隔离 topology discovery 与 pgoutput transport。
- `SpoolJournal` 隔离事务大小、协议 streaming 和内存策略。
- `CommittedTransaction` 暴露 metadata 与有界 change reader，不暴露完整 `Vec` 假设。
- `TableRuntime` 持有 relation incarnation、schema version、generation 和 active binding。
- `SchemaCoordinator` 处理在线 DDL、shadow reload、dependency closure 和 retry。
- `CompletionTracker` 允许 chunk/table/node 并行，但只发布连续 node LSN 前缀。
- `ConsistencyRunner` 不推进 WAL checkpoint，只在 target fence 下修复当前状态。

## Phase 0：工程门禁与契约

目标：让后续行为变化有稳定的测试、配置和迁移边界。

- 修复默认分支 CI、完整 Clippy、Web check/test/build。
- PG18 catalog/slot/snapshot/DDL/control lease 测试进入每 PR 门禁。
- 固定 Windows/WSL shell 行尾和可复现路径。
- API 保存前执行强类型 Source/Target/Pipeline settings 校验。
- 固化 ADR 0004/0005，定义 transaction/schema/topology 三层身份。
- 增加真实 PG18 -> Cloudberry E2E harness；Cloudberry/Citus 进入 nightly/release gate。
- 统一显式 control/source/target migration 和兼容性检查。

退出条件：默认分支 CI 全绿；容器可启动并通过 health smoke；所有 checked-in 配置能被严格验证；
测试环境不依赖开发者机器路径。

## Phase 1：Standalone 连续数据面

目标：在单节点上完成最终数据路径，而不是继续扩展旧 `Vec` assembler。

- 引入 `CommittedTransaction` metadata/change source 与 versioned spool journal。
- protocol v1 先实现透明 spill，取消事务 max bytes/changes 业务失败。
- committed transaction 从 spool 按 rows/bytes chunk apply。
- target 以稳定 manifest、record range/digest 和 durable `next_seq` 记录 chunk；receipt 与 DML
  同事务提交，不能只依赖 deferred checkpoint。
- node completion tracker 在完整事务完成后推进连续 checkpoint。
- snapshot 改为 PK chunk、并行 reader、持久进度和可恢复 shadow load。
- target 每个 apply 验证 table relation oid、generation、schema fingerprint 和 fence。
- 实现 Standalone reconciliation/repair runner。

退出条件：最大测试事务显著大于进程内存预算；内存保持水位内；在 source read、spool write、
target chunk commit、checkpoint commit、ACK 前后 kill 均可收敛；磁盘 high-water 进入
`RESOURCE_WAIT`，扩容后继续且不触发 rebuild。

## Phase 2：DDL 紧密跟随

目标：普通 DDL 不再重启或重建整条 pipeline。

- DDL event envelope v2、持久 schema event/version/transition metadata。
- catalog snapshot 与 type/collation/table/Citus dependency graph。
- table-level barrier、spool gap、shadow reload/catch-up/reconciliation/cutover。
- dynamic Arc/RCU binding registry；row hot path不执行 catalog 查询。
- 逐项启用 ADD/DROP/RENAME/default/nullability/widening/enum append 在线白名单。
- DROP quarantine、新表自动准入和依赖 closure rebuild。

退出条件：并发 DML+DDL、同事务多次 DDL、rapid DDL、rename/drop/recreate、目标 commit ambiguity、
进程重启和重复 WAL 矩阵通过；普通 DDL 不调用 pipeline rebuild。

## Phase 3：吞吐、延迟与长期运行

目标：在正确性模型不变的前提下释放并行能力。

- pgoutput protocol v2 streaming 直接写入 Phase 1 的 spool journal。
- table/node applier 并行，completion tracker 管理连续前缀。
- session-persistent staging 与 batch-local staging 用 Cloudberry benchmark 决定。
- source snapshot、WAL ingest、spool、COPY/apply、reconciliation 使用独立有界资源池。
- 建立 24/72 小时 soak、故障注入、磁盘/WAL/内存容量告警。

退出条件：达到已确定的 steady CDC throughput、snapshot throughput、p95/p99 lag、最大事务、
恢复时间和资源上限；吞吐提升不能改变 checkpoint/DDL/reconciliation 语义。

## Phase 4：Physical HA 与 Citus

目标：把已有 NodeSet/Spool/Schema/Completion 接口扩展到多节点，不复制 Standalone runtime。

- PG18 failover slot continuity 与 stable endpoint；不连续时新 topology generation。
- Citus coordinator/worker publication、slot、identity 和 per-node checkpoint vector。
- 首批只开放 PK 包含 distribution key 的 hash-distributed row table。
- Citus target 默认以 source distribution key 分布；完整 PK 仍是行 identity。
- coordinator DDL、worker relation fingerprint、placement/rebalance transition。
- worker add/drain/rebalance/split/failover 与 reconciliation capability gate。

退出条件：每个 node 的 ACK 只依赖自己的连续 checkpoint；不存在跨 node LSN 排序；并发分布式
CRUD、故障、rebalance 和 topology drift 的最终 PK hash 一致。

## 旧路径删除条件

| 旧路径/字段 | 删除条件 |
| --- | --- |
| transaction `max_changes/max_bytes` 触发失败 | Phase 1 spool/chunk/RESOURCE_WAIT 故障矩阵通过 |
| 完整 `Vec<TransactionChange>` 作为唯一事务载体 | 所有 source/sink/test 使用 bounded change reader |
| DDL -> `request_pipeline_rebuild` | Phase 2 table transition 通过 E2E/soak；全局 rebuild API保留 |
| run-scoped immutable `TableBindingRegistry` | dynamic binding 的 crash/replay/schema version 测试通过 |
| snapshot 整表单流 COPY | PK chunk progress、resume、activation ambiguity 测试通过 |
| 单 Cloudberry client/global apply 串行 | completion tracker 与 table fence 并发测试通过 |
| Citus validation-only discovery | Phase 4 node data plane 和 capability matrix 通过 |

## 测试与压测拓扑

功能和故障测试可以跨 Windows/WSL 虚拟局域网运行：宿主和 WSL 使用
`10.144.144.4/10.144.144.5` 的实际可达地址，不依赖不稳定的 localhost 转发。该模式验证 TLS、
断网、重连和跨边界部署。

峰值压测把 PG18、服务和 Cloudberry coordinator/segments 放在同一 Linux/WSL 实例内，使用
release binary、固定 CPU/memory、独立数据/spool 目录和本地网络，尽量排除虚拟交换与宿主网络
噪声。该结果衡量 engine/database 上限；另保留跨虚拟局域网基准衡量真实网络敏感度，二者不能
混为一个数字。

最小 workload 维度：

- 事务：单行、小批、超大事务、长时间 open transaction、commit/abort；
- 行：窄行、宽行、TOAST、NULL、PK move、热点 key、均匀 key；
- schema：在线白名单、shadow fallback、rapid DDL、共享 enum/domain；
- topology：Standalone、HA failover、Citus 多 worker/rebalance；
- 故障：source/target/network/spool disk/process kill、Cloudberry coordinator/segment restart；
- 验收：rows/s、bytes/s、p50/p95/p99 lag、CPU、RSS、spool bytes、retained WAL、恢复时间和最终
  PK-range digest。

## 容量原则

spool 不是无限空间。初始容量至少覆盖“峰值 WAL 速率 × 可接受 target 故障时间 + 最大预期
并发事务”，并保留独立 minimum free reserve；source WAL 容量覆盖同一窗口。达到水位时先
背压和告警，不能通过提前 ACK 或丢弃数据释放空间。
