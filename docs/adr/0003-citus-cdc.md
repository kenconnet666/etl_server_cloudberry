# ADR 0003：Citus CDC 的逐节点 LSN 与 topology generation

- 状态：Accepted
- 日期：2026-07-20

## 背景

产品既要适用于单 PostgreSQL database，也要适用于大型 Citus 集群。Citus 的 shard 写入发生在 coordinator 和多个 worker 上；PostgreSQL LSN、slot、timeline 和 xid 都是物理节点局部概念，不存在可由本系统可靠构造的跨节点全局 WAL 顺序。

Citus 14.1 提供 pgoutput CDC wrapper，能够把物理 shard relation 归一为逻辑 relation，并过滤部分 shard move/rebalance 内部事件，但 upstream 仍把该能力标记为 preview。生产支持必须建立在明确白名单和真实集成测试上。

## 决策

接受 Citus 14.1 CDC preview 作为受约束能力，不把 Citus 伪装成单一 replication stream。

每个 active coordinator/worker node 都有独立：

- replication connection。
- 显式 business-table publication/coverage。
- logical slot。
- received、applied、flush/ACK LSN。
- system identifier、timeline 和稳定 node identity。

checkpoint 是按 topology generation 保存的 LSN 向量：

```text
G42 = {
  coordinator-a: 0/16B6C50,
  worker-a:      3/91F0028,
  worker-b:      1/A2200F0
}
```

向量分量之间不可比较。禁止按 wall clock、commit timestamp、xid 或 LSN 对不同节点事件重排。产品不保证分布式事务在 Cloudberry 原子可见；最终当前状态由主键幂等应用和 reconciliation 保证。

## Topology generation

一个 generation 固定以下事实：

- Citus cluster identity 和锁定版本。
- coordinator 与 active worker identity 集合。
- 每个 node 的 endpoint、system identifier、timeline、publication、slot 和起始点。
- 逻辑表 identity、table kind、distribution column、shard/placement fingerprint。
- 当前允许的 CDC capability 集。

节点加入/移除、slot coverage 变化、关键 placement 语义变化或 source identity 变化会创建新 generation。旧 generation 的 LSN 不能填入新 generation 向量。

generation transition 可以是：

- 受管切换：提前覆盖所有节点，建立新向量，完成 topology 操作后 reconciliation，通过后激活。
- 重建切换：创建 shadow target generation，重新快照和追赶，再原子激活。

无法证明连续性的 transition 一律选择重建。

## 前置条件

所有节点必须满足：

- PostgreSQL 18、兼容的 Citus 14.1.x。
- `wal_level=logical`，足够的 replication slot 和 walsender。
- 锁定版本要求的 `citus.enable_change_data_capture` 等 CDC 配置已经启用。
- 服务可以直接访问 advertised host/port，或配置稳定 endpoint override。
- publication 显式列出合格业务表，不能 `FOR ALL TABLES`，以免包含 Citus metadata。
- publication 在 coordinator 创建，并按锁定版本的官方方式传播和验证 worker coverage。
- 每个节点 slot 在 generation 激活前存在且 identity 匹配。

DDL 只允许在 coordinator 执行。DDL event trigger 带 coordinator guard；worker 因 DDL 传播产生的本地事件不能被解释为第二个逻辑 DDL。

## 初始同步

Citus bootstrap 采用“先覆盖 WAL，再从 coordinator 做逻辑快照”：

1. 从 coordinator 发现 topology，验证所有 active placement 和表能力。
2. 获取锁定 Citus 14.1 后已经验证的 cluster-change barrier。在 barrier 内禁止扩容、rebalance、split、DDL 和其他会改变 placement 的操作。
3. 为 coordinator 和所有 active worker 创建/验证 publication 与 slot，记录各自 consistent point，形成新 LSN 向量。
4. 从 coordinator 开启业务逻辑快照并读取每个逻辑表；worker 不各自复制完整逻辑表。
5. typed COPY 写入 shadow target generation。快照期间 WAL 由每个 slot 保留。
6. 从各 node consistent point 回放。早于快照可见状态的事件可能重复，使用 PK 幂等消除。
7. 每节点追平后执行 count 和 PK 分块 canonical hash reconciliation。
8. 校验通过才激活 generation 和解除完整运行限制。

具体 barrier API，例如当前版本的 `citus_cluster_changes_block` 或等价机制，必须在锁定 Citus release 的集成测试中验证后实现，不能仅凭函数名假设阻塞范围。若部署环境无法提供经过验证的 topology barrier，初始同步保持 experimental，不能标记 production-ready。

快照不声称提供 Citus 跨节点事务的目标原子可见性。由于 slot 在快照前建立，快照后回放全部节点 WAL，且行操作幂等，稳定写入停止后应最终收敛。

## 表类型能力

采用逐类解锁，不因为 Citus wrapper 能产生事件就自动宣称生产支持。

| Citus table kind | 默认状态 | 说明 |
| --- | --- | --- |
| hash-distributed row table | supported | PK 必须包含 distribution column；首批生产路径 |
| reference table | validation-gated | upstream 有 CDC 测试，本项目仍需证明多 placement 不重复/不丢失 |
| coordinator-local table | validation-gated | 需证明 publication、snapshot 和 node ownership 唯一 |
| single-shard table | validation-gated | 需证明 placement move 与 failover handoff |
| columnar table | rejected | logical decoding 不满足契约 |
| schema-sharded table | rejected | 不在首批 identity/mapping 模型内 |
| append/range distributed | rejected | placement 和 key 收敛契约未覆盖 |
| 复杂分布式 partition | rejected | snapshot、DDL 和 shard identity 组合未验证 |

validation-gated 类型只有在独立 capability flag、真实版本矩阵和故障测试通过后才能启用；不能通过普通配置绕过。

## Rebalance、扩容与 drift

所有 topology 操作必须先走 `prepare worker/topology`：

1. 发现计划后的节点和 placement 范围。
2. 验证 endpoint、CDC 配置、publication、slot 和 WAL 容量。
3. 建立新 topology generation 与 checkpoint vector。
4. 允许 orchestrator 执行 add、drain、rebalance 或 split。
5. 监控 wrapper 事件、未知 relation 和 placement fingerprint。
6. 完成后强制 reconciliation，再激活/结束 transition。

所有相关节点都已被 slot 覆盖时，可以允许受管在线 rebalance，但这仍是 generation transition，不是透明的日常事件。

以下情况立即暂停受影响 pipeline：

- 发现未知 worker 或未知物理 shard relation。
- active worker 没有 slot/publication coverage。
- 在 `prepare` 之外发生 rebalance、node add/remove 或 placement drift。
- `alter_distributed_table` 改变 distribution column、colocation 或 shard count。
- wrapper 输出无法归一到唯一逻辑 relation/generation。

`alter_distributed_table` 和无法证明无损的 drift 默认 `REBUILD_REQUIRED`。系统不能用 timestamp 或“最后到达者获胜”掩盖多节点冲突。

## Citus HA

coordinator 和每个 worker 可以有 physical standby，但每个 node group 仍只有一个逻辑 CDC owner：

- 使用稳定 primary endpoint 或显式 node address override。
- PostgreSQL 18 failover logical slot 必须配置并监控。
- failover 后验证 system identifier、timeline、新 primary slot confirmed LSN 与 target checkpoint。
- 只有连续性得到证明才在同一 generation 恢复；否则切换 generation 并重建受影响范围。
- physical standby 不创建第二条并行 reader，也不把其 LSN 加入向量。

本系统不实现 Patroni、云厂商或 Citus 节点的故障选主，只消费部署环境提供的稳定 primary identity。

## DDL 与 logical message

Citus DDL 的 schema notice 从 coordinator 产生。每个 node stream 中仍可能出现 relation version 变化，decoder 必须把 relation fingerprint 与 coordinator 的 catalog snapshot 对齐。

- 安全列变化在所有节点 coverage 验证后放行。
- PK、distribution、colocation、partition 或 table kind 变化创建 shadow table generation。
- coordinator notice 与 worker relation fingerprint 不一致时阻塞，不选择任意一方猜测。
- 新业务表只有在所有所需 node publication/slot coverage 完整后进入 active generation。

## 监控和验证

Citus pipeline 至少暴露：

- topology generation 和 drift 状态。
- 每节点 received/applied/ACK LSN、lag bytes/time、slot retained WAL。
- worker endpoint、system identifier、timeline、publication/slot coverage。
- 每逻辑表 table kind、distribution key、placement fingerprint 和 capability status。
- unknown shard relation、wrapper filter、duplicate collapse 和 reconciliation 计数。

生产解锁必须覆盖：

- 多 worker 并发 insert/update/delete 和 distribution key update。
- 多 shard 分布式事务，不要求原子可见但最终 hash 必须一致。
- worker restart、primary failover、slot failover。
- 新 worker prepare、rebalance、drain、split 和操作中进程崩溃。
- wrapper 版本升级和 shard relation rename/move。
- reference/local/single-shard 各自的重复、遗漏和快照边界。

## 结果

优点：

- 不虚构不存在的全局 LSN，恢复和监控证据明确。
- 同一架构可覆盖小型单库和大型多 worker 集群。
- generation 把 topology 演进与普通 WAL 消费分开，异常 drift 不会静默污染目标。
- 白名单允许先把已验证的 hash-distributed row table 做到生产质量。

代价：

- 每个 worker 消耗连接、slot 和 WAL 保留空间。
- topology operation 必须与本系统协调，并执行额外 reconciliation。
- Citus upstream preview 增加版本锁定、真实集群测试和升级成本。
- reference/local/single-shard 不能在未验证前默认开启。

## 未选择的方案

- 只连接 coordinator 的单一 slot：无法覆盖所有 worker 本地 WAL。
- 把各节点 LSN 或 commit timestamp 合并成总序：没有可靠语义，可能覆盖新值或跳过事件。
- publication `FOR ALL TABLES`：会包含 Citus metadata 和不合格对象。
- 未覆盖 worker 时继续 rebalance：存在不可证明的 WAL 空洞。
- 把 upstream preview 当作全表类型生产支持：测试证据不足。

