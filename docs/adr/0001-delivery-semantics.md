# ADR 0001：At-least-once 与当前状态收敛

- 状态：Accepted
- 日期：2026-07-20

## 背景

产品只要求 PostgreSQL 18 与 Cloudberry 中受管表的当前状态最终一致。允许同步过程中看到脏数据，不要求 CDC 历史、时间旅行或跨表事务原子可见。

PostgreSQL logical slot 和 Cloudberry 分属两个数据库，无法用一个受支持的分布式事务原子提交目标数据并 ACK 源 LSN。伪造 exactly-once 会在进程崩溃和网络不确定性下产生隐蔽丢数风险。

## 决策

采用 at-least-once 读取和幂等目标应用：

1. source WAL 是未确认变更的恢复来源。
2. 行身份只使用源 primary key。
3. insert/update 在目标执行 typed upsert，delete 使用 old primary key。
4. primary key 或 distribution key update 表示为 delete old key + insert new row。
5. 同一 source node、table、generation 保持 WAL 顺序。
6. 目标 applied checkpoint 只表示“该 node LSN 之前形成连续前缀的所有变更已经持久化”。
7. 推进 checkpoint 与形成该前缀的最后一个 target batch 在同一个 Cloudberry 事务提交。
8. target commit 成功后才向对应 source slot ACK。

目标 metadata 中的 checkpoint 是恢复权威。控制库可以缓存 observed LSN，但不能参与 ACK 决策。

```text
decode -> stage -> target data + checkpoint COMMIT -> source slot ACK
                       ^
                       +-- 唯一允许推进 applied LSN 的位置
```

崩溃窗口的结果：

| 崩溃位置 | 恢复行为 |
| --- | --- |
| target commit 前 | 数据和 checkpoint 一起回滚，WAL 重放 |
| target commit 后、ACK 前 | WAL 重放，PK 幂等应用 |
| ACK 后 | target checkpoint 已经包含对应连续前缀 |

超大源事务允许拆成多个 target batch，因而不保证跨表原子可见。前置 batch 可以先提交，但 completion tracker 只有在所有 batch 完成后才允许最后一个 target 事务推进 checkpoint。崩溃可能重复前置 batch，不会跳过它们。

Citus 不建立跨节点全局顺序。每个节点独立保持顺序和 checkpoint，所有节点的 applied LSN 组成向量；只有向量内的分量可以分别推进。

## 当前状态一致的定义

在源写入停止、DDL 和 topology 稳定、pipeline 没有 blocked table 的条件下，系统必须在有限时间内满足：

- 每个受管目标 relation 的主键集合等于源主键集合。
- 每个主键的所有受支持列按类型规范化后相等。
- active target schema fingerprint 等于源的可表达 schema fingerprint。
- 每个 source node 的 slot lag 收敛到零或正常心跳范围。
- canonical count/PK chunk hash reconciliation 通过。

这个定义不包含相同行物理顺序、索引布局、query plan、MVCC history、事务 ID 或未复制对象。

## 幂等边界

幂等性依赖以下前置条件：

- 每张表有稳定且精确映射的 primary key。
- 同一主键不会在同一 active generation 内被两个不受协调的源拥有。
- 目标 namespace 不接受外部 DML。
- `UnchangedToast` 由 presence mask 保留旧值，不能当作 NULL。
- 每个 batch 验证 schema fingerprint、generation 和 fencing token。

任一条件不成立时暂停表或 pipeline，不使用“尽力写入”。

## 校验与修复

at-least-once 解决崩溃重放，不证明永久没有逻辑偏差。因此 reconciliation 是交付语义的一部分：

- 定期比较 count 和 PK 分块 canonical typed hash。
- 差异块执行 source reread 和目标 delete/upsert repair。
- 重复不收敛、slot continuity 丢失或 schema 不可信时创建 shadow generation 全量重建。

slot 丢失或 requested WAL 被回收后不能静默跳到当前 LSN，必须由管理员确认重建。

## 结果

优点：

- 不需要 Kafka 或跨数据库 2PC。
- 崩溃恢复模型简单，可用故障注入验证。
- 适合最终当前状态镜像，也允许 table 级并行和大批量 COPY。

代价：

- 同步中可能看见跨表或大事务的部分结果。
- target commit 后、ACK 前会重复工作。
- 所有表必须有受支持的主键，目标必须禁止外部写入。
- 必须持续 reconciliation，不能只监控 replication lag。

## 未选择的方案

- Exactly-once 声明：源 ACK 与目标提交间没有共同原子事务，声明无法兑现。
- Kafka 作为必需中间层：增加运维和一致性边界，当前状态镜像不需要其历史保留能力。
- 只保存 checkpoint 到 control DB：会允许 checkpoint 超前于目标数据，产生不可恢复丢失。
- 全库串行单事务 apply：限制吞吐并放大超大事务和 Cloudberry 锁影响，而产品不要求跨表原子可见。

