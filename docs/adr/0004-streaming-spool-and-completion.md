# ADR 0004：流式事务、磁盘 spool 与连续 checkpoint

- 状态：Accepted
- 日期：2026-07-21

## 背景

源事务大小不受服务控制。把完整事务保存在 `Vec<TransactionChange>` 中并在达到固定字节数后
失败，会把正常的大事务变成永久重启循环，也无法满足持续 CDC。系统允许目标在同步过程中
短暂不一致，因此一个已提交的源事务可以拆成多个目标事务；正确性边界是最终状态和连续
checkpoint，而不是目标端的源事务原子可见性。

本地磁盘和源 WAL 都是有限资源。系统不承诺资源耗尽后无限前进，但资源压力不得变成跳过、
提前 ACK 或猜测恢复点。

## 决策

每个 source node 使用同一条数据路径：

```text
pgoutput transport
  -> decoder
  -> versioned spool journal
  -> committed transaction catalog
  -> bounded chunk scheduler
  -> table appliers / schema coordinator
  -> target durable chunk ledger
  -> node completion tracker
  -> Cloudberry checkpoint commit
  -> slot ACK
```

Standalone 是单节点实例，Physical HA 是只有一个 active logical owner 的单节点实例，Citus 是
多个互不比较 LSN 的节点实例。三种 topology 复用同一个 journal、scheduler 和 completion
tracker，不维护独立的大事务实现。

### 事务载体

领域层把事务 metadata 与 change storage 分开：

```text
CommittedTransaction {
  node_identity,
  xid,
  begin_lsn,
  commit_lsn,
  end_lsn,
  commit_time,
  manifest,
  change_source,
}
```

`change_source` 提供有界异步 chunk reader，可以来自内存或 spool segment。下游不得要求把完整
事务重新 materialize 成一个 `Vec`。manifest 只保存调度所需的有界信息，例如 row/change
计数、涉及的 relation/schema version、DDL barrier 和 segment 范围。

当前 protocol v1 的已提交事务也走同一 spool 接口；后续启用 pgoutput protocol v2 streaming
时，`StreamStart/Stop/Commit/Abort` 只是更早地向同一 journal 写入，不产生第二条 pipeline。
只有 `StreamCommit` 或普通 `Commit` 后才能把 change 交给 target applier；abort 只清理 spool。

### Spool 格式与生命周期

- 根目录按 pipeline、topology generation 和 node identity 隔离。
- 当前 spool format v2；segment 使用带版本、长度和 checksum 的 framed binary record，不使用
  JSON 热路径。v2 显式保存完整 DDL transition/typed after-schema，不能静默丢字段。
- manifest 原子发布；不完整尾记录在启动恢复时截断或丢弃。
- spool 文件只允许服务账号访问，不在日志、metric 或 API 中暴露行值。
- source WAL 和 Cloudberry checkpoint 仍是正确性权威；spool 不是外部消息队列。
- target checkpoint 成功提交后，对应 spool 可删除。若 ACK 尚未发送，源 WAL 重放仍由目标
  checkpoint 和主键幂等处理。
- 当前 runtime 先证明 managed slot 覆盖 target checkpoint，并成功从该 LSN 建立
  `START_REPLICATION`，随后清空 exact identity 的所有中断 artifact，从未 ACK WAL 重放。清理阶段
  不解析旧格式，因此二进制升级不依赖 spool 向后兼容；更低 topology generation 也会回收。

### 内存、磁盘与背压

配置表达资源水位，不表达“允许的最大事务”：

```text
memory_high_water_bytes
segment_target_bytes
disk_high_water_bytes
minimum_free_disk_bytes
apply_chunk_rows
apply_chunk_bytes
```

小事务可以完全驻留内存，但越过内存水位后透明 spill。越过磁盘高水位或最低剩余空间时，
reader 进入 `RESOURCE_WAIT`，停止继续读取和推进 ACK，并周期性重试；扩容或目标恢复后从原位置
继续。该状态不请求 rebuild，也不丢弃事务。source WAL retained bytes 与 spool bytes 必须共同
监控和容量规划。

若源端 WAL 也将耗尽，系统保持 fail-closed。运维可以增加空间、恢复 target，或显式选择新
generation 全量同步；服务不能自行失效 slot 后从当前 LSN 继续。

### Chunk apply 与 checkpoint

只处理已经 commit 的源事务。一个事务可以按 table 和有界字节数拆成多个 Cloudberry 事务，
因此目标中间状态可能与源事务不一致。每个 chunk 都必须：

- 保持同一 source node、relation incarnation、table generation 内的 WAL 顺序；
- 使用 delete/upsert/move 和 presence mask 保持幂等；
- 在 target transaction 内验证 fence、table generation 和 schema version；
- 以稳定 manifest digest 和半开 record sequence range 标识；
- 把 user-table DML 与 chunk receipt 写入同一个 Cloudberry transaction；
- 在 completion tracker 中记录完成，但不能独立越过事务的 end LSN。

只延迟 checkpoint 不能保证 chunk replay 安全。例如连续主键移动在所有 chunk 已提交后重新执行
首块，可能因为目标已处于事务最终状态而永久触发唯一键或 validation 失败。因此 target metadata
持久保存事务 manifest、每个已提交 chunk 的 range/digest 和首个未提交的 `next_seq`。重启必须先
校验完整 manifest，再从 target 的 `next_seq` 重新切块；chunk 大小可以改变，但已经持久化的同一
range 若 digest 改变必须按损坏 fail-closed。

只有 manifest 的全部 record receipt 都已提交，target 才签发不可伪造的 completion capability；
该 capability 只能推进与 manifest source identity 和 end LSN 完全相同的 node checkpoint。一个
node 的所有更早 transaction/chunk/schema transition 完成后，completion tracker 才允许发布连续
applied checkpoint。checkpoint commit 后才能删除本地 spool，之后才能向 source ACK。

DDL 或 table rebuild 可以形成 completion gap。其他 table 仍可 apply，reader 仍可把后续 WAL
落到 spool，但 checkpoint/ACK 不能越过 gap。Citus 对每个 node 独立执行该规则。

## 分阶段落地

1. 引入 transaction metadata/change source、spool journal 和 completion token 接口，普通事务仍
   可使用内存实现。
2. protocol v1 assembler 在内存水位后 spill，target 使用 durable chunk ledger 从 spool 分块
   apply；删除“超过事务 max bytes/changes 就失败”的行为。
3. 把磁盘水位接入 WAL reader 原地等待，并在 checkpoint 后清理 spool。
4. 升级/补齐 replication fork 的 protocol v2 streaming message，直接写入同一 journal。
5. completion tracker 允许 table/node applier 并行，并以连续前缀推进 checkpoint。
6. 通过故障和 soak 后删除只接受完整 `Vec<TransactionChange>` 的旧接口与旧配置字段。

旧内存路径只能在以下条件全部满足后删除：小/大事务、commit/abort、进程 kill、磁盘高水位、
target 断连、重复 WAL 和 Citus 多节点矩阵通过；重启后 target checkpoint、spool manifest 和
slot confirmed LSN 能收敛到同一连续位置。

## 结果

该方案不会因为业务事务大小触发协议失败，并允许使用磁盘换取有界内存。代价是需要本地
容量、spool 安全管理和 completion tracker；超大事务期间目标不保证原子可见，但最终状态、
checkpoint 与 ACK 顺序保持可证明。
