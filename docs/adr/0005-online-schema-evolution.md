# ADR 0005：表级 DDL 跟随与 shadow reload fallback

- 状态：Accepted
- 日期：2026-07-21

## 背景

`pgoutput` 不携带可直接重放的 DDL。源 event trigger 已能在源事务内发送 logical message，
但当前 consumer 把任意范围内 DDL/TRUNCATE 都升级为整条 pipeline rebuild。产品要求 DDL
尽量紧密跟随，同时允许中间不一致；只有无法安全在线转换时才重拉数据。

未来 Standalone、Physical HA 和 Citus 必须共享同一个 schema 状态机。为某个 topology 保留
一套特殊 DDL 分支会使恢复、checkpoint 和清理语义不可维护。

## 决策

DDL logical message 是提交后的 wake-up/event identity，不是 SQL 重放载体。consumer 收到已提交
事件后读取权威 `pg_catalog`，构造规范化 `SchemaSnapshot`、依赖闭包和 capability-tested delta。

### 三层身份

- `topology_generation`：source node 集合、system identity/timeline、slot coverage 和 Citus
  placement 契约。
- `relation_incarnation/table_generation/schema_version`：单个逻辑 relation 的持久身份与 schema
  版本；DROP 后同名 CREATE 不复用 incarnation。
- `schema_event_id`：node identity、commit LSN、message ordinal 和 payload hash 组成的幂等键。

wire decoder 的连接内 relation generation 只是协议缓存，不能作为持久 table generation。

### Event v2 与 catalog planner

event envelope v2 增加 audit/event id、对象身份和类型、command ordinal、affected relation
incarnation、coordinator marker 与 post-state checksum。`commands` 仅用于定位和分类，不执行其中
的 SQL。v1 event 保持兼容，但一律按未知能力进入 shadow reload，不能猜测在线变更。

规范化 catalog snapshot 至少覆盖：

- stable attnum、列顺序、name、type namespace/name/kind/typmod；
- nullability、generated/default 表达能力、collation；
- PK、replica identity、partition；
- Citus table kind、distribution/colocation 与 placement fingerprint；
- enum/domain/type/collation 到 relation column 的依赖图。

变更对依赖图取 closure。共享 enum/domain/collation 可能让多个 table 进入同一 transition group，
但普通单表 DDL 不升级为整 pipeline rebuild。

### 持久表级状态机

target metadata 是恢复权威：

```text
ABSENT -> SNAPSHOTTING -> CATCHING_UP -> ACTIVE
ACTIVE -> SCHEMA_PENDING -> ONLINE_APPLYING -> ACTIVE
ACTIVE -> SCHEMA_PENDING -> REBUILDING -> CATCHING_UP -> CUTOVER_PENDING -> ACTIVE
ACTIVE/REBUILDING -> BLOCKED_RETRY
ACTIVE -> QUARANTINED
```

状态记录 event id、active/pending generation、schema fingerprint、barrier LSN、snapshot/catch-up
位置、retry/error、fencing token 和 relation incarnation。每次状态迁移与 target schema/metadata
修改在同一 Cloudberry transaction 中提交。

`BLOCKED_RETRY` 不停止整个 pipeline。受影响 table 的 WAL 进入有界 spool，其他 table 继续
apply；node completion tracker 保留 barrier gap，所以 ACK 不会越过未完成 schema transition。

### 在线白名单

仅在 source delta、target capability、依赖图和实际数据检查都成功时在线执行：

- nullable ADD COLUMN，或具有已验证 immutable default 的 ADD COLUMN；
- DROP/RENAME COLUMN，rename 以 stable attnum 关联；
- SET/DROP DEFAULT；
- 经过 target 数据验证的 SET/DROP NOT NULL；
- 明确白名单中的 widening；
- enum 追加/无歧义 rename。

target ALTER、managed table metadata 和新的 dynamic binding 原子发布。row hot path 按
`(relation_incarnation, schema_version)` 从 RCU/Arc binding registry 读取，不在每行重做 catalog
规划。

### Shadow/full reload fallback

以下情况自动转 table 或依赖闭包 shadow reload：PK/distribution/collation 改变、不兼容类型、
generated expression、partition/table kind、TRUNCATE、同一事务无法证明的复杂 DDL、在线执行前置
检查失败以及未知 v1 event。

流程为：创建 pending table generation -> 建 typed shadow -> 建立 source snapshot -> COPY ->
从 barrier 起重放该 table 已 spool WAL -> reconciliation -> Cloudberry transaction 内把旧表移入
quarantine、shadow 激活、binding/metadata 切换 -> completion gap 完成。

fallback 层级固定为：单表、共享依赖 closure、topology/pipeline。只有 slot/WAL 丢失、source
identity 不连续、未知事件范围或 Citus node coverage/topology 失真才允许整 pipeline rebuild。

DROP TABLE 进入 quarantine，不立即物理删除。新表通过相同准入、snapshot、catch-up 和 activation
流程自动加入。

### Topology 统一

- Standalone：一个 node stream 和一组 table transition。
- Physical HA：仍只有一个 active logical node；failover 连续性成立时状态机不变。
- Citus：schema event 只由 coordinator 产生；每个 worker relation fingerprint/coverage 必须与
  coordinator snapshot 一致，table transition 对每个 node 保存 barrier/catch-up progress。

不同 Citus node 的 LSN 不比较，schema cutover 只有在 transition group 所有需要节点完成后才
激活。

## 分阶段落地与旧路径删除

1. 增加 v2 event、持久 schema event/version/transition metadata；v1 保留整表 reload fallback。
2. 实现 table-level barrier、shadow reload、retry/restart 和 dynamic binding，暂不开放在线 ALTER。
3. 逐项开放在线 DDL 白名单；任一 capability 失败自动转 shadow。
4. 接入 Physical HA/Citus node vector、worker fingerprint 和 topology transition。

只有 table-level crash/replay/soak 矩阵通过后，才能删除 immutable run-scoped binding。只有在线
白名单全部通过 PG18 -> Cloudberry 真实矩阵后，才能删除 `reject_schema_barriers` 的普通 DDL
分支。整 pipeline rebuild API 永久保留，但只用于全局正确性损坏，不再作为普通 DDL 路径。

## 结果

安全 DDL 可以低延迟跟随，复杂 DDL 自动重拉受影响数据，其他 table 继续工作。代价是引入持久
schema state、dependency planner、dynamic binding 和 checkpoint gap；这些复杂度集中在 rare
path，正常 row apply 保持简单。
