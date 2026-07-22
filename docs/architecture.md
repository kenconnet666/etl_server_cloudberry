# PostgreSQL 到 Cloudberry 当前状态镜像架构

## 文档状态

- 状态：目标架构，已接受
- 日期：2026-07-20
- 当前实现状态：V3 正在实现中。除下节明确列出的能力外，本文描述的是交付目标，不代表功能已经完成或通过生产验证。
- 版本基线：PostgreSQL 18、Apache Cloudberry 2.1.0-incubating、Citus 14.1.x。

版本基线是兼容和测试边界，不使用浮动的 `latest`。实现时锁定 Cargo lockfile、容器镜像 digest 和外部源码 commit；升级通过完整兼容矩阵后进行。

## V3 当前实现边界

- 数据面运行时只执行 `Standalone` topology。`PhysicalHa` 和 `Citus` 会返回明确的 unsupported-topology 错误，不会退化为单机模式继续运行。
- Citus 已有 topology/catalog discovery、表元数据分类和 opt-in 真实集群测试环境；尚未接通逐 worker snapshot、WAL、checkpoint 和 Cloudberry apply，因此不能声明端到端支持。
- 源 catalog 能同时返回合格表与拒绝表；当前 pipeline planner 对配置范围采用严格 fail-closed：任意一张表不合格都会拒绝整条 pipeline 启动，绝不静默跳过。
- “持久化逐表 `BLOCKED` 状态并让其他合格表继续”是目标能力，需在 control metadata 与运行时恢复语义接通后才能启用。
- 后续章节中的 Citus、物理 HA、分块 reconciliation、表级隔离和完整 DDL 分类均是必须通过生产验证矩阵的目标设计。

## 目标与非目标

系统只解决一件事：把一个或多个 PostgreSQL 18 database 中符合契约的表，最终收敛为 Cloudberry 中的同一份当前状态。

目标：

- 支持单机 PostgreSQL、带物理从库的 PostgreSQL，以及 Citus coordinator/worker 集群。
- 仅复制有普通主键且类型可精确映射的表。
- 初始全量加载后持续消费 WAL，追随受支持的 DDL。
- 允许复制过程中短暂脏读、跨表状态不一致和重复应用，但最终必须收敛。
- 多源通过明确的目标 namespace 隔离，并支持显式 schema/table 映射。
- 从第一版保留可观测性、故障恢复、在线重建和 active/standby 服务能力。

非目标：

- 不提供 CDC 历史、审计日志或时间旅行。
- 不支持 PostgreSQL 18 之外的源，也不支持 Cloudberry 之外的目的端。
- 不复制 view、materialized view、function、trigger、sequence 当前值、RLS、grant 或任意源索引。
- 不保证源事务在目标跨表原子可见，也不构造 Citus 跨节点全局提交顺序。
- 不对不支持的类型静默转成 `text`、`json` 或 `bytea`。
- 不在首版引入 Kafka 或其他外部消息队列。

详细准入规则见 [source-contract.md](source-contract.md)。

## 正确性不变量

以下不变量优先于吞吐量和可用性：

1. 只有 Cloudberry 中不大于某 LSN 的全部数据已经持久化，才能推进该节点的 applied checkpoint。
2. 数据变更和推进 checkpoint 必须在同一个 Cloudberry database 中提交；checkpoint 不能先写入控制库。
3. PostgreSQL slot ACK 只能发生在对应 Cloudberry checkpoint 成功提交之后。
4. 重启后允许从更早 LSN 重放，禁止跳过尚未确认的数据。
5. 同一 source node、table、generation 内保持 WAL 顺序；不同 Citus node 的 LSN 不可比较。
6. schema、映射、主键、分布策略或拓扑发生不兼容变化时，不得把新旧事件混入同一 table generation。
7. `NULL`、未改变的 TOAST 值和缺失列是三种不同状态。
8. 任意类型没有经过源版本、typmod、目标能力和编码路径验证时，该表必须保持 blocked。

这些不变量产生 at-least-once 交付。崩溃发生在目标提交后、slot ACK 前时会重复消费；主键 delete/upsert 和 generation fence 使重复应用幂等。详见 [ADR 0001](adr/0001-delivery-semantics.md)。

## 系统边界

```text
bootstrap config
      |
      v
control PostgreSQL 18 <----> Axum control API <----> Svelte UI
      |                              |
      | desired state / lease        | supervisor
      v                              v
source PG18/Citus ---> CDC runtime ---> Cloudberry 2.1
  pgoutput + DDL        bounded flow     target data
  metadata schema      table appliers   atomic checkpoints
      ^                                      |
      +------------- reconciliation <-------+
```

一个 pipeline 对应一个源 database 和一个 Cloudberry target database。一个服务实例可以运行多个 pipeline。物理从库是源的 HA 组成部分，不是另一个 pipeline，也不会被重复读取。

默认多源目标命名为：

```text
target schema = <source_prefix>__<source_database>__<source_schema>
target table  = <source_table>
```

所有标识符都经过 PostgreSQL 标识符长度检查和稳定 hash 缩短。修改 prefix、source identity、目标 namespace 或显式映射会创建新 generation，并要求重新快照。发现未被本系统拥有的同名目标对象时默认拒绝；只有显式 `adopt` 操作在结构校验和全量 reconciliation 成功后才能接管。

## 代码边界

仓库采用 Cargo workspace。业务代码只能放入具有明确所有权的 crate，禁止在 `app` 或通用 `utils` 中堆积协议与存储逻辑。

```text
crates/
  core/                领域类型、LSN、generation、schema 与命名不变量
  config/              严格 bootstrap 配置与校验
  metadata/            control metadata、migration、secret 与 lease
  source-postgres/     PG18 catalog、snapshot、pgoutput、DDL 与 Citus discovery
  target-cloudberry/   类型 DDL、COPY、staging、apply、checkpoint 与 snapshot
  engine/              pipeline lifecycle、规划、runtime 与 supervisor
  api/                 认证后的 control-plane HTTP API
  app/                 binary、依赖装配、静态前端与进程生命周期
web/                   Svelte 5 + TypeScript + Vite
tests/
  integration/         PG18、Cloudberry、Citus 的 opt-in 真实服务测试
docs/
  adr/                 已接受的架构决策
```

依赖方向：适配器 crate 依赖 `core`，`core` 不依赖数据库驱动、HTTP 或前端。跨 crate 共享的类型只有稳定领域概念；某数据库专有 SQL、OID 和错误不能泄漏进核心层。Citus discovery 仍由 `source-postgres` 所有，只有逐节点运行时职责足够独立时才拆 crate。先选择性吸收 Supabase ETL 等项目中经验证的思路和协议测试，不复制其 workspace 结构，也不依赖滚动 `main`。

## Pipeline 生命周期

### 1. 配置与预检

控制面保存 desired-state revision。保存草稿不会改变运行态；`Validate` 执行只读检查，`Apply` 才生成不可变 revision。

启动前至少验证：

- 源和目标身份，版本、编码、extension 与 topology。
- publication、slot、event trigger、WAL 保留和 direct worker connectivity。
- 每张表的主键、类型、collation、generated column、partition 和 Citus table kind。
- 目标对象冲突、目标 ownership、分布键和剩余空间。
- 所有 Citus active node 是否已经进入当前 topology generation。

目标语义是把预检结果按表持久化，让表级 blocked 不阻塞其他合格表；会破坏全库 checkpoint 或 topology 覆盖的错误仍阻塞整个 pipeline。当前 V3 尚未持久化 rejected inventory，因此只要配置范围内存在不合格表就拒绝整条 pipeline 启动。

### 2. 初始快照

独立 PostgreSQL 使用以下顺序：

1. 创建显式 publication 和 logical slot，记录 consistent point。
2. 在 slot 对应的一致性快照中读取 catalog 和业务表。
3. 以主键范围并行读取，使用 typed COPY 写入新的目标 generation。
4. 保留快照期间产生的 WAL，快照完成后从 consistent point 重放。
5. 追平、reconciliation 通过后将新 generation 原子切换为 active。

快照期间允许业务写入。快照与 WAL 重放重叠导致的重复由主键幂等应用消除。超大表必须支持可恢复的分块进度；分块进度不能被当作 WAL checkpoint。

Citus 快照只通过 coordinator 读取逻辑表，worker 不各自复制完整逻辑表。所有 active node 的 slot 必须在快照开始前就绪，且快照期间禁止未编排的 topology change。详细流程见 [ADR 0003](adr/0003-citus-cdc.md)。

### 3. WAL 读取和内存模型

reader 使用 PostgreSQL logical replication 和 `pgoutput`，请求 transactional messages，并在能力验证后启用大事务 streaming。领域事件至少包含：

```text
SourcePosition { pipeline, generation, node, lsn, xid }
RelationVersion { source_relation, schema_fingerprint, columns, key }
RowChange { insert | update | delete, old_key, values, presence_mask }
SchemaNotice { affected_relations, source_position }
Commit { source_position }
```

行值在解析前保留为 `Bytes`，状态模型为 `Null | UnchangedToast | Text(Bytes) | Binary(Bytes)`。OID 只用于当前连接内定位，持久化类型身份同时包含 namespace、name、kind、typmod 和结构 fingerprint，不能把数据库本地 OID 当成跨重启契约。

所有 channel 以事件数和字节数双重限界。普通事务可以在内存中合并；越过内存水位后透明写入本服务专属的有界磁盘 spool，不以事务字节数触发业务失败。收到 COMMIT 后才能按有界 chunk 交给 applier，ABORT 时丢弃。磁盘达到高水位时停止读取和 ACK、进入可恢复的资源等待；spool 不是独立消息权威，进程崩溃后仍可由未 ACK 的 WAL 重放恢复。详见 [ADR 0004](adr/0004-streaming-spool-and-completion.md)。

### 4. 调度与批处理

decoder 输出按 source node 有序。调度器可以把已提交事务拆成 table batch 并行执行，但必须满足：

- 同一 table generation 的事件保持该节点 WAL 顺序。
- 同一批中同一主键的事件按顺序折叠为最终操作。
- 只有某 node LSN 之前的所有 table batch 都完成后，completion tracker 才允许推进该 node checkpoint。
- 跨表原子性不是契约；大事务可拆批，但 checkpoint 永远不越过未完成批次。

合批由行数、字节数和最大等待时间共同触发。并发、batch size 和速率限制可热更新；schema、映射、PK 和 distribution 不能热更新。

### 5. Cloudberry 应用

首批目标表使用 heap，并以源主键全部列作为 `DISTRIBUTED BY`。目标表保留可表达的主键约束；目标 namespace 不允许外部 DML/DDL。

每个 table batch 先写 typed staging table，再在一个 Cloudberry 事务内：

1. 锁定并验证 pipeline fencing token、table generation 和 schema fingerprint。
2. 按 old primary key 删除旧行，包括 PK 更新的旧 key。
3. 按 presence mask 合并未改变的 TOAST 列。
4. 以主键执行 insert/upsert；distribution key 更新始终表现为 delete + insert。
5. 当 completion tracker 确认形成连续前缀时，推进 target metadata 中该 node 的 applied checkpoint。
6. 提交后才向 source slot 发送 ACK。

一个超大源事务可能拆成多个目标事务。前置分块可以先提交，但 checkpoint 只能在所有分块完成后的事务推进，因此崩溃只会造成重复应用，不会丢失变更。

### 6. DDL 跟随

`pgoutput` 不复制 DDL。每个源 database 安装独立 `pg2cb_meta` schema 和 event trigger。trigger 不解析或重放 `current_query()`，而是在源事务中发送 transactional logical message：

```text
prefix = pg2cloudberry_ddl_v2
payload version = 2
payload = command/scope identity + ordered per-relation typed after-schema
```

legacy v1 仍可解码，但只能进入受影响表的保守重拉。消费端在整个源事务提交后读取
`pg_catalog`，只把每个 relation 的 terminal after-schema 与当前 catalog 对齐；事务内中间快照保留
用于解释有序 schema/DML。安全变化先修改目标 schema，再放行使用新 relation version 的行事件；
不安全变化创建 shadow generation，执行快照、WAL 追赶、reconciliation 和原子切换。

默认分类：

- 在线跟随：兼容的 add/drop/rename column、default、nullability 和安全 widening。
- shadow rebuild：PK、distribution、collation、不兼容类型、generated expression、不可原地完成的 partition 变化。
- 阻塞：无法精确翻译的对象或来源不明的 DDL。
- DROP TABLE：移动到 quarantine，默认保留 30 天后才允许显式清理。

DDL 错误阻塞受影响表并保留 WAL，不跳过行。其他表可以继续 apply/spool，但 completion tracker 不允许 checkpoint 越过 schema barrier。安全变化在线执行；无法证明安全时自动重建受影响表或依赖闭包。只有 WAL/slot、source identity 或 topology coverage 失真才升级为整 pipeline 重建。详见 [ADR 0005](adr/0005-online-schema-evolution.md)。

### 7. Reconciliation 与修复

最终一致不能只依赖 slot 没报错。系统定期按表执行：

1. 快速 row count 和边界检查。
2. 按稳定 PK 范围计算 canonical typed hash。
3. 对差异块重新读取并执行 delete/upsert repair。
4. 重复失败或 schema 不可信时升级为 shadow rebuild。

hash 输入包含类型标签、NULL 标记和无歧义长度编码，不依赖 locale 或显示格式。校验任务限速、可暂停，并记录 source snapshot LSN，避免把正常并发写误判为永久差异。

## Citus 模型（目标设计，V3 尚不可运行）

目标 Citus reader 连接 coordinator 和每个 active worker。每个物理节点有独立 publication/slot/reader/checkpoint：

```text
TopologyGeneration {
  cluster_identity,
  generation,
  nodes: {
    node_identity: { system_identifier, timeline, slot, applied_lsn }
  }
}
```

LSN 只在同一 node identity 和 timeline 上比较。系统禁止按 timestamp、xid 或 LSN 对不同 worker 的事件排序。拓扑成员、slot coverage 或 shard placement 语义变化时切换 generation；旧 generation checkpoint 不能填充新向量。

Citus 14.1 CDC 是 upstream preview 功能，因此采取能力白名单。计划首先解锁含 distribution key 主键的 hash-distributed row table；在逐节点数据面和真实集群矩阵完成前，所有 Citus pipeline 都保持 validation-gated。reference、coordinator-local 和 single-shard table 有设计路径，但必须分别通过真实集群的快照、增量、rebalance、failover 和重复事件矩阵后才可解锁。columnar、schema-sharded、append/range distributed 和复杂分布式 partition 默认拒绝。

扩容、drain、rebalance 和 split 必须先通过 UI/CLI 的 `prepare worker`。已覆盖节点之间的受管 rebalance 可以在线进行，但必须创建新 topology generation 并在结束后 reconciliation。未知 worker、缺 slot、外部 `alter_distributed_table` 或无法解释的 placement drift 会暂停受影响 pipeline，并要求重建。

## HA 与故障恢复

### 源数据库 HA

- 独立 PostgreSQL 的 logical reader 只连接当前 primary 的稳定 endpoint。
- PostgreSQL 18 HA 必须配置并验证 failover logical slot；物理 standby 不作为并行 CDC 源。
- failover 后校验 system identifier、timeline、slot confirmed LSN 和目标 checkpoint。不能证明连续性时暂停并重建，禁止猜测 LSN。
- Citus coordinator 和 worker 分别应用同一规则，且 node address 可以配置稳定 endpoint override。

### 服务 HA

首版可部署单 active 实例，但从一开始实现 control DB lease 和单调 fencing token。每次 Cloudberry apply 都锁定 target checkpoint row 并验证 token。旧实例即使仍存活，也不能在新 active 获得更高 token 后提交。

控制库不可用时停止配置变更；运行中的 pipeline 只运行到租约过期，然后停止读取和应用。目标 checkpoint 是恢复权威，控制库中的 observed status 只是缓存。

### WAL 丢失

slot 丢失、requested WAL 已被回收或节点身份不匹配时自动暂停。系统不会悄悄从当前 LSN 继续，也不自动覆盖目标；管理员在 UI 确认后创建新 generation 并执行快照。WAL 保留同时配置字节和时间上限，优先保护源数据库磁盘。

## 控制面、安全和可观测性

bootstrap 配置只包含监听地址、单管理员 Argon2id hash、明确的 `control_dsn` 和主密钥来源。连接 secret 使用带 key version 的 AEAD envelope encryption，UI 保存后不回显，日志、错误、metric label 和审计事件都必须脱敏。

前端采用 Svelte 5，编译成静态资源嵌入 Axum binary。REST 负责配置和命令，SSE 负责实时状态。默认在私网运行，由反向代理终止 TLS；session cookie 使用 HttpOnly、Secure、SameSite，写请求带 CSRF 防护并对登录限流。首版不提供多用户、RBAC 或 API token。

最小可观测面：

- JSON structured logs 和 OpenTelemetry trace correlation。
- 每 pipeline/node 的 received、decoded、applied、ACK LSN 与 lag bytes/time。
- 每 table 的 snapshot/rebuild phase、queue bytes、apply rows/seconds、reconciliation 结果。
- slot retained WAL、spool bytes、Cloudberry batch latency、retry 和 blocked reason。
- readiness 区分 control plane、source coverage 和 target apply；liveness 不因单表 blocked 而失败。
- 高基数 source/table 名不直接成为默认 metric label，详细维度通过状态 API 查询。

## 生产验证门槛

任何能力只有通过对应矩阵后才能从 validation-gated 改为 supported：

- PG18 到 Cloudberry 2.1 的每个类型、typmod、NULL/TOAST、PK update 测试。
- 初始快照与并发 insert/update/delete/DDL 的最终 hash 一致。
- 目标 commit 前后 kill、source/target 网络中断、重复 WAL、服务双 active fencing。
- 大事务 streaming、spool 上限、背压和源 WAL 保护。
- Cloudberry segment failure、coordinator restart、shadow generation swap。
- PostgreSQL primary failover 和 failover slot 连续性。
- Citus worker failover、受管 rebalance、新 worker、reference/local/single-shard 各自的重复和丢失矩阵。
- 至少覆盖单库小负载和多 worker/大表/高 WAL 的 soak、恢复时间与资源上限。

## 已接受的决策

- [ADR 0001：At-least-once 与当前状态收敛](adr/0001-delivery-semantics.md)
- [ADR 0002：控制面和元数据权威](adr/0002-control-and-metadata.md)
- [ADR 0003：Citus CDC 的逐节点 LSN 与 topology generation](adr/0003-citus-cdc.md)
- [ADR 0004：流式事务、磁盘 spool 与连续 checkpoint](adr/0004-streaming-spool-and-completion.md)
- [ADR 0005：表级 DDL 跟随与 shadow reload fallback](adr/0005-online-schema-evolution.md)
