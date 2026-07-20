# 源 PostgreSQL 契约与支持边界

## 文档状态

- 状态：目标契约，已接受
- 源版本：PostgreSQL 18.x
- Citus 版本：14.1.x，CDC upstream 状态为 preview
- 目标版本：Apache Cloudberry 2.1.0-incubating，基于 PostgreSQL 14.4

本契约使用白名单。未明确列为 supported 的能力都视为 validation-gated 或 rejected，不能自动降级。

## Database 级契约

一个 pipeline 精确对应一个 PostgreSQL database。一个实例上的多个 database 使用多个 pipeline，因为 publication、logical slot、catalog 和 LSN 都是 database 级边界。

源必须满足：

- PostgreSQL 18，database encoding 为 UTF-8。
- `wal_level=logical`，并为本服务预留足够的 `max_replication_slots` 和 `max_wal_senders`。
- 服务身份有 replication、catalog 读取、业务表 SELECT，以及安装 publication、event trigger 和 `pg2cb_meta` 的权限。
- publication 显式列出已准入业务表，不使用 `FOR ALL TABLES`。
- publication 输出 insert、update、delete、truncate；其中 TRUNCATE 默认触发重建，而不是直接清空目标。
- partitioned table 使用 `publish_via_partition_root=true`，保证 WAL relation identity 与逻辑父表映射一致，且不能同时把叶子表作为另一个逻辑对象重复发布。
- 存在 supported stored generated column 时使用 PostgreSQL 18 的 `publish_generated_columns=stored`，并在预检中用真实变更证明 pgoutput 携带目标所需值。
- replication connection 开启 logical messages；DDL notice prefix 固定为 `pg2cloudberry_ddl_v1`。
- 源端时区、`DateStyle`、`IntervalStyle`、`bytea_output` 和 extra float digits 由连接初始化固定，不依赖用户默认值。
- `pg2cb_meta`、Citus metadata 和 PostgreSQL system schema 永远排除在 publication 与快照之外。

服务安装的 event trigger 只发 transactional logical message 并维护 schema identity，不保存业务行。不得解析 `current_query()` 来重放 DDL；DDL 真相来自提交后的 `pg_catalog` snapshot。

## 拓扑模式

### Standalone

支持单 primary。初始快照和 logical replication 都从 primary 执行。

### Primary + physical standby

支持一个 primary 和多个物理从库，但它们共同构成一个 source node identity：

- 只消费当前 primary，禁止同时消费 standby。
- 使用稳定 primary endpoint。
- PostgreSQL 18 failover logical slot 必须启用并由部署方监控同步状态。
- failover 后必须验证 system identifier、timeline、slot 和 checkpoint 连续性。
- 不满足连续性证明时进入 `REBUILD_REQUIRED`。

### Citus

Citus coordinator 与每个 active worker 都必须：

- 使用 PostgreSQL 18 和兼容的 Citus 14.1.x。
- 设置 `wal_level=logical` 和足够的 slot/walsender。
- 启用当前锁定版本要求的 Citus CDC 开关。
- 允许服务直接连接节点 advertised address，或提供稳定 endpoint override。
- 在 topology generation 激活前完成 publication、slot 和 identity 验证。

DDL 只能从 coordinator 执行。source event trigger 必须带 coordinator guard，worker 上的传播 DDL 不能产生重复 schema notice。

详细的 Citus 约束见 [ADR 0003](adr/0003-citus-cdc.md)。

## Schema 与表范围

默认发现所有非 system schema 中的普通、partitioned 和 Citus 逻辑表，但逐表执行准入检查。新建表不会绕过准入；通过后创建新 table generation。

### Supported

- 有普通、立即生效、非 deferrable primary key 的 heap row table。
- 单列或复合主键；每个 key column 的类型、collation 和目标 equality 必须验证兼容。
- PostgreSQL declarative range/list/hash partition，仅在 Cloudberry 能精确表达并通过兼容测试时支持。
- Citus hash-distributed row table，且 primary key 包含 distribution column。
- stored generated column，作为目标普通列物化；publication 必须实际输出其值。
- 列 default、NOT NULL 和可精确翻译的 CHECK 只作为 schema mirror；源仍是写入权威。

Cloudberry 目标表首批固定为 heap，`DISTRIBUTED BY` 使用源 primary key 的全部 key column。主键变化、partition 变化或 distribution 变化不会在线修改现有 generation。

### Validation-gated

以下有设计路径，但在真实 PG18/Citus/Cloudberry 矩阵通过前默认 blocked：

- Citus reference table。
- Citus coordinator-local table。
- Citus single-shard table。
- 复杂但理论可表达的多层 partition。
- 源 primary key 使用 `INCLUDE` column 的表。
- 能力矩阵中尚未覆盖的新 PG18 built-in type 或 typmod。

### Rejected

- 没有 primary key、只存在 unique index，或 replica identity 不是普通 primary key 的表。
- unlogged、temporary、foreign table 和 materialized view。
- Citus columnar、schema-sharded、append/range distributed table。
- Citus distribution key 不在 primary key 中的表。
- virtual generated column，以及依赖目标不可表达函数/extension 的 stored generated column。
- 需要目标 trigger、rule、RLS、sequence ownership 或外部对象才能保持行语义的表。
- 无法验证一致 equality/hash/collation 行为的主键。

拒绝一张表默认只把该表置为 `BLOCKED`，其他表继续。publication、slot、node coverage 或 source identity 的全局错误阻塞整个 pipeline。

## 主键与行变更

primary key 是行身份和幂等性的唯一依据：

- `INSERT` 写入完整当前行。
- `DELETE` 必须包含 old primary key；缺失时阻塞表。
- 普通 `UPDATE` 以 new primary key upsert。
- primary key update 显式转换成 `DELETE old_key` 和 `UPSERT new_row`。
- Citus distribution key update 同样是 delete + insert，不使用 `ON CONFLICT DO UPDATE` 修改分布列。

`REPLICA IDENTITY DEFAULT` 是正常配置。若某 DDL 或插件行为导致 update/delete 不再携带所需 key，服务暂停该表，而不是依赖全行模糊匹配。

TOAST 行值必须区分：

```text
Null             SQL NULL
UnchangedToast   本事件未携带值，目标保留旧列
Text(bytes)      pgoutput text encoding，按已验证类型解析
Binary(bytes)    pgoutput binary encoding，按已验证类型解析
```

`UnchangedToast` 不能写成 NULL。新行或目标不存在旧行时出现 `UnchangedToast` 属于契约破坏，进入 repair/rebuild。

## 类型白名单

类型注册表以 source namespace/name/kind/typmod、目标能力和 codec 共同决定支持状态。仅名称相同不代表兼容。PG18 有而 Cloudberry 2.1/PG14.4 不具备的语义默认拒绝。

| 源类型族 | 目标策略 | 状态 |
| --- | --- | --- |
| `boolean`, `int2`, `int4`, `int8` | 同名精确类型 | supported |
| `numeric(p,s)` 与无 typmod `numeric` | 保留 precision/scale 语义 | supported，超目标上限时拒绝 |
| `float4`, `float8` | 保留 IEEE 值，包括非有限值测试 | supported |
| `text`, `varchar(n)`, `char(n)` | 保留 typmod，验证编码/collation | supported |
| `bytea`, `uuid` | 同名精确类型 | supported |
| `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `interval` | 保留 typmod，固定编码规则 | supported |
| `json`, `jsonb` | 同名类型，按数据库语义写入 | supported |
| `bit(n)`, `varbit(n)` | 保留 typmod | supported |
| `inet`, `cidr`, `macaddr`, `macaddr8` | 版本能力验证后同名写入 | supported |
| 上述 supported 标量的一维或多维数组 | 保留维度、lower bound、NULL element | supported，逐元素 codec 必须通过 |
| PostgreSQL enum | 在隔离 namespace 创建真实 enum | supported；删除/重排 label 需重建 |
| domain | 仅可精确翻译 base type 与约束时创建真实 domain | supported，否则 blocked |
| `money`, `xml`, full-text 类型 | 不依赖显示/locale 做近似转换 | rejected |
| range、multirange、composite | 不做字符串降级 | rejected |
| `oid`, `reg*`, `xid*`, `cid`, `tid`, `pg_lsn` | 实例本地身份无稳定目标语义 | rejected |
| extension 类型，包括 PostGIS、`citext` | 没有通用精确契约 | rejected |
| unknown/user-defined base type | 没有注册 codec | rejected |

初始实现可以只解锁表中已经完成 codec 与 round-trip 测试的 supported 子集。表格是产品目标，不允许用通用 `Display`/字符串 fallback 提前宣称支持。

## Collation、浮点和时间语义

- 文本普通列可以保留值而不保证排序计划相同。
- 文本 primary key、unique key 或 distribution key 只有在 Cloudberry 可验证兼容 deterministic collation 时才准入。
- nondeterministic ICU collation 和源目标版本行为不同的 collation 默认拒绝用于 key。
- `timestamptz` 按绝对时间写入；session timezone 不参与持久化语义。
- `timestamp` 和 `time` 不附加时区推断。
- `NaN`、正负 infinity、`-0` 必须有 round-trip 和 target equality 测试，不能经 JSON 中转。

## DDL 分类

每个 DDL notice 触发 catalog rescan。schema fingerprint 是列顺序、类型身份/typmod、nullability、generated 属性、key、partition 和 Citus distribution 的规范化 hash。

### 在线跟随候选

- ADD COLUMN：nullable，或有能在目标安全表达的 immutable default。
- DROP COLUMN：先停止新 schema apply，再修改目标。
- RENAME COLUMN：保留稳定 source attribute identity，目标执行 rename。
- SET/DROP DEFAULT。
- SET/DROP NOT NULL：先验证目标现有数据。
- 明确白名单中的安全 widening，例如受验证的整数或 varchar 扩展。
- enum 追加 label，前提是目标版本支持等价顺序。

所有候选仍需通过目标 capability check；检查失败不会自动降级。

### 必须 shadow rebuild

- ADD/DROP/ALTER PRIMARY KEY。
- key collation、distribution column 或 Citus distribution 属性变化。
- 非白名单类型转换、缩窄 typmod、enum 删除或重排。
- generated expression 变化。
- 不能在 Cloudberry 原地精确完成的 partition 变化。
- TRUNCATE，避免误把来源不明的 truncate 当作普通 delete。

### 必须阻塞

- DDL 使表进入 rejected 类型。
- catalog fingerprint 无法解析，或事件和 relation message 不一致。
- worker 直接执行 Citus DDL。
- 未受管 topology drift、未知 shard relation 或缺失 node slot。

DROP TABLE 不立即物理删除目标。目标对象改名并进入 quarantine，默认保留 30 天；恢复或最终清理都需要显式操作和审计记录。

## 命名、映射与 ownership

每个 source 配置不可为空且全局唯一的 `source_prefix`。默认映射：

```text
<prefix> + source database + source schema -> target schema
source table                              -> target table
```

显式规则可以覆盖 schema/table 名，但必须是双射。同一 pipeline 内两个 source relation 不能映射到同一 target relation。

第一次接入时：

- 目标不存在：创建对象并写 ownership fingerprint。
- 目标存在且 ownership 匹配当前 pipeline/generation：按 checkpoint 恢复。
- 目标存在但未受管理：拒绝启动。
- `adopt`：要求管理员二次确认、结构兼容检查和全量 canonical hash；通过前不写业务数据。

改变映射、prefix、source database identity 或 target profile 会创建新 generation，不原地挪动 active 表。

## WAL 与运维契约

源数据库必须为最大快照时间、最长可接受故障时间和峰值 WAL 留出容量，但 slot 不得无限保留 WAL。每个 pipeline 配置：

- retained WAL warning 和 hard limit。
- 最大未 ACK 时间。
- 最大 transaction/spool bytes。
- snapshot 和 reconciliation 的源端 I/O 限速。

达到 hard limit 前服务暂停新工作并告警；若继续会危及源磁盘，运维可以显式失效 slot。之后必须执行新 generation 快照，不能从猜测位置续传。

以下操作必须先走 `prepare` 流程：

- Citus 添加/移除 worker、drain、rebalance、split。
- PostgreSQL/Citus major/minor 升级影响 logical decoding 时。
- publication、slot、event trigger 或 `pg2cb_meta` 变更。
- 会改变主键、partition、distribution 或 table kind 的 DDL。

## 最终一致验收

表只有同时满足以下条件才显示 `HEALTHY`：

- snapshot 已完成，WAL lag 在配置阈值内。
- target checkpoint 是每个 source node 已应用的连续前缀。
- schema fingerprint 与 active generation 一致。
- 最近一次 count 与 PK 分块 canonical hash reconciliation 通过。
- 没有未确认的 topology、DDL、type 或 ownership drift。

“复制仍在运行”不等同于“数据已验证一致”。
