# Phase 2: DDL 紧密跟随设计(ADR)

> 状态:设计中。本文档规划 DDL 事件 v2、table-level transition 和 dynamic binding registry 的实现路径。

## 目标

普通 DDL(ADD COLUMN、DROP COLUMN、RENAME、widening 等)不再触发全量 pipeline rebuild,而是 table-level shadow reload + catch-up + cutover,实现在线 schema evolution。

## 当前机制(Phase 1)

1. source `ddl.rs` event trigger 发出 `pg2cloudberry_ddl_v1` 消息
2. engine adapter 检查 `requires_barrier(DdlMessage)` → 返回 `SchemaBarrier` error
3. runtime job 捕获 error,调用 `request_pipeline_rebuild` → 全量 snapshot 重做

退出条件:pipeline 停止 → snapshot → activation。DDL 期间停服。

## Phase 2 架构

### 1. DDL Event v2 Envelope

扩展 `DdlMessage`,增加:
- `table_transitions: Vec<TableTransition>`:受影响表的 before/after schema + transition type
- `schema_event_id: Uuid`:持久化 event 标识,支持 idempotent replay
- `whitelisted_operations: Vec<DdlOp>`:已知安全的在线 DDL 类型

`TableTransition`:
```rust
struct TableTransition {
    relation_id: u32,
    before_generation: u64,
    after_generation: u64,
    before_schema: TableSchema,  // 从 WAL 前 snapshot 捕获
    after_schema: TableSchema,   // DDL 后立即 catalog 快照
    transition_type: TransitionType,
}

enum TransitionType {
    AddColumn { nullable: bool, has_default: bool },
    DropColumn,
    RenameColumn { old_name: String, new_name: String },
    AlterType { widening: bool },  // widening=true 表示兼容(int4->int8)
    AddTable,  // 新表自动准入
    DropTable, // → quarantine
}
```

### 2. 持久化 Schema Event

新表 `pg2cb_meta.schema_events`:
```sql
CREATE TABLE schema_events (
    event_id UUID PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    source_lsn PG_LSN NOT NULL,
    source_xid BIGINT NOT NULL,
    command_tag TEXT NOT NULL,
    table_transitions JSONB NOT NULL,  -- Vec<TableTransition> 序列化
    whitelisted_operations JSONB NOT NULL,
    emitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    state TEXT NOT NULL,  -- 'pending' | 'in_transition' | 'completed' | 'failed'
    UNIQUE (pipeline_id, source_lsn, source_xid)
);
```

用途:
- DDL 到达时持久化 event,防止 process crash 丢失
- `state='pending'` → runtime 启动时恢复未完成 transition
- Idempotent replay:相同 LSN+xid 的 DDL 只处理一次

### 3. Dynamic Binding Registry

当前 `TableBindingRegistry` 是 run-scoped immutable `Arc<HashMap>`。Phase 2 改为:
```rust
pub struct DynamicBindingRegistry {
    bindings: Arc<RwLock<HashMap<u32, Arc<TableBinding>>>>,
    // relation_id → binding
}
```

Row hot path:
```rust
// 读路径:shared lock,Arc clone
let binding = registry.bindings.read().get(&relation_id).cloned();

// DDL transition:exclusive lock,swap Arc
registry.bindings.write().insert(relation_id, new_binding);
```

DDL transition 期间旧 binding 的 Arc 仍被 inflight batches 持有,直到它们 commit 完成;新 rows 拿到新 binding。

### 4. Table-Level Transition 流程

DDL 到达 → 不返回 `SchemaBarrier`,而是:

1. **Pause table**:该表的 CDC apply 进入 barrier 等待(spool 继续接收)
2. **Persist event**:`schema_events` 插入 `state='pending'`
3. **Shadow reload**:
   - 复用 `begin_snapshot_pages` + bounded paging
   - 用 DDL 后的 `after_schema` 创建新 shadow
   - 从 source 当前 snapshot 全量重新加载(PK 一致性保证)
4. **Catch-up**:从 spool 回放该表 barrier 后的 CDC changes(apply 到新 shadow)
5. **Cutover**:
   - 原子 RENAME:old_target → quarantine,shadow → target
   - Swap binding registry:relation_id → new binding
   - Resume CDC apply
6. **Mark completed**:`state='completed'`

崩溃恢复:runtime 启动时扫描 `state='pending'|'in_transition'`,按 LSN 顺序重放。

### 5. 在线 DDL 白名单

初版只支持安全操作:
- `ADD COLUMN ... DEFAULT NULL` 或 `NOT NULL DEFAULT <literal>`(no rewrite)
- `DROP COLUMN`(target 侧忽略该列,source 继续发送旧 schema 直到 cutover)
- `RENAME COLUMN`(binding plan 更新列名映射)
- `ALTER TYPE` widening(int4→int8,varchar(10)→varchar(20))
- `ADD TABLE`(新表自动准入,复用 snapshot 路径)

不支持(仍 fail-closed):
- `ALTER TYPE` narrowing 或不兼容转换
- `ADD CONSTRAINT`(需要 validation scan)
- Partition DDL(Phase 4)
- Citus distribution key 变更(Phase 4)

### 6. DROP 与 Quarantine

`DROP TABLE` → 不立即删除 target,而是:
1. target table RENAME 到 `pg2cb_quarantine.<uuid>`
2. Binding registry 移除该表
3. 后续 CDC 忽略该 relation_id
4. 周期性 GC:保留 `quarantine_retention` 天后物理删除

用户可手动 `SELECT * FROM pg2cb_quarantine.<uuid>` 救回数据。

## 实现路径

### Milestone 1: DDL Event v2 + 持久化(本次会话)
- [ ] source `ddl.rs` 增强:emit v2 envelope with `TableTransition`
- [ ] target migration 添加 `schema_events` 表
- [ ] engine 解析 v2,持久化到 `schema_events`
- [ ] 单测:v2 envelope 序列化/反序列化

### Milestone 2: Dynamic Binding Registry
- [ ] 重构 `TableBindingRegistry` → `Arc<RwLock<HashMap>>`
- [ ] Hot path benchmark:验证 RwLock 读性能
- [ ] Transition API:`registry.swap_binding(relation_id, new_binding)`

### Milestone 3: Shadow Reload + Cutover
- [ ] Table barrier 机制(spool 继续,apply 等待)
- [ ] Shadow reload 复用 `begin_snapshot_pages`
- [ ] Catch-up from spool
- [ ] 原子 cutover(RENAME + registry swap)

### Milestone 4: 白名单 + E2E
- [ ] `ADD COLUMN` transition handler
- [ ] `DROP COLUMN` transition handler
- [ ] Integration test:DDL + concurrent DML
- [ ] Crash recovery test

## 风险与 Tradeoff

**风险**:
- Dynamic binding 引入 RwLock 争用(hot path 读锁)
- Shadow reload + catch-up 期间表双写(target 负载增加)
- Cutover RENAME 不是真正的 MVCC(短暂锁表)

**Tradeoff**:
- Phase 2 只支持白名单 DDL;复杂 DDL 仍 fail-closed 或手动 rebuild
- Quarantine 机制增加存储开销
- 需要额外 metadata 表(`schema_events`)

## 退出条件

- 并发 DML + DDL 矩阵通过(ADD/DROP/RENAME COLUMN)
- 同事务多次 DDL、rapid DDL、process crash + replay 测试通过
- 普通 DDL 不调用 `request_pipeline_rebuild`
