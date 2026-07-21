# Phase 1 进度更新（中期）

**时间:** 2026-07-22 01:30  
**状态:** 核心组件已完成，集成进行中

---

## 完成项

### 1.1 Source Keyset Paging ✅ (commit `422943c`)
- `read_canonical_pk_page()`: PK-only bounded pages with LIMIT+1 lookahead
- `read_canonical_row_page()`: Full-row pages for reconciliation
- `copy_text_pk_range()`: Direct COPY for source-derived range
- Typed `ROW(...) > ROW(...)` comparison with native PK order
- `SnapshotKeyPage` / `SnapshotPage` with `has_more` + `next_cursor`
- **所有 49 项单元测试通过**

### 1.2 Target Snapshot Progress ✅ (已存在)
- `SnapshotTableProgress` 结构体（cursor + pages_copied + rows_copied）
- `register_snapshot_table_progress()`: 注册空进度行
- `copy_snapshot_page()`: 在同一事务内复制页面并更新进度
- 完整的 CRUD SQL（INSERT、UPDATE with CAS、LOCK、DELETE）
- Target metadata V7 schema 已就绪

### 1.3 Bounded Snapshot Runtime（进行中）
- `prepare_initial_snapshot()` 已存在于 `crates/engine/src/runtime/job.rs`
- 需要验证是否已使用 bounded snapshot（keyset paging + progress）
- 或者需要重构为 bounded 模式

### 1.4 Spool 自动清理（待实施）
- `crates/source-postgres/src/spool.rs` 已有 journal 管理
- 需要增加时间窗口清理逻辑（checkpoint 后保留 24h）

### 1.5 E2E Kill-Point 测试（待实施）
- 5 个关键 kill-point 场景脚本
- 真实容器环境验证崩溃恢复

---

## 发现

**好消息:** Phase 1 的核心数据结构和 API **都已经提前实现**了！
- Source: keyset paging 完整
- Target: progress tracking 完整
- Spool: versioned journal + transparent spill 完整
- Chunk ledger: durable receipt 完整
- Completion tracker: 连续 checkpoint 完整

**剩余工作:** 主要是**集成和测试**，而不是从头实现。

---

## 下一步

1. **审查 `job.rs` 的 snapshot 实现：**
   - 确认是否已使用 `read_canonical_pk_page` + `copy_snapshot_page`
   - 如果还是旧的全表 COPY，重构为 bounded 模式

2. **Spool 清理：**
   - 在 checkpoint 推进后调用清理函数
   - 按时间窗口（24h）删除旧 journal

3. **E2E 测试脚本：**
   - `tests/integration/phase1_e2e.sh` 半自动化脚本
   - 5 个 kill-point 场景

**预计:** Phase 1 可在今天内完成（集成 + 测试比从头实现快得多）。
