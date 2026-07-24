# 下一步执行计划

更新日期：2026-07-23。

## 当前判断

Standalone 的 bounded snapshot、durable spool、原子批处理、表级 DDL reload 和周期
reconciliation 已进入真实 PostgreSQL 18 / Cloudberry 2.1 路径。当前主风险已经从“能否持续复制”
转为“DDL/reconciliation 在外部故障下是否可证明恢复”以及“分析侧收益是否足以抵消独立 ETL
链路的运维成本”。因此下一阶段先关闭可靠性证据，再启用在线 ALTER 和并行优化。

## P0：关闭 Standalone 生产长稳门禁（1-2 周）

2026-07-23 已关闭：普通 PostgreSQL 的 Citus worker guard 回归、reconciliation reload 前置失败
原子清理、局部 reload 后 full rebuild 的 table generation 单调性、active schema drift 自动 rebuild，
以及独立进程 SIGKILL、source/target 网络断连和 PostgreSQL/Cloudberry 容器重启矩阵。5 分钟 mixed
DML+三阶段 DDL+目标篡改 soak 已收敛（最终 10,477 行，RSS 峰值约 48.6 MiB）；production image、PostgreSQL 18 control volume、migration、
`*_FILE` secret、non-root/read-only runtime、Caddy 和 Prometheus 配置均已做容器级验证。控制库
custom-format 备份、归档校验和临时库真实 restore drill 已通过（恢复 8 张控制表且自动清理临时库）；
Compose/Caddy/Prometheus/备份脚本静态校验已加入 CI。新增分页预算回归修复了小于 1 MiB
`batch.max_bytes` 时主键页容器开销导致的无限 reload 重试；真实 PG18 分页测试和 Cloudberry
mixed soak 均已覆盖该路径。

追加短时证据：180 秒 mixed soak（7,020 insert、3,510 update、702 delete，最终 6,319 行）和
120 秒 64 KiB budget soak（4,640 insert、2,320 update、464 delete，最终 4,177 行）均逐行一致，
RSS 峰值分别约 46.6/47.1 MiB，spool 均归零；后者特意触发了 byte-bounded page continuation。

本轮又补齐了 soak 的 CSV/JSON 证据采集：lag、CPU、RSS、spool、retained WAL、元数据/隔离计数
和 reconciliation 修复耗时均可按样本导出；120 秒真实 PG18/Cloudberry 运行 24 个样本全部完整，
lag p50/p95/p99 为 4,248/42,128/52,720 bytes，RSS 峰值约 46.6 MiB，retained WAL 峰值约
86 KiB，spool 为 0，reconciliation 约 4.36 秒修复。新增
`tests/integration/standalone-soak-gate.sh` 会拒绝短时结果冒充 24 小时门禁。process E2E
故障注入已加串行互斥，SIGKILL、网络断连、PG/Cloudberry 容器重启四项完整运行 4/4 通过；
PG18 source contract 与 schema-event ignored suites 也在并发运行下通过。

1. 将同一 mixed workload 延长到 24 小时，记录 p50/p95/p99 lag、RSS、spool、retained WAL、
   quarantine/metadata 增长和每次恢复时间；任何资源单调增长或最终数据差异都阻断发布。
2. 补齐 rename/drop/recreate、同事务多表 DDL、rapid DDL 与 reconciliation 同时发生的 nightly
   矩阵；无法证明 table-local 的场景允许显式 full rebuild，但不得无限重试同一 generation。
3. 控制库备份、forward migration 和隔离 restore drill 已通过；剩余一次上一版本镜像与控制库
   备份成对回滚演练，并记录完整 RTO/RPO。不得把隔离临时库 restore 当成线上停机回滚证据。
4. 将 `runtime/job.rs` 的 schema/reconciliation executor 拆分成独立模块。只做等价迁移，状态机
   改动另行提交。

退出条件：Phase 2 故障矩阵全部自动化进入 CI/nightly；所有 commit ambiguity 都有持久状态
证明；24 小时混合 DML+DDL soak 无数据差异、无无限增长的 WAL/spool/metadata，且回滚演练通过。

## P1：逐项开放在线 ALTER（1-2 周）

按 ADD nullable column、RENAME column、widening、DROP column 的顺序，在 Cloudberry 2.1 上分别
验证 AOCO DDL、并发读写、事务回滚、进程/目标重启和重复 WAL。每项只有在 crash-safe 原地 ALTER
证据完整后才从 table-local reload 切换到 Online；default/nullability 和共享 enum/domain 依赖图
后置。任何不确定场景继续局部 reload，不扩大为 pipeline rebuild。

退出条件：每个白名单动作有 capability test、真实 E2E 和 kill-point 证据；回退路径仍可幂等接管。

## P1：验证项目价值（并行开展，1 周得到首轮结论）

价值证据拆成三个互不替代的维度：

1. 数据新鲜度与复制成本：沿用 `standalone-benchmark.md`，报告 snapshot/CDC rows/s、p95 lag、
   source WAL、RSS、spool 和恢复时间。
2. 分析收益：运行 `tests/benchmark/cloudberry-vs-postgres.sh`，比较相同数据和 SQL 在 PostgreSQL
   heap 与 Cloudberry AOCO 的查询中位数、存储占用，同时保留点查反例。
3. 规模收益：在 1/4/8 segment 上运行 5M/50M/200M scale factor，并增加 1/4/16 并发用户。
   单 segment 结果只能证明列存价值信号，不能声明 MPP 扩展性。

2026-07-23 的 5M 首轮结果已完成：4 segment AOCO 比 PostgreSQL heap 节省 71.6% 存储，五个
OLAP 查询的无权重几何平均加速为 1.44 倍，其中高基数 Top-N 聚合为 4.50 倍，但维表 join 慢
32%；点查控制项慢 28.9 倍。结果集哈希全部一致。详细环境、原始口径和限制见
[`analytics-value-benchmark.md`](analytics-value-benchmark.md)。这证明了存储密度与特定聚合价值，
尚未通过“代表性分析组合至少 2 倍”的整体门槛。

首轮 go/no-go 口径：代表性查询按实际业务权重计算几何平均加速比；AOCO 存储至少降低 30%，
分析组合至少提升 2 倍，p95 数据新鲜度满足 SLA，且 24 小时正确性/恢复门禁通过。若收益只来自
单个宽列扫描，或 join/group 在多 segment 没有稳定收益，应收缩产品价值表述，而不是继续用写入
吞吐代替分析价值。

## P2：性能分层与 72 小时稳定性（2-3 周）

1. 先补 Rust pgoutput decode-only、spool、normalize、target COPY/apply 分层基准，定位 46-48k
   rows/s CDC 上限的真实占比。
2. 只有 decode/spool 有足够可重叠成本时，实验完整事务的有界 channel；DDL/TRUNCATE 必须 drain，
   ACK 仍只跟随 durable continuous checkpoint。
3. 在真实多 segment 正收益前，不启用 per-table target connection pool；现有单 segment 数据已证明
   2/4 连接比单连接低约 32%-33%。
4. 建立 72 小时 DML+DDL+reconciliation soak，记录 p50/p95/p99 lag、CPU、RSS、spool、retained
   WAL、Cloudberry 膨胀和恢复时间。

## P3：HA/Citus（前述门禁完成后）

实现 PG18 failover slot continuity 和 stable endpoint，再将同一 NodeStream/Spool/Completion 模型
扩展为 Citus per-worker slot/checkpoint vector。Citus 当前继续 fail-closed；在 worker add/drain、
rebalance、failover 和 topology drift 矩阵完成前不对外声明支持。
