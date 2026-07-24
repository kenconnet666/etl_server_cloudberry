# PostgreSQL 与 Cloudberry 分析性能价值基准

最后测量：2026-07-23。

## 结论

当前结果**部分验证**项目价值，但不支持“Cloudberry 对所有查询都更快”的表述。

- 4 segment Cloudberry AOCO 总关系大小为 279,786,072 bytes，PostgreSQL heap 为
  984,096,768 bytes，降低 **71.6%**。
- 500 万行、单并发、warm-cache 下，高基数 Top-N 聚合快 **4.50 倍**；全表/过滤/宽列扫描快
  **1.08-1.41 倍**。
- 维表 join 的 Cloudberry 耗时高 **32.2%**；101 行主键范围查询慢 **28.9 倍**。点查是有意
  保留的 row-store-friendly 控制项，说明 PostgreSQL 仍应承担事务与点查服务。
- 排除点查控制项后，五个 OLAP 查询的无权重几何平均加速为 **1.44 倍**，尚未达到执行计划的
  2 倍 go/no-go 门槛。需要真实业务查询权重、50M+ 数据和并发结果才能形成产品级价值声明。

## 环境与口径

| 项目 | PostgreSQL | Cloudberry |
| --- | --- | --- |
| 版本 | PostgreSQL 18.4 | Apache Cloudberry 2.1.0，GPORCA on |
| 存储 | 原生 heap + 主键 | `ao_column`，ZSTD level 1 + 主键 |
| 执行拓扑 | 单实例，2 个 parallel worker + leader | 单 host，4 个 primary segment |
| 容器限制 | 无 CPU/memory limit | 无 CPU/memory limit |
| Host | WSL Debian，12 logical CPU，30 GiB RAM | 同左 |

事实表为 500 万行，包含 11 列和 96 字符 payload；维表为 10 万行。两端使用相同的确定性数据、
类型、主键和 SQL。Cloudberry 按事实表 `id` 分布，四个 segment 分别为 1,249,354、1,249,079、
1,250,529 和 1,251,038 行，没有实质数据倾斜。

每项先执行一次不报告的预热，再运行五次 `EXPLAIN ANALYZE`；表中为 execution time 中位数。
查询串行运行，未包含客户端连接时间。六个有序结果集在两端逐项计算 SHA-256，全部一致。

## 4 Segment 结果

| 查询 | PostgreSQL median | Cloudberry median | PG / CB | 判断 |
| --- | ---: | ---: | ---: | --- |
| Q1 窄列全表聚合 | 1,203.528 ms | 998.979 ms | **1.21x** | 小幅收益 |
| Q2 日期过滤 + 小基数组合 | 325.306 ms | 302.298 ms | **1.08x** | 基本持平 |
| Q3 压缩宽列扫描 | 602.333 ms | 428.367 ms | **1.41x** | 有收益 |
| Q4 高基数 customer Top-N | 2,367.048 ms | 526.437 ms | **4.50x** | 明显收益 |
| Q5 事实表 + 维表 join | 733.604 ms | 969.555 ms | **0.76x** | Cloudberry 慢 32.2% |
| Q6 主键 101 行范围查询 | 0.089 ms | 2.568 ms | **0.035x** | Cloudberry 慢 28.9x |

AOCO 在 Q1 计划中由 4 个 segment 各扫描约 125 万行再 `Gather Motion 4:1`。PostgreSQL 使用两个
worker 加 leader 的三路 parallel scan。Q4 的主要收益来自分段 hash aggregate；Q5 则需要维表
broadcast 和多阶段 motion，在当前规模下抵消了列存与分段扫描收益。

## 单 Segment 对照

同一 5M 数据在默认单 primary segment Cloudberry 上，Q1/Q2/Q3/Q5 分别比当轮 PostgreSQL 慢
2.23/1.92/2.00/2.67 倍，只有 Q4 快 1.47 倍；点查慢约 24.8 倍。单 segment AOCO 大小为
251,460,816 bytes，仍节省 74.4% 存储。该对照证明列式压缩本身不保证查询加速，Cloudberry 的
分析价值依赖可并行的 segment 拓扑和具体算子形态。

## 下一轮门禁

1. 用实际 dashboard/report SQL 和查询频率替换无权重平均，保留本套查询作为回归基线。
2. 运行 5M/50M/200M 与 1/4/8 segment scale matrix，报告 speedup、segment efficiency 和数据
   倾斜；当前单 host 4 segment 只证明进程级并行，不证明多机扩展。
3. 增加 1/4/16 并发、冷缓存、资源隔离和 p95/p99；同时测源库业务负载被分析查询影响的程度，
   把“从 PostgreSQL 卸载分析”计入价值，而不只比较单条查询延迟。
4. 针对 Q5 检查 replicated dimension、业务分布键和统计信息策略，但不得为基准改变项目的主键
   current-state 正确性约束。任何物理设计优化都要同时复跑 CDC apply 和 reconciliation。

## 复现

脚本与数据定义位于 [`tests/benchmark`](../tests/benchmark/README.md)：

```bash
CBDB_CONTAINER=cbdb-bench CBDB_PORT=55434 CBDB_SEGMENTS=4 \
  bash tests/integration/cloudberry/build-local-image.sh
CLOUDBERRY_CONTAINER=cbdb-bench \
  bash tests/benchmark/cloudberry-vs-postgres.sh setup 5000000
CLOUDBERRY_CONTAINER=cbdb-bench \
  bash tests/benchmark/cloudberry-vs-postgres.sh run
```

该脚本只替换独立 `analytics_bench` 数据库中的同名 schema，不修改 integration `source`/`target`
数据库。`clean` 子命令只删除该 schema。
