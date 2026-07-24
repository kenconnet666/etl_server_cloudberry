# DuckDB Iceberg 与 DuckLake 查询对照

最后测量：2026-07-23，Windows，DuckDB 1.5.4，8 threads，500 万行事实表、10 万行维表，
每项 1 次预热后 5 次测量。Iceberg 使用 PyIceberg 0.11.1 建 catalog；两个 catalog 注册
**同一组 9 个 ZSTD Parquet 数据文件**，所以数据文件大小和内容相同。查询由 DuckDB 执行，
结果物化到客户端并计算 SHA-256。

证据文件：[`target/duckdb-iceberg-vs-ducklake/result.json`](../target/duckdb-iceberg-vs-ducklake/result.json)
和 `result.csv`。复现脚本为
[`tests/benchmark/duckdb-iceberg-vs-ducklake.py`](../tests/benchmark/duckdb-iceberg-vs-ducklake.py)。

## 结果

单位为毫秒，表中是 5 次中位数；`DuckLake / Iceberg` 大于 1 表示 Iceberg 更快。

| 查询 | Iceberg 热 | DuckLake 热 | 比值 | Iceberg 新连接 | DuckLake 新连接 | 比值 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 窄列聚合 | 83.09 | 85.51 | 1.03x | 81.03 | 167.82 | 2.07x |
| Q2 日期过滤分组 | 54.81 | 69.70 | 1.27x | 58.68 | 159.36 | 2.72x |
| Q3 宽 payload 扫描 | 310.05 | 350.10 | 1.13x | 362.69 | 387.98 | 1.07x |
| Q4 customer Top-N | 222.51 | 197.50 | 0.89x | 212.19 | 353.56 | 1.67x |
| Q5 维表 join | 197.97 | 119.98 | 0.61x | 194.10 | 212.69 | 1.10x |
| Q6 101 行范围查询 | 49.49 | 50.71 | 1.02x | 41.41 | 100.57 | 2.43x |

所有查询的结果哈希均一致。热连接下，DuckLake 在 Top-N 和 join 上分别约快 1.13x、1.65x；
Iceberg 在日期过滤和宽 payload 扫描上分别约快 1.27x、1.13x，窄列聚合和范围查询基本持平。
新连接下，DuckLake 的 catalog/metadata 初始化成本更高，六项均慢于 Iceberg，Q2 约慢
2.72x，Q6 约慢 2.43x。

## 解释边界

这是 DuckDB 单机、同一 Parquet 输入的 reader/catalog 对照，不是写入吞吐、压缩率、
compaction、UPDATE/DELETE 或多进程并发结论。新连接测试没有清空操作系统文件缓存，不能称为
硬件冷缓存；它主要反映进程和 metadata 初始化成本。默认 DuckLake writer 使用 Snappy 的
另一轮结果没有并入格式结论，因为它改变了物理压缩输入；若评估生产配置，还需单独报告
writer 默认、文件大小、维护成本和真实业务 SQL 权重。

复现：

```powershell
python tests/benchmark/duckdb-iceberg-vs-ducklake.py `
  --rows 5000000 --samples 5 --threads 8
```
