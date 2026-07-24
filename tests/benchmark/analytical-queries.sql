\set ON_ERROR_STOP on
\pset pager off
SET statement_timeout = '10min';

\echo BENCH|q1_scan_aggregate|0
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q1_scan_aggregate;
\echo BENCH|q1_scan_aggregate|1
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q1_scan_aggregate;
\echo BENCH|q1_scan_aggregate|2
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q1_scan_aggregate;
\echo BENCH|q1_scan_aggregate|3
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q1_scan_aggregate;
\echo BENCH|q1_scan_aggregate|4
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q1_scan_aggregate;
\echo BENCH|q1_scan_aggregate|5
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q1_scan_aggregate;

\echo BENCH|q2_filtered_group|0
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q2_filtered_group;
\echo BENCH|q2_filtered_group|1
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q2_filtered_group;
\echo BENCH|q2_filtered_group|2
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q2_filtered_group;
\echo BENCH|q2_filtered_group|3
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q2_filtered_group;
\echo BENCH|q2_filtered_group|4
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q2_filtered_group;
\echo BENCH|q2_filtered_group|5
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q2_filtered_group;

\echo BENCH|q3_wide_column_scan|0
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q3_wide_column_scan;
\echo BENCH|q3_wide_column_scan|1
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q3_wide_column_scan;
\echo BENCH|q3_wide_column_scan|2
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q3_wide_column_scan;
\echo BENCH|q3_wide_column_scan|3
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q3_wide_column_scan;
\echo BENCH|q3_wide_column_scan|4
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q3_wide_column_scan;
\echo BENCH|q3_wide_column_scan|5
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q3_wide_column_scan;

\echo BENCH|q4_top_customers|0
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q4_top_customers;
\echo BENCH|q4_top_customers|1
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q4_top_customers;
\echo BENCH|q4_top_customers|2
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q4_top_customers;
\echo BENCH|q4_top_customers|3
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q4_top_customers;
\echo BENCH|q4_top_customers|4
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q4_top_customers;
\echo BENCH|q4_top_customers|5
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q4_top_customers;

\echo BENCH|q5_dimension_join|0
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q5_dimension_join;
\echo BENCH|q5_dimension_join|1
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q5_dimension_join;
\echo BENCH|q5_dimension_join|2
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q5_dimension_join;
\echo BENCH|q5_dimension_join|3
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q5_dimension_join;
\echo BENCH|q5_dimension_join|4
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q5_dimension_join;
\echo BENCH|q5_dimension_join|5
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q5_dimension_join;

\echo BENCH|q6_point_range|0
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q6_point_range;
\echo BENCH|q6_point_range|1
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q6_point_range;
\echo BENCH|q6_point_range|2
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q6_point_range;
\echo BENCH|q6_point_range|3
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q6_point_range;
\echo BENCH|q6_point_range|4
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q6_point_range;
\echo BENCH|q6_point_range|5
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY ON) SELECT * FROM analytics_bench.q6_point_range;

