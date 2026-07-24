\set ON_ERROR_STOP on

CREATE VIEW analytics_bench.q1_scan_aggregate AS
SELECT count(*) AS row_count,
       sum(quantity) AS units,
       round(sum(quantity * unit_price * (1 - discount)), 2) AS revenue
  FROM analytics_bench.fact_sales;

CREATE VIEW analytics_bench.q2_filtered_group AS
SELECT region_id,
       channel_id,
       sum(quantity) AS units,
       round(sum(quantity * unit_price * (1 - discount)), 2) AS revenue
  FROM analytics_bench.fact_sales
 WHERE event_date >= DATE '2025-01-01'
   AND event_date < DATE '2025-04-01'
 GROUP BY region_id, channel_id
 ORDER BY region_id, channel_id;

CREATE VIEW analytics_bench.q3_wide_column_scan AS
SELECT region_id,
       sum(length(payload)) AS payload_characters
  FROM analytics_bench.fact_sales
 WHERE status = 'R'
 GROUP BY region_id
 ORDER BY region_id;

CREATE VIEW analytics_bench.q4_top_customers AS
SELECT customer_id,
       round(sum(quantity * unit_price * (1 - discount)), 2) AS revenue
  FROM analytics_bench.fact_sales
 WHERE event_date >= DATE '2025-01-01'
 GROUP BY customer_id
 ORDER BY revenue DESC, customer_id
 LIMIT 20;

CREATE VIEW analytics_bench.q5_dimension_join AS
SELECT product.category_id,
       sum(sales.quantity) AS units,
       round(sum(sales.quantity * sales.unit_price * (1 - sales.discount)), 2) AS revenue
  FROM analytics_bench.fact_sales AS sales
  JOIN analytics_bench.dim_product AS product USING (product_id)
 WHERE sales.event_date >= DATE '2025-01-01'
 GROUP BY product.category_id
 ORDER BY product.category_id;

CREATE VIEW analytics_bench.q6_point_range AS
SELECT *
  FROM analytics_bench.fact_sales
 WHERE id BETWEEN (:bench_rows::bigint / 2) AND (:bench_rows::bigint / 2 + 100)
 ORDER BY id;
