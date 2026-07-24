\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS analytics_bench CASCADE;
CREATE SCHEMA analytics_bench;

CREATE TABLE analytics_bench.dim_product (
    product_id integer PRIMARY KEY,
    category_id integer NOT NULL,
    brand_id integer NOT NULL,
    product_name text NOT NULL
);

INSERT INTO analytics_bench.dim_product
SELECT product_id,
       ((product_id * 17) % 200 + 1)::integer,
       ((product_id * 29) % 2000 + 1)::integer,
       'product-' || product_id::text
  FROM generate_series(1, 100000) AS products(product_id);

CREATE TABLE analytics_bench.fact_sales (
    id bigint PRIMARY KEY,
    event_date date NOT NULL,
    customer_id integer NOT NULL,
    product_id integer NOT NULL,
    region_id smallint NOT NULL,
    channel_id smallint NOT NULL,
    quantity smallint NOT NULL,
    unit_price numeric(12, 2) NOT NULL,
    discount numeric(5, 4) NOT NULL,
    status character(1) NOT NULL,
    payload text NOT NULL
);

INSERT INTO analytics_bench.fact_sales
SELECT id,
       DATE '2021-01-01' + ((id * 17) % 1826)::integer,
       ((id * 7919) % 500000 + 1)::integer,
       ((id * 3571) % 100000 + 1)::integer,
       ((id * 13) % 32 + 1)::smallint,
       ((id * 7) % 5 + 1)::smallint,
       ((id % 10) + 1)::smallint,
       (5 + ((id * 19) % 20000) / 100.0)::numeric(12, 2),
       (((id * 23) % 3000) / 10000.0)::numeric(5, 4),
       CASE id % 4
           WHEN 0 THEN 'N'
           WHEN 1 THEN 'P'
           WHEN 2 THEN 'S'
           ELSE 'R'
       END::character(1),
       repeat(md5(id::text), 3)
  FROM generate_series(1, :bench_rows::bigint) AS sales(id);

ANALYZE analytics_bench.dim_product;
ANALYZE analytics_bench.fact_sales;

