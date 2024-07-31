-- name: test_agg_state_hll_sketch_count

CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
-- PARTITION BY (province, dt) 
DISTRIBUTED BY HASH(id) BUCKETS 4;

CREATE TABLE test_hll_sketch (
  dt VARCHAR(10),
  hll_id agg_state<hll_sketch_count(varchar not null)> agg_state_union,
  hll_province agg_state<hll_sketch_count(varchar)> agg_state_union,
  hll_age agg_state<hll_sketch_count(varchar)> agg_state_union,
  hll_dt agg_state<hll_sketch_count(varchar not null)> agg_state_union
)
AGGREGATE KEY(dt)
PARTITION BY (dt) 
DISTRIBUTED BY HASH(dt) BUCKETS 4;

-- basic test for empty table
select id, province, from_binary(hll_sketch_count_state(id), 'hex') from t1 order by 1, 2 limit 3;
select id, province, from_binary(hll_sketch_count_state(province), 'hex') from t1 order by 1, 2 limit 3;
select id, province, from_binary(hll_sketch_count_state(age), 'hex') from t1 order by 1, 2 limit 3;
select id, province, from_binary(hll_sketch_count_state(dt), 'hex') from t1 order by 1, 2 limit 3;

select hll_sketch_count_merge(hll_province) from test_hll_sketch;
select hll_sketch_count_merge(hll_province) from test_hll_sketch group by dt order by 1;

-- first insert & test result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-25" from table(generate_series(1, 1000));
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
-- insert with different sketch size
insert into test_hll_sketch select dt, hll_sketch_count_state(id, 20), hll_sketch_count_state(province, 19), hll_sketch_count_state(age, 18), hll_sketch_count_state(dt, 17) from t1;
ALTER TABLE test_hll_sketch COMPACT;

-- query    
select hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch;
select dt, hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;

-- second insert & test result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
select hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch;
select dt, hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;