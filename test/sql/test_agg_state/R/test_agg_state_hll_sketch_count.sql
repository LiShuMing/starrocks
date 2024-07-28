-- name: test_agg_state_hll_sketch_count
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;
-- result:
-- !result
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
-- result:
-- !result
select id, province, from_binary(hll_sketch_count_state(id), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(hll_sketch_count_state(province), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(hll_sketch_count_state(age), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(hll_sketch_count_state(dt), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select hll_sketch_count_merge(hll_province) from test_hll_sketch;
-- result:
None
-- !result
select hll_sketch_count_merge(hll_province) from test_hll_sketch group by dt order by 1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
-- result:
-- !result
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
-- result:
-- !result
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
-- result:
-- !result
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
-- result:
-- !result
ALTER TABLE test_hll_sketch COMPACT;
-- result:
-- !result
select hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch;
-- result:
1000	1000	100	1
-- !result
select dt, hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;
-- result:
2024-07-24	1000	1000	100	1
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
-- !result
insert into test_hll_sketch select dt, hll_sketch_count_state(id), hll_sketch_count_state(province), hll_sketch_count_state(age), hll_sketch_count_state(dt) from t1;
-- result:
-- !result
select hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch;
-- result:
1000	1002	100	3
-- !result
select dt, hll_sketch_count_merge(hll_id), hll_sketch_count_merge(hll_province), hll_sketch_count_merge(hll_age), hll_sketch_count_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;
-- result:
2024-07-22	1	1	1	1
2024-07-24	1000	1000	100	1
2024-07-25	1	1	1	1
-- !result