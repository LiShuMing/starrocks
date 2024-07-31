-- name: test_ds_hll_count_distinct
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;

-- init with 10w values
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100000));

-- without params
select hll_sketch_count(id), hll_sketch_count(province), hll_sketch_count(age), hll_sketch_count(dt) from t1 order by 1, 2;

-- with 1 param
select hll_sketch_count(id, 4), hll_sketch_count(province, 4), hll_sketch_count(age, 4), hll_sketch_count(dt, 4) from t1 order by 1, 2;
select hll_sketch_count(id, 10), hll_sketch_count(province, 10), hll_sketch_count(age, 10), hll_sketch_count(dt, 10) from t1 order by 1, 2;
select hll_sketch_count(id, 21), hll_sketch_count(province, 21), hll_sketch_count(age, 21), hll_sketch_count(dt, 21) from t1 order by 1, 2;

-- with 2 params
select hll_sketch_count(id, 10, "HLL_4"), hll_sketch_count(province, 10, "HLL_4"), hll_sketch_count(age, 10, "HLL_4"), hll_sketch_count(dt, 10, "HLL_4") from t1 order by 1, 2;
select hll_sketch_count(id, 10, "HLL_6"), hll_sketch_count(province, 10, "HLL_6"), hll_sketch_count(age, 10, "HLL_6"), hll_sketch_count(dt, 10, "HLL_6") from t1 order by 1, 2;
select hll_sketch_count(id, 10, "HLL_8"), hll_sketch_count(province, 10, "HLL_8"), hll_sketch_count(age, 10, "HLL_8"), hll_sketch_count(dt, 10, "HLL_8") from t1 order by 1, 2;

INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (2, 'b', 1, '2024-07-23'), (3, NULL, NULL, '2024-07-24');
select id, province, hll_sketch_count(id), hll_sketch_count(province), hll_sketch_count(age), hll_sketch_count(dt) from t1 group by 1, 2 order by 1, 2 limit 3;
select id, province, hll_sketch_count(id, 10), hll_sketch_count(province, 10), hll_sketch_count(age, 10), hll_sketch_count(dt, 10) from t1 group by 1, 2 order by 1, 2 limit 3;
select id, province, hll_sketch_count(id, 10, "HLL_4"), hll_sketch_count(province, 10, "HLL_4"), hll_sketch_count(age, 10, "HLL_4"), hll_sketch_count(dt, 10, "HLL_4") from t1 group by 1, 2 order by 1, 2 limit 3;

-- bad cases
select hll_sketch_count(id, 1)  from t1 order by 1, 2;
select hll_sketch_count(id, 100)  from t1 order by 1, 2;
select hll_sketch_count(id, 10, "INVALID") from t1 order by 1, 2;