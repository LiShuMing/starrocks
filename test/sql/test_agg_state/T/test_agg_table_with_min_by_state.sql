-- name: test_agg_table_with_min_by_state
 CREATE TABLE `t1` ( 
    `k1`  date, 
    `k2`  datetime not null,
    `k3`  char(20), 
    `k4`  varchar(20) not null, 
    `k5`  boolean, 
    `k6`  tinyint not null, 
    `k7`  smallint, 
    `k8`  int not null, 
    `k9`  bigint, 
    `k10` largeint not null, 
    `k11` float, 
    `k12` double not null, 
    `k13` decimal(27,9)
) DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) 
-- PARTITION BY date_trunc('day', %s) 
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) 
PROPERTIES (  "replication_num" = "1");
insert into t1  values('2020-01-01', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);

CREATE TABLE test_agg_tbl1(
  k1 VARCHAR(10),
  k2 agg_state<min_by(datetime, date)> agg_state_union,
  k6 agg_state<min_by(tinyint, date)> agg_state_union,
  k7 agg_state<min_by(smallint, date)> agg_state_union,
  k8 agg_state<min_by(int, date)> agg_state_union,
  k9 agg_state<min_by(bigint, date)> agg_state_union,
  k10 agg_state<min_by(largeint, date)> agg_state_union,
  k11 agg_state<min_by(float, date)> agg_state_union,
  k12 agg_state<min_by(double, date)> agg_state_union,
  k13 agg_state<min_by(decimal(27, 9), date)> agg_state_union
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;

-- first insert & test result
insert into test_agg_tbl1 select k1, min_by_state(k2, k1), min_by_state(k3, k1), min_by_state(k4, k1), min_by_state(k5, k1), min_by_state(k6, k1), min_by_state(k7, k1), min_by_state(k8, k1), min_by_state(k9, k1), min_by_state(k10, k1), min_by_state(k11, k1), min_by_state(k12, k1), min_by_state(k13, k1) from t1;
-- query
select min_by_merge(k2), min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1;
select k1, min_by_merge(k2), min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1 group by 1 order by 1 limit 3;

-- second insert & test result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
insert into test_agg_tbl1 select k1, min_by_state(k2, k1), min_by_state(k3, k1), min_by_state(k4, k1), min_by_state(k5, k1), min_by_state(k6, k1), min_by_state(k7, k1), min_by_state(k8, k1), min_by_state(k9, k1), min_by_state(k10, k1), min_by_state(k11, k1), min_by_state(k12, k1), min_by_state(k13, k1) from t1;
insert into test_agg_tbl1 select k1, min_by_state(k2, k1), min_by_state(k3, k1), min_by_state(k4, k1), min_by_state(k5, k1), min_by_state(k6, k1), min_by_state(k7, k1), min_by_state(k8, k1), min_by_state(k9, k1), min_by_state(k10, k1), min_by_state(k11, k1), min_by_state(k12, k1), min_by_state(k13, k1) from t1;
-- COMPACT by hand
ALTER TABLE test_agg_tbl1 COMPACT;
-- query
select min_by_merge(k2), min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1;
select k1, min_by_merge(k2), min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1 group by 1 order by 1 limit 3;