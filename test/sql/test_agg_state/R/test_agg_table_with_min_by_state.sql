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
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) 
PROPERTIES (  "replication_num" = "1");
-- result:
-- !result
insert into t1  values('2020-01-01', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);
-- result:
-- !result
CREATE TABLE test_agg_tbl1(
  k1 VARCHAR(10),
  k6 agg_state<min_by(tinyint, datetime)> agg_state_union,
  k7 agg_state<min_by(smallint, datetime)> agg_state_union,
  k8 agg_state<min_by(int, datetime)> agg_state_union,
  k9 agg_state<min_by(bigint, datetime)> agg_state_union,
  k10 agg_state<min_by(largeint, datetime)> agg_state_union,
  k11 agg_state<min_by(float, datetime)> agg_state_union,
  k12 agg_state<min_by(double, datetime)> agg_state_union,
  k13 agg_state<min_by(decimal(27, 9), datetime)> agg_state_union
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;
-- result:
-- !result
insert into test_agg_tbl1 select k1, min_by_state(k6, k2), min_by_state(k7, k2), min_by_state(k8, k2), min_by_state(k9, k2), min_by_state(k10, k2), min_by_state(k11, k2), min_by_state(k12, k2), min_by_state(k13, k2) from t1;
-- result:
-- !result
select min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1;
-- result:
E: (1064, 'Cannot invoke "com.starrocks.catalog.AggregateFunction.functionName()" because "aggFunc" is null')
-- !result
select k1, min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1 group by k1 order by 1 limit 3;
-- result:
E: (1064, 'Cannot invoke "com.starrocks.catalog.AggregateFunction.functionName()" because "aggFunc" is null')
-- !result
insert into t1  values('2020-01-02', '2020-01-01 10:10:10', 'aaaa', 'aaaa', 0, 1, 1, 1, 1, 1, 1.11, 11.111, 111.1111),('2020-02-01', '2020-02-01 10:10:10', 'bbbb', 'bbbb', 0, 2, 2, 2, 2, 2, 2.22, 22.222, 222.2222);
-- result:
-- !result
insert into test_agg_tbl1 select k1, min_by_state(k6, k2), min_by_state(k7, k2), min_by_state(k8, k2), min_by_state(k9, k2), min_by_state(k10, k2), min_by_state(k11, k2), min_by_state(k12, k2), min_by_state(k13, k2) from t1;
-- result:
-- !result
insert into test_agg_tbl1 select k1, min_by_state(k6, k2), min_by_state(k7, k2), min_by_state(k8, k2), min_by_state(k9, k2), min_by_state(k10, k2), min_by_state(k11, k2), min_by_state(k12, k2), min_by_state(k13, k2) from t1;
-- result:
-- !result
ALTER TABLE test_agg_tbl1 COMPACT;
-- result:
-- !result
select min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1;
-- result:
E: (1064, 'Cannot invoke "com.starrocks.catalog.AggregateFunction.functionName()" because "aggFunc" is null')
-- !result
select k1, min_by_merge(k6), min_by_merge(k7), min_by_merge(k8), min_by_merge(k9), min_by_merge(k10), min_by_merge(k11), min_by_merge(k12), min_by_merge(k13) from test_agg_tbl1 group by k1 order by 1 limit 3;
-- result:
E: (1064, 'Cannot invoke "com.starrocks.catalog.AggregateFunction.functionName()" because "aggFunc" is null')
-- !result