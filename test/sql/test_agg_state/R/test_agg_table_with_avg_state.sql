-- name: test_agg_table_with_avg_state
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
  k2 agg_state<avg(datetime)> agg_state_union,
  k6 agg_state<avg(tinyint)> agg_state_union,
  k7 agg_state<avg(smallint)> agg_state_union,
  k8 agg_state<avg(int)> agg_state_union,
  k9 agg_state<avg(bigint)> agg_state_union,
  k10 agg_state<avg(largeint)> agg_state_union,
  k11 agg_state<avg(float)> agg_state_union,
  k12 agg_state<avg(double)> agg_state_union,
  k13 agg_state<avg(decimal)> agg_state_union
)
AGGREGATE KEY(k1)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;
-- result:
E: (1064, 'Unexpected exception: Cannot invoke "com.starrocks.catalog.Type.withAggStateDesc(com.starrocks.catalog.combinator.AggStateDesc)" because the return value of "com.starrocks.catalog.AggregateFunction.getIntermediateType()" is null')
-- !result
insert into test_agg_tbl1 select k1, avg_state(k2), avg_state(k6), avg_state(k7), avg_state(k8), avg_state(k9), avg_state(k10), avg_state(k11), avg_state(k12), avg_state(k13) from t1;
-- result:
E: (1064, 'Getting analyzing error. Detail message: Table test_agg_tbl1 is not found.')
-- !result
select avg_merge(k2), avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1;
-- result:
E: (5502, "Getting analyzing error. Detail message: Unknown table 'test_db_f72b06ae4ce511ef8c28f59689b429b3.test_agg_tbl1'.")
-- !result
select k1, avg_merge(k2), avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1 group by dt order by 1 limit 3;
-- result:
E: (5502, "Getting analyzing error. Detail message: Unknown table 'test_db_f72b06ae4ce511ef8c28f59689b429b3.test_agg_tbl1'.")
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
E: (5604, "Getting analyzing error. Detail message: Inserted target column count: 13 doesn't match select/value column count: 4.")
-- !result
insert into test_agg_tbl1 select k1, avg_state(k2), avg_state(k6), avg_state(k7), avg_state(k8), avg_state(k9), avg_state(k10), avg_state(k11), avg_state(k12), avg_state(k13) from t1;
-- result:
E: (1064, 'Getting analyzing error. Detail message: Table test_agg_tbl1 is not found.')
-- !result
insert into test_agg_tbl1 select k1, avg_state(k2), avg_state(k6), avg_state(k7), avg_state(k8), avg_state(k9), avg_state(k10), avg_state(k11), avg_state(k12), avg_state(k13) from t1;
-- result:
E: (1064, 'Getting analyzing error. Detail message: Table test_agg_tbl1 is not found.')
-- !result
ALTER TABLE test_agg_tbl1 COMPACT;
-- result:
E: (1064, 'Getting analyzing error. Detail message: Table test_agg_tbl1 is not found.')
-- !result
select avg_merge(k2), avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1;
-- result:
E: (5502, "Getting analyzing error. Detail message: Unknown table 'test_db_f72b06ae4ce511ef8c28f59689b429b3.test_agg_tbl1'.")
-- !result
select k1, avg_merge(k2), avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11), avg_merge(k12), avg_merge(k13) from test_agg_tbl1 group by dt order by 1 limit 3;
-- result:
E: (5502, "Getting analyzing error. Detail message: Unknown table 'test_db_f72b06ae4ce511ef8c28f59689b429b3.test_agg_tbl1'.")
-- !result