-- name: test_agg_table_with_sum_state
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
  k2 datetime,
  k6 agg_state<sum(tinyint)> agg_state_union,
  k7 agg_state<sum(smallint)> agg_state_union,
  k8 agg_state<sum(int)> agg_state_union,
  k9 agg_state<sum(bigint)> agg_state_union,
  k10 agg_state<sum(largeint)> agg_state_union,
  k11 agg_state<sum(float)> agg_state_union,
  k12 agg_state<sum(double)> agg_state_union,
  k13 agg_state<sum(decimal(27,9))> agg_state_union
)
AGGREGATE KEY(k1, k2)
PARTITION BY (k1) 
DISTRIBUTED BY HASH(k1) BUCKETS 3;

-- first insert & test result
insert into test_agg_tbl1 select k1, k2, sum_state(k6), sum_state(k7), sum_state(k8), sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;

-- query    
select sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1;
select k1, sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1 group by k1 order by 1 limit 3;

-- second insert & test result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
insert into test_agg_tbl1 select k1, k2, sum_state(k6), sum_state(k7), sum_state(k8), sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;
insert into test_agg_tbl1 select k1, k2, sum_state(k6), sum_state(k7), sum_state(k8), sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;
ALTER TABLE test_agg_tbl1 COMPACT;
select sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1;
select k1, sum_merge(k6), sum_merge(k7), sum_merge(k8), sum_merge(k9), sum_merge(k10), sum_merge(k11), sum_merge(k12), sum_merge(k13) from test_agg_tbl1 group by k1 order by 1 limit 3;