create materialized view lineitem_agg_mv
distributed by hash(l_orderkey) buckets 96
partition by l_shipdate
refresh manual
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_orderkey,
              l_suppkey,
              l_shipdate,
              l_returnflag,
              l_linestatus,
              l_partkey,
              sum(l_quantity) as sum_qty,
              count(l_quantity) as count_qty,
              sum(l_extendedprice) as sum_base_price,
              count(l_extendedprice) as count_base_price,
              sum(l_discount) as sum_discount,
              count(l_discount) as count_discount,
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
--               avg(l_quantity) as avg_qty,
--               avg(l_extendedprice) as avg_price,
--               avg(l_discount) as avg_disc,
              count(*) as count_order
   from
              lineitem
   group by
       l_orderkey, l_suppkey, l_shipdate, l_partkey,
       l_returnflag, l_linestatus
;

-- customer_order_mv (used to match query22)
-- query22 needs avg & rollup -> sum/count
create materialized view customer_mv
distributed by hash(c_custkey) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,
              c_phone,
              c_acctbal,
              substring(c_phone, 1  ,2) as substring_phone,
              count(c_acctbal) as c_count,
              sum(c_acctbal) as c_sum
   from
              customer
   group by c_custkey, c_phone, c_acctbal, substring(c_phone, 1  ,2);
