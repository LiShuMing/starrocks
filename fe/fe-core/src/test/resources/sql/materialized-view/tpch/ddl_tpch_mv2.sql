-- query1,query18
create materialized view lineitem_agg_mv1
distributed by hash(l_orderkey,
               l_shipdate,
               l_returnflag,
               l_linestatus) buckets 96
partition by l_shipdate
refresh deferred manual
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_orderkey,
              l_shipdate,
              l_returnflag,
              l_linestatus,
              count(1) as total_cnt,
              sum(l_quantity) as sum_qty,
              sum(l_extendedprice) as sum_base_price,
              sum(l_discount) as sum_discount,
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
   from
              lineitem
   group by
       l_orderkey,
       l_shipdate,
       l_returnflag,
       l_linestatus
;