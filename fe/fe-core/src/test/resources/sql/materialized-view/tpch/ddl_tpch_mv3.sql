-- query13
create materialized view customer_order_mv1
distributed by hash( c_custkey,
       o_comment) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,
              o_comment,
              count(o_orderkey) as c_count
   from
              customer_order_mv
   group by
       c_custkey,
       o_comment
;

-- query1
create materialized view lineitem_agg_mv1
distributed by hash(l_shipdate, l_returnflag, l_linestatus) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              l_shipdate,
              l_returnflag,
              l_linestatus,
              sum(sum_qty) as sum_qty1,
              sum(count_qty) as count_qty1,
              sum(sum_base_price) as sum_base_price1,
              sum(count_base_price) as count_base_price1,
              sum(sum_disc_price) as sum_disc_price1,
              sum(sum_charge) as sum_charge1,
              sum(count_discount) as count_discount1,
              sum(sum_discount) as sum_discount1,
              sum(sum_qty) / sum(count_qty) as avg_qty,
              sum(sum_base_price) / sum(count_base_price) as avg_price,
              sum(sum_discount) / sum(count_discount) as avg_discount,
--               avg(l_quantity) as avg_qty,
--               avg(l_extendedprice) as avg_price,
--               avg(l_discount) as avg_disc,
              sum(count_order)
   from
              lineitem_agg_mv
   group by
       l_shipdate,
       l_returnflag,
       l_linestatus
;

-- query15
create materialized view lineitem_agg_mv2
distributed by hash(l_suppkey, l_shipdate) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              l_shipdate,
              l_suppkey,
              sum(sum_disc_price) as sum_disc_price
   from
              lineitem_agg_mv
   group by
       l_suppkey,
       l_shipdate
;

-- query9
create materialized view lineitem_mv1
distributed by hash(p_name,
              o_orderyear,
              n_name1) buckets 96
refresh manual
properties (
    "replication_num" = "1"
)
as select /*+ SET_VAR(query_timeout = 7200) */
              p_name,
              o_orderyear,
              n_name1,
              sum(l_amount) as sum_amount
   from
              lineitem_mv
   group by
       p_name,
       o_orderyear,
       n_name1
;

-- query10
create materialized view lineitem_mv2
distributed by hash(o_custkey,
    c_name,
    c_acctbal,
    c_phone,
    c_address,
    c_comment,
    n_name1) buckets 96
refresh manual
properties (
    "replication_num" = "1"
)
as select /*+ SET_VAR(query_timeout = 7200) */
              o_orderdate,
              l_returnflag,
              o_custkey,
              c_name,
              c_acctbal,
              c_phone,
              c_address,
              c_comment,
              n_name1,
              sum(l_saleprice) as sum_amount
   from
              lineitem_mv
-- where
--   o_orderdate >= date '1994-05-01'
--   and o_orderdate < date '1994-08-01'
--   and l_returnflag = 'R'
   group by
       o_orderdate,
       l_returnflag,
       o_custkey,
       c_name,
       c_acctbal,
       c_phone,
       c_address,
       c_comment,
       n_name1
;
