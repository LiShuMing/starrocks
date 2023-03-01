-- partsupp_mv
create materialized view partsupp_mv
distributed by hash(ps_partkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              n_name,
              p_mfgr,p_size,p_type,
              ps_partkey, ps_suppkey,ps_supplycost,
              r_name,
              s_acctbal,s_address,s_comment,s_name,s_nationkey,s_phone,
              ps_supplycost * ps_availqty as ps_partvalue
   from
              partsupp
                  inner join supplier
                  inner join part
                  inner join nation
                  inner join region
   where
                  partsupp.ps_suppkey = supplier.s_suppkey
     and partsupp.ps_partkey=part.p_partkey
     and supplier.s_nationkey=nation.n_nationkey
     and nation.n_regionkey=region.r_regionkey;

-- lineitem_mv
create materialized view lineitem_mv
distributed by hash(o_orderdate, l_orderkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_address, c_acctbal,c_comment,c_mktsegment,c_name,c_nationkey,c_phone,
              l_commitdate,l_extendedprice,l_orderkey,l_partkey,l_quantity,l_receiptdate,l_returnflag,l_shipdate,l_shipinstruct,l_shipmode,l_suppkey,
              o_custkey,o_orderdate,o_orderpriority,o_orderstatus,o_shippriority,o_totalprice,
              p_brand,p_container,p_name,p_size,p_type,
              s_name,s_nationkey,
              extract(year from l_shipdate) as l_shipyear,
              l_extendedprice * (1 - l_discount) as l_saleprice,
              ps_supplycost * l_quantity as l_supplycost,
              extract(year from o_orderdate) as o_orderyear,
              s_nation.n_name as n_name1,
              s_nation.n_regionkey as n_regionkey1,
              c_nation.n_name as n_name2,
              c_nation.n_regionkey as n_regionkey2,
              s_region.r_name as r_name1,
              c_region.r_name as r_name2
   from
              lineitem
                  inner join partsupp
                  inner join orders
                  inner join supplier
                  inner join part
                  inner join customer
                  inner join nation as s_nation
                  inner join nation as c_nation
                  inner join region as s_region
                  inner join region as c_region
   where
                  lineitem.l_partkey=partsupp.ps_partkey
     and lineitem.l_suppkey=partsupp.ps_suppkey
     and lineitem.l_orderkey=orders.o_orderkey
     and partsupp.ps_partkey=part.p_partkey
     and partsupp.ps_suppkey=supplier.s_suppkey
     and customer.c_custkey=orders.o_custkey
     and supplier.s_nationkey=s_nation.n_nationkey
     and customer.c_nationkey=c_nation.n_nationkey
     and s_region.r_regionkey=s_nation.n_regionkey
     and c_region.r_regionkey=c_nation.n_regionkey
;

-- customer_order_mv (used to match query13)
create materialized view customer_order_mv
distributed by hash(c_custkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,o_comment,o_orderkey
   from
              customer
                  left outer join
              orders
              on orders.o_custkey=customer.c_custkey;

-- query15/query18/q20
create materialized view lineitem_agg_mv
distributed by hash(l_orderkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
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
distributed by hash(c_custkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,
              c_phone,
              c_acctbal,
              -- TODO: can be deduced from c_phone
              substring(c_phone, 1  ,2) as substring_phone,
              count(c_acctbal) as c_count,
              sum(c_acctbal) as c_sum
   from
              customer
   group by c_custkey, c_phone, c_acctbal, substring(c_phone, 1  ,2);