-- query13
create materialized view query21_mv
distributed by hash(s_name,
              o_orderstatus,
              n_name) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              s_name,
              o_orderstatus,
              n_name,
              count(*) as cnt_star
   from
       supplier,
       lineitem l1,
       orders,
       nation
   where
                  s_suppkey = l1.l_suppkey
     and o_orderkey = l1.l_orderkey
     and l1.l_receiptdate > l1.l_commitdate
     and exists (
           select
               *
           from
               lineitem l2
           where
                   l2.l_orderkey = l1.l_orderkey
             and l2.l_suppkey <> l1.l_suppkey
       )
     and not exists (
           select
               *
           from
               lineitem l3
           where
                   l3.l_orderkey = l1.l_orderkey
             and l3.l_suppkey <> l1.l_suppkey
             and l3.l_receiptdate > l3.l_commitdate
       )
     and s_nationkey = n_nationkey
group by s_name,
         o_orderstatus,
         n_name;
