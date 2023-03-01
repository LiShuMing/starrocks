-- query13
create materialized view customer_order_mv_agg_mv1
distributed by hash( c_custkey,
       o_comment) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,
              o_comment,
              count(o_orderkey) as c_count,
              count(1) as c_count_star
   from
              (select
                   c_custkey,o_comment,o_orderkey
               from
                   customer
                       left outer join
                   orders
                   on orders.o_custkey=customer.c_custkey) a
   group by
       c_custkey,
       o_comment
;

-- -- query9
-- create materialized view lineitem_mv_agg_mv1
-- distributed by hash(p_name,
--               o_orderyear,
--               n_name1) buckets 96
-- refresh manual
-- properties (
--     "replication_num" = "1"
-- )
-- as select /*+ SET_VAR(query_timeout = 7200) */
--               p_name,
--               o_orderyear,
--               n_name1,
--               sum(l_amount) as sum_amount
--    from
--               lineitem_mv
--    group by
--        p_name,
--        o_orderyear,
--        n_name1
-- ;