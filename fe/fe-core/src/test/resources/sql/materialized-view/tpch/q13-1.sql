[sql]
select
    c_custkey,
    count(o_orderkey) as c_count
from
    customer_order_mv
where
    o_comment not like '%unusual%deposits%'
group by
    c_custkey
[result]
AGGREGATE ([GLOBAL] aggregate [{9: count=sum(9: count)}] group by [[5: c_custkey]] having [null]
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([LOCAL] aggregate [{9: count=sum(7: c_count)}] group by [[5: c_custkey]] having [null]
            SCAN (mv[customer_order_mv_agg_mv1] columns[5: c_custkey, 6: o_comment, 7: c_count] predicate[NOT 6: o_comment LIKE %unusual%deposits%])
[end]

