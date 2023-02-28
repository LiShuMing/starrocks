[sql]
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 315
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate limit 100;
[result]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                EXCHANGE SHUFFLE[10]
                    SCAN (mv[lineitem_mv] columns[96: l_orderkey, 111: l_quantity, 119: o_custkey, 121: o_orderdate, 126: o_totalprice, 151: c_name] predicate[null])
                EXCHANGE SHUFFLE[37]
                    AGGREGATE ([GLOBAL] aggregate [{172: sum=sum(63: sum_qty)}] group by [[57: l_orderkey]] having [172: sum > 315.0]
                        SCAN (mv[lineitem_agg_mv] columns[57: l_orderkey, 63: sum_qty] predicate[63: sum_qty > 315.0])
[end]

