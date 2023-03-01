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
                    SCAN (mv[lineitem_mv] columns[85: c_name, 90: l_orderkey, 92: l_quantity, 99: o_custkey, 100: o_orderdate, 104: o_totalprice] predicate[null])
                EXCHANGE SHUFFLE[37]
                    AGGREGATE ([GLOBAL] aggregate [{134: sum=sum(69: sum_qty)}] group by [[63: l_orderkey]] having [134: sum > 315.0]
                        SCAN (mv[lineitem_agg_mv] columns[63: l_orderkey, 69: sum_qty] predicate[69: sum_qty > 315.0])
[end]

