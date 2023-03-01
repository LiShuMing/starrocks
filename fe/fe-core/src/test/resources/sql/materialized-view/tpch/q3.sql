[sql]
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
  c_mktsegment = 'HOUSEHOLD'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-11'
  and l_shipdate > date '1995-03-11'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate limit 10;
[result]
TOP-N (order by [[38: sum DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum=sum(38: sum)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    SCAN (mv[lineitem_mv] columns[60: c_mktsegment, 66: l_orderkey, 71: l_shipdate, 76: o_orderdate, 79: o_shippriority, 89: l_saleprice] predicate[60: c_mktsegment = HOUSEHOLD AND 76: o_orderdate < 1995-03-11 AND 71: l_shipdate > 1995-03-11])
[end]

