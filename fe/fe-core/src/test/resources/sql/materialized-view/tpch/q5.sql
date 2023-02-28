[sql]
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AFRICA'
  and o_orderdate >= date '1995-01-01'
  and o_orderdate < date '1996-01-01'
group by
    n_name
order by
    revenue desc ;
[result]
TOP-N (order by [[55: sum DESC NULLS LAST]])
    TOP-N (order by [[55: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum=sum(55: sum)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    SCAN (mv[lineitem_mv] columns[143: l_saleprice, 149: o_orderdate, 158: s_nationkey, 181: c_nationkey, 187: n_name1, 195: r_name1] predicate[158: s_nationkey = 181: c_nationkey AND 181: c_nationkey = 158: s_nationkey AND 149: o_orderdate >= 1995-01-01 AND 149: o_orderdate < 1996-01-01 AND 195: r_name1 = AFRICA])
[end]

