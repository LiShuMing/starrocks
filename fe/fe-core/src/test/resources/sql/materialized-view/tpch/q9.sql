[sql]
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
                s_suppkey = l_suppkey
          and ps_suppkey = l_suppkey
          and ps_partkey = l_partkey
          and p_partkey = l_partkey
          and o_orderkey = l_orderkey
          and s_nationkey = n_nationkey
          and p_name like '%peru%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc ;
[result]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[83: c_nationkey, 104: p_name, 108: s_nationkey, 110: l_saleprice, 111: l_supplycost, 112: o_orderyear, 113: n_name1] predicate[83: c_nationkey = 108: s_nationkey AND 104: p_name LIKE %peru%])
[end]

