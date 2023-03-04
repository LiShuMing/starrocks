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
TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
    TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{53: sum=sum(53: sum)}] group by [[48: n_name, 51: year]] having [null]
            EXCHANGE SHUFFLE[48, 51]
                AGGREGATE ([LOCAL] aggregate [{53: sum=sum(52: expr)}] group by [[48: n_name, 51: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[135: p_name, 141: l_saleprice, 142: l_supplycost, 143: o_orderyear, 144: n_name1] predicate[135: p_name LIKE %peru%])
[end]

