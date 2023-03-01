[sql]
select
    o_year,
    sum(case
            when nation = 'IRAN' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-01-01' and date '1996-12-31'
          and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year ;
[result]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[112: o_orderdate, 121: p_type, 125: l_saleprice, 127: o_orderyear, 128: n_name1, 129: n_regionkey1, 131: n_regionkey2, 132: r_name1] predicate[131: n_regionkey2 = 129: n_regionkey1 AND 129: n_regionkey1 = 131: n_regionkey2 AND 112: o_orderdate >= 1995-01-01 AND 112: o_orderdate <= 1996-12-31 AND 121: p_type = ECONOMY ANODIZED STEEL AND 132: r_name1 = MIDDLE EAST])
[end]

