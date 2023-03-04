[sql]
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
                s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
                (n1.n_name = 'CANADA' and n2.n_name = 'IRAN')
                or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')
            )
          and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year ;
[result]
TOP-N (order by [[42: n_name ASC NULLS FIRST, 46: n_name ASC NULLS FIRST, 49: year ASC NULLS FIRST]])
    TOP-N (order by [[42: n_name ASC NULLS FIRST, 46: n_name ASC NULLS FIRST, 49: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{51: sum=sum(51: sum)}] group by [[42: n_name, 46: n_name, 49: year]] having [null]
            EXCHANGE SHUFFLE[42, 46, 49]
                AGGREGATE ([LOCAL] aggregate [{51: sum=sum(50: expr)}] group by [[42: n_name, 46: n_name, 49: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[115: l_shipdate, 132: l_shipyear, 133: l_saleprice, 136: n_name1, 138: n_name2] predicate[115: l_shipdate <= 1996-12-31 AND 136: n_name1 = CANADA AND 138: n_name2 = IRAN OR 136: n_name1 = IRAN AND 138: n_name2 = CANADA AND 136: n_name1 IN (CANADA, IRAN) AND 138: n_name2 IN (IRAN, CANADA)])
[end]

