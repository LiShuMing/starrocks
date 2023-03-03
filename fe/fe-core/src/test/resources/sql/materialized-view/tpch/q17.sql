[sql]
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#35'
  and p_container = 'JUMBO CASE'
  and l_quantity < (
    select
--             0.2 * avg(l_quantity)
        0.2 * sum(l_quantity) / count(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{48: sum=sum(48: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{48: sum=sum(6: L_EXTENDEDPRICE)}] group by [[]] having [null]
            PREDICATE 5: L_QUANTITY < multiply(0.2, 121: avg)
                ANALYTIC ({121: avg=avg(5: L_QUANTITY)} [18: P_PARTKEY] [] )
                    TOP-N (order by [[18: P_PARTKEY ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[18]
                            SCAN (mv[lineitem_mv] columns[73: l_extendedprice, 75: l_partkey, 76: l_quantity, 89: p_brand, 90: p_container] predicate[89: p_brand = Brand#35 AND 90: p_container = JUMBO CASE])
[end]

