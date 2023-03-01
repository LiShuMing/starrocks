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
            0.2 * avg(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{48: sum=sum(48: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{48: sum=sum(6: L_EXTENDEDPRICE)}] group by [[]] having [null]
            PREDICATE 5: L_QUANTITY < multiply(0.2, 133: avg)
                ANALYTIC ({133: avg=avg(5: L_QUANTITY)} [18: P_PARTKEY] [] )
                    TOP-N (order by [[18: P_PARTKEY ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[18]
                            SCAN (mv[lineitem_mv] columns[88: l_extendedprice, 90: l_partkey, 91: l_quantity, 104: p_brand, 105: p_container] predicate[104: p_brand = Brand#35 AND 105: p_container = JUMBO CASE])
[end]

