[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24 ;
[result]
AGGREGATE ([GLOBAL] aggregate [{18: sum=sum(18: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{18: sum=sum(multiply(cast(7: l_extendedprice as decimal128(15, 2)), cast(8: l_discount as decimal128(15, 2))))}] group by [[]] having [null]
            SCAN (table[lineitem] columns[1: l_shipdate, 6: l_quantity, 7: l_extendedprice, 8: l_discount] predicate[8: l_discount >= 0.02 AND 8: l_discount <= 0.04 AND 6: l_quantity < 24])
[end]

