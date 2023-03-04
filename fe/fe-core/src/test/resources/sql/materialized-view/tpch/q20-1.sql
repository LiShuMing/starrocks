[sql]
select
        0.5 * sum(l_quantity)
from
    lineitem
where
  l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-01-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{88: sum=sum(88: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{88: sum=sum(25: sum_qty)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv] columns[21: l_shipdate, 25: sum_qty] predicate[null])
[end]

