[sql]
select
        0.5 * sum(l_quantity)
from
    lineitem
where
  l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-01-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{106: sum=sum(106: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{106: sum=sum(22: sum_qty)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv3] columns[20: l_shipdate, 22: sum_qty] predicate[20: l_shipdate >= 1993-01-01 AND 20: l_shipdate < 1994-01-01])
[end]

