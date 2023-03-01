[sql]
select
    max(total_revenue)
from
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) b;
[result]
AGGREGATE ([GLOBAL] aggregate [{19: max=max(19: max)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{19: max=max(18: sum)}] group by [[]] having [null]
            AGGREGATE ([GLOBAL] aggregate [{107: sum=sum(107: sum)}] group by [[20: l_suppkey]] having [null]
                EXCHANGE SHUFFLE[20]
                    AGGREGATE ([LOCAL] aggregate [{107: sum=sum(29: sum_disc_price)}] group by [[20: l_suppkey]] having [null]
                        SCAN (mv[lineitem_agg_mv3] columns[20: l_suppkey, 21: l_shipdate, 29: sum_disc_price] predicate[21: l_shipdate >= 1995-07-01 AND 21: l_shipdate < 1995-10-01 AND 21: l_shipdate >= 1995-01-01 AND 21: l_shipdate < 1996-01-01])
[end]

