[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) a
where
        s_suppkey = supplier_no
  and total_revenue = (
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
                 l_suppkey) b
)
order by
    s_suppkey;
[result]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{144: sum=sum(144: sum)}] group by [[65: l_suppkey]] having [144: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[65]
                            AGGREGATE ([LOCAL] aggregate [{144: sum=sum(76: sum_disc_price)}] group by [[65: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv] columns[65: l_suppkey, 66: l_shipdate, 76: sum_disc_price] predicate[66: l_shipdate >= 1995-07-01 AND 66: l_shipdate < 1995-10-01 AND 76: sum_disc_price IS NOT NULL])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{133: sum=sum(133: sum)}] group by [[65: l_suppkey]] having [null]
                                                EXCHANGE SHUFFLE[65]
                                                    AGGREGATE ([LOCAL] aggregate [{133: sum=sum(76: sum_disc_price)}] group by [[65: l_suppkey]] having [null]
                                                        SCAN (mv[lineitem_agg_mv] columns[65: l_suppkey, 66: l_shipdate, 76: sum_disc_price] predicate[66: l_shipdate >= 1995-07-01 AND 66: l_shipdate < 1995-10-01])
[end]

