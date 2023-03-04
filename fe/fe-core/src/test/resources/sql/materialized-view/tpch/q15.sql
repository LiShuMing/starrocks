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
TOP-N (order by [[1: s_suppkey ASC NULLS FIRST]])
    TOP-N (order by [[1: s_suppkey ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: s_suppkey = 12: l_suppkey] post-join-predicate [null])
            SCAN (table[supplier] columns[1: s_suppkey, 2: s_name, 3: s_address, 5: s_phone] predicate[null])
            EXCHANGE SHUFFLE[12]
                INNER JOIN (join-predicate [25: sum = 44: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{143: sum=sum(143: sum)}] group by [[47: l_suppkey]] having [143: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[47]
                            AGGREGATE ([LOCAL] aggregate [{143: sum=sum(58: sum_disc_price)}] group by [[47: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv] columns[47: l_suppkey, 48: l_shipdate, 58: sum_disc_price] predicate[48: l_shipdate >= 1995-07-01 AND 48: l_shipdate < 1995-10-01 AND 58: sum_disc_price IS NOT NULL])
                    EXCHANGE BROADCAST
                        PREDICATE 44: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{44: max=max(44: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{44: max=max(43: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{132: sum=sum(132: sum)}] group by [[47: l_suppkey]] having [null]
                                                EXCHANGE SHUFFLE[47]
                                                    AGGREGATE ([LOCAL] aggregate [{132: sum=sum(58: sum_disc_price)}] group by [[47: l_suppkey]] having [null]
                                                        SCAN (mv[lineitem_agg_mv] columns[47: l_suppkey, 48: l_shipdate, 58: sum_disc_price] predicate[48: l_shipdate >= 1995-07-01 AND 48: l_shipdate < 1995-10-01])
[end]

