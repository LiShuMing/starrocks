[sql]
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= date '1994-09-01'
  and o_orderdate < date '1994-12-01'
  and exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_receiptdate > l_commitdate
    )
group by
    o_orderpriority
order by
    o_orderpriority ;
[result]
TOP-N (order by [[6: o_orderpriority ASC NULLS FIRST]])
    TOP-N (order by [[6: o_orderpriority ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{27: count=count(27: count)}] group by [[6: o_orderpriority]] having [null]
            EXCHANGE SHUFFLE[6]
                AGGREGATE ([LOCAL] aggregate [{27: count=count()}] group by [[6: o_orderpriority]] having [null]
                    LEFT SEMI JOIN (join-predicate [1: o_orderkey = 11: l_orderkey] post-join-predicate [null])
                        SCAN (table[orders] columns[1: o_orderkey, 2: o_orderdate, 6: o_orderpriority] predicate[2: o_orderdate >= 1994-09-01 AND 2: o_orderdate < 1994-12-01])
                        EXCHANGE SHUFFLE[11]
                            SCAN (table[lineitem] columns[21: l_commitdate, 22: l_receiptdate, 11: l_orderkey] predicate[22: l_receiptdate > 21: l_commitdate])
[end]

