[sql]
select
    s_name,
    o_orderstatus,
    n_name,
    s_name
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
[result]
TOP-N (order by [[71: count DESC NULLS LAST, 2: s_name ASC NULLS FIRST]])
    TOP-N (order by [[71: count DESC NULLS LAST, 2: s_name ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{71: count=count(71: count)}] group by [[2: s_name]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{71: count=count()}] group by [[2: s_name]] having [null]
                    LEFT ANTI JOIN (join-predicate [9: l_orderkey = 55: l_orderkey AND 58: l_suppkey != 12: l_suppkey] post-join-predicate [null])
                        LEFT SEMI JOIN (join-predicate [9: l_orderkey = 38: l_orderkey AND 41: l_suppkey != 12: l_suppkey] post-join-predicate [null])
                            EXCHANGE SHUFFLE[9]
                                SCAN (mv[lineitem_mv] columns[121: l_commitdate, 124: l_orderkey, 127: l_receiptdate, 132: l_suppkey, 136: o_orderstatus, 144: s_name, 150: n_name1] predicate[136: o_orderstatus = F AND 150: n_name1 = CANADA AND 127: l_receiptdate > 121: l_commitdate])
                            EXCHANGE SHUFFLE[38]
                                SCAN (table[lineitem] columns[38: l_orderkey, 41: l_suppkey] predicate[null])
                        EXCHANGE SHUFFLE[55]
                            SCAN (table[lineitem] columns[65: l_commitdate, 66: l_receiptdate, 55: l_orderkey, 58: l_suppkey] predicate[66: l_receiptdate > 65: l_commitdate])
[end]

