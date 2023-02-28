[sql]
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
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
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[result]
TOP-N (order by [[77: count DESC NULLS LAST, 2: S_NAME ASC NULLS FIRST]])
    TOP-N (order by [[77: count DESC NULLS LAST, 2: S_NAME ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{77: count=count(77: count)}] group by [[2: S_NAME]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{77: count=count()}] group by [[2: S_NAME]] having [null]
                    LEFT ANTI JOIN (join-predicate [9: L_ORDERKEY = 59: L_ORDERKEY AND 61: L_SUPPKEY != 11: L_SUPPKEY] post-join-predicate [null])
                        LEFT SEMI JOIN (join-predicate [9: L_ORDERKEY = 41: L_ORDERKEY AND 43: L_SUPPKEY != 11: L_SUPPKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[9]
                                SCAN (mv[lineitem_mv] columns[140: l_orderkey, 147: l_commitdate, 148: l_receiptdate, 152: l_suppkey, 164: o_orderstatus, 172: s_name, 203: n_name1] predicate[164: o_orderstatus = F AND 203: n_name1 = CANADA AND 148: l_receiptdate > 147: l_commitdate])
                            EXCHANGE SHUFFLE[41]
                                SCAN (columns[41: L_ORDERKEY, 43: L_SUPPKEY] predicate[null])
                        EXCHANGE SHUFFLE[59]
                            SCAN (columns[70: L_COMMITDATE, 71: L_RECEIPTDATE, 59: L_ORDERKEY, 61: L_SUPPKEY] predicate[71: L_RECEIPTDATE > 70: L_COMMITDATE])
[end]

