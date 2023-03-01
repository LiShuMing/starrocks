[sql]
select
    s_name,
    s_address
from
    supplier,
    nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                        p_name like 'sienna%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                lineitem
            where
                    l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= date '1993-01-01'
              and l_shipdate < date '1994-01-01'
        )
    )
  and s_nationkey = n_nationkey
  and n_name = 'ARGENTINA'
order by
    s_name ;
[result]
TOP-N (order by [[2: S_NAME ASC NULLS FIRST]])
    TOP-N (order by [[2: S_NAME ASC NULLS FIRST]])
        INNER JOIN (join-predicate [4: S_NATIONKEY = 9: N_NATIONKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[4]
                LEFT SEMI JOIN (join-predicate [1: S_SUPPKEY = 15: PS_SUPPKEY] post-join-predicate [null])
                    SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 4: S_NATIONKEY] predicate[null])
                    EXCHANGE SHUFFLE[15]
                        INNER JOIN (join-predicate [14: PS_PARTKEY = 32: L_PARTKEY AND 15: PS_SUPPKEY = 33: L_SUPPKEY AND cast(16: PS_AVAILQTY as double) > multiply(0.5, 48: sum)] post-join-predicate [null])
                            LEFT SEMI JOIN (join-predicate [14: PS_PARTKEY = 20: P_PARTKEY] post-join-predicate [null])
                                SCAN (columns[14: PS_PARTKEY, 15: PS_SUPPKEY, 16: PS_AVAILQTY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: P_PARTKEY, 21: P_NAME] predicate[21: P_NAME LIKE sienna%])
                            EXCHANGE SHUFFLE[32]
                                AGGREGATE ([GLOBAL] aggregate [{135: sum=sum(135: sum)}] group by [[68: l_suppkey, 72: l_partkey]] having [null]
                                    EXCHANGE SHUFFLE[68, 72]
                                        AGGREGATE ([LOCAL] aggregate [{135: sum=sum(73: sum_qty)}] group by [[68: l_suppkey, 72: l_partkey]] having [null]
                                            SCAN (mv[lineitem_agg_mv] columns[68: l_suppkey, 69: l_shipdate, 72: l_partkey, 73: sum_qty] predicate[69: l_shipdate >= 1993-01-01 AND 69: l_shipdate < 1994-01-01])
            EXCHANGE SHUFFLE[9]
                SCAN (columns[9: N_NATIONKEY, 10: N_NAME] predicate[10: N_NAME = ARGENTINA])
[end]

