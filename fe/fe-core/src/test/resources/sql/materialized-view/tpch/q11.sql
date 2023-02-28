[sql]
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
    partsupp,
    supplier,
    nation
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'PERU'
    )
order by
    value desc ;
[result]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(21: sum)}] group by [[1: PS_PARTKEY]] having [null]
                EXCHANGE SHUFFLE[1]
                    AGGREGATE ([LOCAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                        SCAN (columns[45: ps_partkey, 50: ps_partvalue, 68: n_name] predicate[68: n_name = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(42: sum)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                                SCAN (columns[50: ps_partvalue, 68: n_name] predicate[68: n_name = PERU])
[end]

