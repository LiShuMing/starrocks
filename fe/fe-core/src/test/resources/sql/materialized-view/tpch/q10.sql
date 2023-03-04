[sql]
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1994-05-01'
  and o_orderdate < date '1994-08-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc limit 20;
[result]
TOP-N (order by [[39: sum DESC NULLS LAST]])
    TOP-N (order by [[39: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{39: sum=sum(39: sum)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 35, 3, 8]
                AGGREGATE ([LOCAL] aggregate [{39: sum=sum(38: expr)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
                    SCAN (mv[lineitem_mv] columns[104: c_address, 105: c_acctbal, 106: c_comment, 108: c_name, 109: c_nationkey, 110: c_phone, 118: l_returnflag, 123: o_custkey, 124: o_orderdate, 135: s_nationkey, 137: l_saleprice, 141: n_name1] predicate[109: c_nationkey = 135: s_nationkey AND 135: s_nationkey = 109: c_nationkey AND 124: o_orderdate >= 1994-05-01 AND 124: o_orderdate < 1994-08-01 AND 118: l_returnflag = R AND 124: o_orderdate >= 1994-01-01 AND 124: o_orderdate < 1995-01-01])
[end]

