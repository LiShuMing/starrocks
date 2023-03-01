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
TOP-N (order by [[43: sum DESC NULLS LAST]])
    TOP-N (order by [[43: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum=sum(43: sum)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                AGGREGATE ([LOCAL] aggregate [{43: sum=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
                    SCAN (mv[lineitem_mv] columns[83: c_address, 84: c_acctbal, 85: c_comment, 87: c_name, 88: c_nationkey, 89: c_phone, 96: l_returnflag, 101: o_custkey, 102: o_orderdate, 113: s_nationkey, 117: l_saleprice, 123: n_name1] predicate[113: s_nationkey = 88: c_nationkey AND 88: c_nationkey = 113: s_nationkey AND 102: o_orderdate >= 1994-05-01 AND 102: o_orderdate < 1994-08-01 AND 96: l_returnflag = R])
[end]

