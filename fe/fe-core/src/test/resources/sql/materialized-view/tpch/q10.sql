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
                    SCAN (mv[lineitem_mv] columns[62: c_address, 63: c_acctbal, 64: c_comment, 66: c_name, 67: c_nationkey, 68: c_phone, 75: l_returnflag, 80: o_custkey, 81: o_orderdate, 92: s_nationkey, 94: l_saleprice, 97: n_name1] predicate[92: s_nationkey = 67: c_nationkey AND 67: c_nationkey = 92: s_nationkey AND 81: o_orderdate >= 1994-05-01 AND 81: o_orderdate < 1994-08-01 AND 75: l_returnflag = R])
[end]

