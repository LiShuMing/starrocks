[sql]
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
        p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 12
  and p_type like '%COPPER'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AMERICA'
  and ps_supplycost = (
    select
        min(ps_supplycost)
    from
        partsupp,
        supplier,
        nation,
        region
    where
            p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'AMERICA'
)
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey limit 100;
[result]
TOP-N (order by [[16: S_ACCTBAL DESC NULLS LAST, 26: N_NAME ASC NULLS FIRST, 12: S_NAME ASC NULLS FIRST, 1: P_PARTKEY ASC NULLS FIRST]])
    TOP-N (order by [[16: S_ACCTBAL DESC NULLS LAST, 26: N_NAME ASC NULLS FIRST, 12: S_NAME ASC NULLS FIRST, 1: P_PARTKEY ASC NULLS FIRST]])
        PREDICATE 22: PS_SUPPLYCOST = 164: min
            ANALYTIC ({164: min=min(22: PS_SUPPLYCOST)} [1: P_PARTKEY] [] )
                TOP-N (order by [[1: P_PARTKEY ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[1]
                        SCAN (columns[59: ps_partkey, 63: ps_supplycost, 65: s_name, 66: s_address, 68: s_phone, 69: s_acctbal, 70: s_comment, 74: p_mfgr, 76: p_type, 77: p_size, 82: n_name, 86: r_name] predicate[77: p_size = 12 AND 86: r_name = AMERICA AND 76: p_type LIKE %COPPER])
[end]

