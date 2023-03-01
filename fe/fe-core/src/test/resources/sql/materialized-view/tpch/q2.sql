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
        PREDICATE 22: PS_SUPPLYCOST = 115: min
            ANALYTIC ({115: min=min(22: PS_SUPPLYCOST)} [1: P_PARTKEY] [] )
                TOP-N (order by [[1: P_PARTKEY ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[1]
                        SCAN (mv[partsupp_mv] columns[100: n_name, 101: p_mfgr, 102: p_size, 103: p_type, 104: ps_partkey, 106: ps_supplycost, 107: r_name, 108: s_acctbal, 109: s_address, 110: s_comment, 111: s_name, 113: s_phone] predicate[102: p_size = 12 AND 107: r_name = AMERICA AND 103: p_type LIKE %COPPER])
[end]

