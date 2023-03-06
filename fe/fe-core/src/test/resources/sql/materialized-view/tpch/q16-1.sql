[sql]
select
    p_brand,
    p_type,
    p_size,
    ps_suppkey as supplier_cnt
from
    partsupp,
    part
where
        p_partkey = ps_partkey
  and p_brand <> 'Brand#43'
  and p_type not like 'PROMO BURNISHED%'
  and p_size in (31, 43, 9, 6, 18, 11, 25, 1)
[result]
SCAN (mv[partsupp_mv] columns[21: p_size, 22: p_type, 24: ps_suppkey, 31: s_nationkey] predicate[cast(31: s_nationkey as varchar(1048576)) != Brand#43 AND NOT 22: p_type LIKE PROMO BURNISHED% AND 21: p_size IN (31, 43, 9, 6, 18, 11, 25, 1)])
[end]

