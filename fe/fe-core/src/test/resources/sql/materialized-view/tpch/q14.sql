[sql]
select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
        l_partkey = p_partkey
  and l_shipdate >= date '1997-02-01'
  and l_shipdate < date '1997-03-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{30: sum=sum(if(76: p_type LIKE PROMO%, 80: l_saleprice, 0.0)), 31: sum=sum(29: expr)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[62: l_shipdate, 76: p_type, 80: l_saleprice] predicate[62: l_shipdate >= 1997-02-01 AND 62: l_shipdate < 1997-03-01])
[end]

