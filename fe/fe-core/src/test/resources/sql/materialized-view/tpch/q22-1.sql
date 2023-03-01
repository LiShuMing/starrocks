[sql]
select
    sum(c_acctbal) / count(c_acctbal)
from
    customer
where
        c_acctbal > 0.00
  and substring(c_phone , 1  ,2)  in
      ('21', '28', '24', '32', '35', '34', '37');
[result]
AGGREGATE ([GLOBAL] aggregate [{63: sum=sum(63: sum), 64: count=sum(64: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{63: sum=sum(62: c_sum), 64: count=sum(61: c_count)}] group by [[]] having [null]
            SCAN (mv[customer_mv] columns[59: c_acctbal, 60: substring_phone, 61: c_count, 62: c_sum] predicate[59: c_acctbal > 0.0 AND 60: substring_phone IN (21, 28, 24, 32, 35, 34, 37)])
[end]

