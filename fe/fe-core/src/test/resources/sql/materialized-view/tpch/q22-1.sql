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
AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: count=sum(73: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{72: sum=sum(17: c_sum), 73: count=sum(16: c_count)}] group by [[]] having [null]
            SCAN (mv[customer_mv] columns[14: c_acctbal, 15: substring_phone, 16: c_count, 17: c_sum] predicate[14: c_acctbal > 0.00 AND 15: substring_phone IN (21, 28, 24, 32, 35, 34, 37)])
[end]

