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
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(19: sum), 20: count=sum(20: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{19: sum=sum(18: c_sum), 20: count=sum(17: c_count)}] group by [[]] having [null]
            SCAN (columns[15: c_acctbal, 16: substring_phone, 17: c_count, 18: c_sum] predicate[15: c_acctbal > 0.0 AND 16: substring_phone IN (21, 28, 24, 32, 35, 34, 37)])
[end]

