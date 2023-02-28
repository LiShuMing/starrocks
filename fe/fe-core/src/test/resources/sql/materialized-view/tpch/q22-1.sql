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
AGGREGATE ([GLOBAL] aggregate [{10: sum=sum(10: sum), 11: count=count(11: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{10: sum=sum(6: C_ACCTBAL), 11: count=count(6: C_ACCTBAL)}] group by [[]] having [null]
            SCAN (columns[5: C_PHONE, 6: C_ACCTBAL] predicate[6: C_ACCTBAL > 0.0 AND substring(5: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
[end]

