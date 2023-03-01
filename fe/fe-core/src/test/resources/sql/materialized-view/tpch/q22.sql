[sql]
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone , 1  ,2) as cntrycode,
            c_acctbal
        from
            customer
        where
                substring(c_phone , 1  ,2)  in
                ('21', '28', '24', '32', '35', '34', '37')
          and c_acctbal > (
            select
                sum(c_acctbal) / count(c_acctbal)
            from
                customer
            where
                    c_acctbal > 0.00
              and substring(c_phone , 1  ,2)  in
                  ('21', '28', '24', '32', '35', '34', '37')
        )
          and not exists (
                select
                    *
                from
                    orders
                where
                        o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode ;
[result]
TOP-N (order by [[34: substring ASC NULLS FIRST]])
    TOP-N (order by [[34: substring ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{35: count=count(35: count), 36: sum=sum(36: sum)}] group by [[34: substring]] having [null]
            EXCHANGE SHUFFLE[34]
                AGGREGATE ([LOCAL] aggregate [{35: count=count(), 36: sum=sum(6: C_ACCTBAL)}] group by [[34: substring]] having [null]
                    LEFT ANTI JOIN (join-predicate [1: C_CUSTKEY = 24: O_CUSTKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [6: C_ACCTBAL > 21: expr] post-join-predicate [null])
                            SCAN (table[customer] columns[1: C_CUSTKEY, 5: C_PHONE, 6: C_ACCTBAL] predicate[substring(5: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                            EXCHANGE BROADCAST
                                ASSERT LE 1
                                    AGGREGATE ([GLOBAL] aggregate [{88: sum=sum(88: sum), 89: count=sum(89: count)}] group by [[]] having [null]
                                        EXCHANGE GATHER
                                            AGGREGATE ([LOCAL] aggregate [{88: sum=sum(86: c_sum), 89: count=sum(85: c_count)}] group by [[]] having [null]
                                                SCAN (mv[customer_mv] columns[83: c_acctbal, 84: substring_phone, 85: c_count, 86: c_sum] predicate[83: c_acctbal > 0.0 AND 84: substring_phone IN (21, 28, 24, 32, 35, 34, 37)])
                        EXCHANGE SHUFFLE[24]
                            SCAN (table[orders] columns[24: O_CUSTKEY] predicate[null])
[end]

