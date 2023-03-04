[sql]
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum(l_quantity) / count(l_quantity) as avg_qty,
    sum(l_extendedprice) / count(l_extendedprice) as avg_price,
    sum(l_discount) / count(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
group by
    l_returnflag,
    l_linestatus
[result]
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(19: sum), 20: sum=sum(20: sum), 21: sum=sum(21: sum), 22: sum=sum(22: sum), 23: avg=avg(23: avg), 24: avg=avg(24: avg), 25: avg=avg(25: avg), 26: count=count(26: count)}] group by [[10: l_returnflag, 11: l_linestatus]] having [null]
    EXCHANGE SHUFFLE[10, 11]
        AGGREGATE ([LOCAL] aggregate [{19: sum=sum(6: l_quantity), 20: sum=sum(7: l_extendedprice), 21: sum=sum(17: expr), 22: sum=sum(18: expr), 23: avg=avg(6: l_quantity), 24: avg=avg(7: l_extendedprice), 25: avg=avg(8: l_discount), 26: count=count()}] group by [[10: l_returnflag, 11: l_linestatus]] having [null]
            SCAN (table[lineitem] columns[6: l_quantity, 7: l_extendedprice, 8: l_discount, 9: l_tax, 10: l_returnflag, 11: l_linestatus] predicate[null])
[end]

