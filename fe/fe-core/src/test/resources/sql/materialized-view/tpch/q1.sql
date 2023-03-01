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
where
    l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus ;
[result]
TOP-N (order by [[9: L_RETURNFLAG ASC NULLS FIRST, 10: L_LINESTATUS ASC NULLS FIRST]])
    TOP-N (order by [[9: L_RETURNFLAG ASC NULLS FIRST, 10: L_LINESTATUS ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{20: sum=sum(20: sum), 21: sum=sum(21: sum), 22: sum=sum(22: sum), 23: sum=sum(23: sum), 24: count=count(24: count), 25: count=count(25: count), 26: sum=sum(26: sum), 27: count=count(27: count), 28: count=count(28: count)}] group by [[9: L_RETURNFLAG, 10: L_LINESTATUS]] having [null]
            EXCHANGE SHUFFLE[9, 10]
                AGGREGATE ([LOCAL] aggregate [{20: sum=sum(5: L_QUANTITY), 21: sum=sum(6: L_EXTENDEDPRICE), 22: sum=sum(18: expr), 23: sum=sum(19: expr), 24: count=count(5: L_QUANTITY), 25: count=count(6: L_EXTENDEDPRICE), 26: sum=sum(7: L_DISCOUNT), 27: count=count(7: L_DISCOUNT), 28: count=count()}] group by [[9: L_RETURNFLAG, 10: L_LINESTATUS]] having [null]
                    SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 8: L_TAX, 9: L_RETURNFLAG, 10: L_LINESTATUS, 11: L_SHIPDATE] predicate[11: L_SHIPDATE <= 1998-12-01])
[end]

