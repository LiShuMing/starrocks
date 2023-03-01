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
        AGGREGATE ([GLOBAL] aggregate [{88: sum=sum(88: sum), 89: sum=sum(89: sum), 90: sum=sum(90: sum), 91: sum=sum(91: sum), 92: count=sum(92: count), 93: count=sum(93: count), 94: sum=sum(94: sum), 95: count=sum(95: count), 96: count=sum(96: count)}] group by [[35: l_returnflag, 36: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[35, 36]
                AGGREGATE ([LOCAL] aggregate [{88: sum=sum(38: sum_qty), 89: sum=sum(40: sum_base_price), 90: sum=sum(44: sum_disc_price), 91: sum=sum(45: sum_charge), 92: count=sum(39: count_qty), 93: count=sum(41: count_base_price), 94: sum=sum(42: sum_discount), 95: count=sum(43: count_discount), 96: count=sum(46: count_order)}] group by [[35: l_returnflag, 36: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv] columns[34: l_shipdate, 35: l_returnflag, 36: l_linestatus, 38: sum_qty, 39: count_qty, 40: sum_base_price, 41: count_base_price, 42: sum_discount, 43: count_discount, 44: sum_disc_price, 45: sum_charge, 46: count_order] predicate[34: l_shipdate <= 1998-12-01])
[end]

