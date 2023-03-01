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
TOP-N (order by [[10: l_returnflag ASC NULLS FIRST, 11: l_linestatus ASC NULLS FIRST]])
    TOP-N (order by [[10: l_returnflag ASC NULLS FIRST, 11: l_linestatus ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{119: sum=sum(119: sum), 120: sum=sum(120: sum), 121: sum=sum(121: sum), 122: sum=sum(122: sum), 123: count=sum(123: count), 124: count=sum(124: count), 125: sum=sum(125: sum), 126: count=sum(126: count), 127: count=sum(127: count)}] group by [[52: l_returnflag, 53: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[52, 53]
                AGGREGATE ([LOCAL] aggregate [{119: sum=sum(54: sum_qty), 120: sum=sum(56: sum_base_price), 121: sum=sum(60: sum_disc_price), 122: sum=sum(61: sum_charge), 123: count=sum(55: count_qty), 124: count=sum(57: count_base_price), 125: sum=sum(58: sum_discount), 126: count=sum(59: count_discount), 127: count=sum(62: count_order)}] group by [[52: l_returnflag, 53: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv1] columns[51: l_shipdate, 52: l_returnflag, 53: l_linestatus, 54: sum_qty, 55: count_qty, 56: sum_base_price, 57: count_base_price, 58: sum_discount, 59: count_discount, 60: sum_disc_price, 61: sum_charge, 62: count_order] predicate[51: l_shipdate <= 1998-12-01])
[end]

