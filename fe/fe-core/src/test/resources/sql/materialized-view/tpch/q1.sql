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
        AGGREGATE ([GLOBAL] aggregate [{49: sum=sum(49: sum), 50: count=sum(50: count), 51: count=sum(51: count), 43: sum=sum(43: sum), 44: sum=sum(44: sum), 45: sum=sum(45: sum), 46: sum=sum(46: sum), 47: count=sum(47: count), 48: count=sum(48: count)}] group by [[32: l_returnflag, 33: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[32, 33]
                AGGREGATE ([LOCAL] aggregate [{49: sum=sum(38: sum_discount), 50: count=sum(39: count_discount), 51: count=sum(42: count_order), 43: sum=sum(34: sum_qty), 44: sum=sum(36: sum_baske_price), 45: sum=sum(40: sum_disc_price), 46: sum=sum(41: sum_charge), 47: count=sum(35: count_qty), 48: count=sum(37: count_base_price)}] group by [[32: l_returnflag, 33: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv2] columns[31: l_shipdate, 32: l_returnflag, 33: l_linestatus, 34: sum_qty, 35: count_qty, 36: sum_baske_price, 37: count_base_price, 38: sum_discount, 39: count_discount, 40: sum_disc_price, 41: sum_charge, 42: count_order] predicate[31: l_shipdate <= 1998-12-01])
[end]

