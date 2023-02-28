[sql]
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    sum(l_quantity) / count(*) as avg_qty,
    sum(l_extendedprice) / count(*) as avg_price,
    sum(l_discount) / count(*) as avg_disc,
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
        AGGREGATE ([GLOBAL] aggregate [{120: sum=sum(120: sum), 121: sum=sum(121: sum), 122: sum=sum(122: sum), 123: sum=sum(123: sum), 124: count=sum(124: count), 125: sum=sum(125: sum)}] group by [[32: l_returnflag, 33: l_linestatus]] having [null]
            EXCHANGE SHUFFLE[32, 33]
                AGGREGATE ([LOCAL] aggregate [{120: sum=sum(35: sum_qty), 121: sum=sum(36: sum_base_price), 122: sum=sum(38: sum_disc_price), 123: sum=sum(39: sum_charge), 124: count=sum(43: count_order), 125: sum=sum(37: sum_discount)}] group by [[32: l_returnflag, 33: l_linestatus]] having [null]
                    SCAN (mv[lineitem_agg_mv] columns[31: l_shipdate, 32: l_returnflag, 33: l_linestatus, 35: sum_qty, 36: sum_base_price, 37: sum_discount, 38: sum_disc_price, 39: sum_charge, 43: count_order] predicate[31: l_shipdate <= 1998-12-01])
[end]

