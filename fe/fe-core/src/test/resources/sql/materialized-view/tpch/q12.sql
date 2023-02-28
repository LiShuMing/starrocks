[sql]
select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then cast (1 as bigint)
            else cast(0 as bigint)
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then cast (1 as bigint)
            else cast(0 as bigint)
        end) as low_line_count
from
    orders,
    lineitem
where
        o_orderkey = l_orderkey
  and l_shipmode in ('REG AIR', 'MAIL')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1997-01-01'
  and l_receiptdate < date '1998-01-01'
group by
    l_shipmode
order by
    l_shipmode ;
[result]
TOP-N (order by [[25: L_SHIPMODE ASC NULLS FIRST]])
    TOP-N (order by [[25: L_SHIPMODE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[25: L_SHIPMODE]] having [null]
            EXCHANGE SHUFFLE[25]
                AGGREGATE ([LOCAL] aggregate [{30: sum=sum(28: case), 31: sum=sum(29: case)}] group by [[25: L_SHIPMODE]] having [null]
                    SCAN (columns[71: l_shipdate, 72: l_commitdate, 73: l_receiptdate, 75: l_shipmode, 91: o_orderpriority] predicate[73: l_receiptdate >= 1997-01-01 AND 73: l_receiptdate < 1998-01-01 AND 75: l_shipmode IN (REG AIR, MAIL) AND 72: l_commitdate < 73: l_receiptdate AND 71: l_shipdate < 72: l_commitdate])
[end]

