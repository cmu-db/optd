-- TPC-H Q12
SELECT
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT'
             or o_orderpriority = '2-HIGH'
             then 1 else 0 end) as high_priority_orders,
    sum(case when o_orderpriority <> '1-URGENT'
             and o_orderpriority <> '2-HIGH'
             then 1 else 0 end) as low_priority_orders
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode in ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1995-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;

/*
Error
External error: optd internal error: col lineitem.l_orderkey not found, current local bindings: [Binding { table_ref: Bare { table: "orders" }, schema: Schema { fields: [Field { name: "o_orderkey", data_type: Int32 }, Field { name: "o_orderpriority", data_type: Utf8View }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "l_orderkey", data_type: Int32 }, Field { name: "l_shipmode", data_type: Utf8View }], metadata: {} }, table_index: 3 }]
*/

