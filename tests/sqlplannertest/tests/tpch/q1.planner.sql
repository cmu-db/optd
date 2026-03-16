-- TPC-H Q1
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag, l_linestatus
ORDER BY
    l_returnflag, l_linestatus
LIMIT 3;

/*
Error
External error: optd internal error: col lineitem.l_quantity not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#2" }, schema: Schema { fields: [Field { name: "__common_expr_1", data_type: Decimal128(38, 4) }, Field { name: "l_quantity", data_type: Decimal128(15, 2) }, Field { name: "l_extendedprice", data_type: Decimal128(15, 2) }, Field { name: "l_discount", data_type: Decimal128(15, 2) }, Field { name: "l_tax", data_type: Decimal128(15, 2) }, Field { name: "l_returnflag", data_type: Utf8View }, Field { name: "l_linestatus", data_type: Utf8View }], metadata: {} }, table_index: 2 }]
*/

