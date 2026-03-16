-- TPC-H Q6
SELECT
    SUM(l_extendedprice * l_discount) AS revenue_loss
FROM
    lineitem
WHERE
    l_shipdate >= DATE '2023-01-01'
    AND l_shipdate < DATE '2024-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

/*
Error
External error: optd internal error: col lineitem.l_extendedprice not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#2" }, schema: Schema { fields: [Field { name: "l_extendedprice", data_type: Decimal128(15, 2) }, Field { name: "l_discount", data_type: Decimal128(15, 2) }], metadata: {} }, table_index: 2 }]
*/

