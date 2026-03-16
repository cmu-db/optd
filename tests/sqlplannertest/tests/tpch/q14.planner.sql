-- TPC-H Q14
SELECT
    100.00 * sum(case when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;

/*
Error
External error: optd internal error: col lineitem.l_partkey not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#2" }, schema: Schema { fields: [Field { name: "l_partkey", data_type: Int32 }, Field { name: "l_extendedprice", data_type: Decimal128(15, 2) }, Field { name: "l_discount", data_type: Decimal128(15, 2) }], metadata: {} }, table_index: 2 }, Binding { table_ref: Bare { table: "part" }, schema: Schema { fields: [Field { name: "p_partkey", data_type: Int32 }, Field { name: "p_type", data_type: Utf8View }], metadata: {} }, table_index: 3 }]
*/

