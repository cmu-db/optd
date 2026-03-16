-- TPC-H Q17
SELECT
    ROUND(SUM(l_extendedprice) / 7.0, 16) AS avg_yearly 
FROM
    lineitem,
    part 
WHERE
    p_partkey = l_partkey 
    AND p_brand = 'Brand#13' 
    AND p_container = 'JUMBO PKG' 
    AND l_quantity < ( 
        SELECT
            0.2 * AVG(l_quantity) 
        FROM
            lineitem 
        WHERE
            l_partkey = p_partkey 
    );

/*
Error
External error: optd internal error: col part.p_partkey not found, current local bindings: [Binding { table_ref: Bare { table: "lineitem" }, schema: Schema { fields: [Field { name: "l_partkey", data_type: Int32 }, Field { name: "l_quantity", data_type: Decimal128(15, 2) }, Field { name: "l_extendedprice", data_type: Decimal128(15, 2) }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "p_partkey", data_type: Int32 }], metadata: {} }, table_index: 3 }]
*/

