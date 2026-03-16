-- TPC-H Q10
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;

/*
Error
External error: optd internal error: col orders.o_custkey not found, current local bindings: [Binding { table_ref: Bare { table: "customer" }, schema: Schema { fields: [Field { name: "c_custkey", data_type: Int32 }, Field { name: "c_name", data_type: Utf8View }, Field { name: "c_address", data_type: Utf8View }, Field { name: "c_nationkey", data_type: Int32 }, Field { name: "c_phone", data_type: Utf8View }, Field { name: "c_acctbal", data_type: Decimal128(15, 2) }, Field { name: "c_comment", data_type: Utf8View }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "o_orderkey", data_type: Int32 }, Field { name: "o_custkey", data_type: Int32 }], metadata: {} }, table_index: 3 }]
*/

