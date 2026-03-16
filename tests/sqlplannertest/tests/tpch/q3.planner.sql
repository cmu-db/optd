-- TPC-H Q3
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority 
FROM
    customer,
    orders,
    lineitem 
WHERE
    c_mktsegment = 'FURNITURE' 
    AND c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND o_orderdate < DATE '1995-03-29' 
    AND l_shipdate > DATE '1995-03-29' 
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority 
ORDER BY
    revenue DESC,
    o_orderdate LIMIT 10;

/*
Error
External error: optd internal error: col customer.c_custkey not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#2" }, schema: Schema { fields: [Field { name: "c_custkey", data_type: Int32 }], metadata: {} }, table_index: 2 }, Binding { table_ref: Bare { table: "orders" }, schema: Schema { fields: [Field { name: "o_orderkey", data_type: Int32 }, Field { name: "o_custkey", data_type: Int32 }, Field { name: "o_orderdate", data_type: Date32 }, Field { name: "o_shippriority", data_type: Int32 }], metadata: {} }, table_index: 3 }]
*/

