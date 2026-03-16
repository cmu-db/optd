-- TPC-H Q5
SELECT
    n_name AS nation,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'Asia' -- Specified region
    AND o_orderdate >= DATE '2023-01-01'
    AND o_orderdate < DATE '2024-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC;

/*
Error
External error: optd internal error: col orders.o_custkey not found, current local bindings: [Binding { table_ref: Bare { table: "customer" }, schema: Schema { fields: [Field { name: "c_custkey", data_type: Int32 }, Field { name: "c_nationkey", data_type: Int32 }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "o_orderkey", data_type: Int32 }, Field { name: "o_custkey", data_type: Int32 }], metadata: {} }, table_index: 3 }]
*/

