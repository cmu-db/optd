-- TPC-H Q18
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 250 -- original: 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;

/*
Error
External error: optd internal error: col orders.o_orderkey not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "c_custkey", data_type: Int32 }, Field { name: "c_name", data_type: Utf8View }, Field { name: "o_orderkey", data_type: Int32 }, Field { name: "o_totalprice", data_type: Decimal128(15, 2) }, Field { name: "o_orderdate", data_type: Date32 }], metadata: {} }, table_index: 3 }, Binding { table_ref: Bare { table: "lineitem" }, schema: Schema { fields: [Field { name: "l_orderkey", data_type: Int32 }, Field { name: "l_quantity", data_type: Decimal128(15, 2) }], metadata: {} }, table_index: 4 }]
*/

