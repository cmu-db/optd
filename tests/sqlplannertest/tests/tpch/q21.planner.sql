-- TPC-H Q21
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = 'SAUDI ARABIA'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;

/*
Error
External error: optd internal error: col l1.l_orderkey not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#5" }, schema: Schema { fields: [Field { name: "s_name", data_type: Utf8View }, Field { name: "s_nationkey", data_type: Int32 }, Field { name: "l_orderkey", data_type: Int32 }, Field { name: "l_suppkey", data_type: Int32 }], metadata: {} }, table_index: 5 }, Binding { table_ref: Bare { table: "__internal_#7" }, schema: Schema { fields: [Field { name: "o_orderkey", data_type: Int32 }], metadata: {} }, table_index: 7 }]
*/

