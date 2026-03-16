-- TPC-H Q20
select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'indian%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'IRAQ'
order by
    s_name;

/*
Error
External error: optd internal error: col nation.n_nationkey not found, current local bindings: [Binding { table_ref: Bare { table: "supplier" }, schema: Schema { fields: [Field { name: "s_suppkey", data_type: Int32 }, Field { name: "s_name", data_type: Utf8View }, Field { name: "s_address", data_type: Utf8View }, Field { name: "s_nationkey", data_type: Int32 }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "n_nationkey", data_type: Int32 }], metadata: {} }, table_index: 3 }]
*/

