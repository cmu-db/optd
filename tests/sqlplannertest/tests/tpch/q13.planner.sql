-- TPC-H Q13
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;

/*
Error
External error: optd internal error: col orders.o_custkey not found, current local bindings: [Binding { table_ref: Bare { table: "customer" }, schema: Schema { fields: [Field { name: "c_custkey", data_type: Int32 }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "__internal_#3" }, schema: Schema { fields: [Field { name: "o_orderkey", data_type: Int32 }, Field { name: "o_custkey", data_type: Int32 }], metadata: {} }, table_index: 3 }]
*/

