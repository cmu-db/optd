-- TPC-H Q16
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;

/*
Error
External error: error converting aggregate expressions: optd internal error: col alias1 not found, current local bindings: [Binding { table_ref: Bare { table: "partsupp" }, schema: Schema { fields: [Field { name: "ps_partkey", data_type: Int32 }, Field { name: "ps_suppkey", data_type: Int32 }, Field { name: "ps_availqty", data_type: Int32 }, Field { name: "ps_supplycost", data_type: Decimal128(15, 2) }, Field { name: "ps_comment", data_type: Utf8View }], metadata: {} }, table_index: 1 }, Binding { table_ref: Bare { table: "part" }, schema: Schema { fields: [Field { name: "p_partkey", data_type: Int32 }, Field { name: "p_name", data_type: Utf8View }, Field { name: "p_mfgr", data_type: Utf8View }, Field { name: "p_brand", data_type: Utf8View }, Field { name: "p_type", data_type: Utf8View }, Field { name: "p_size", data_type: Int32 }, Field { name: "p_container", data_type: Utf8View }, Field { name: "p_retailprice", data_type: Decimal128(15, 2) }, Field { name: "p_comment", data_type: Utf8View }], metadata: {} }, table_index: 2 }, Binding { table_ref: Bare { table: "__correlated_sq_1" }, schema: Schema { fields: [Field { name: "s_suppkey", data_type: Int32 }], metadata: {} }, table_index: 5 }, Binding { table_ref: Bare { table: "__internal_#6" }, schema: Schema { fields: [], metadata: {} }, table_index: 6 }]
*/

