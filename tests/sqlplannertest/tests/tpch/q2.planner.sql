-- TPC-H Q2
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
and p_size = 4
and p_type like '%TIN'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'AFRICA'
        )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/*
Error
External error: optd internal error: col part.p_partkey not found, current local bindings: [Binding { table_ref: Bare { table: "__internal_#2" }, schema: Schema { fields: [Field { name: "p_partkey", data_type: Int32 }, Field { name: "p_mfgr", data_type: Utf8View }], metadata: {} }, table_index: 2 }, Binding { table_ref: Bare { table: "partsupp" }, schema: Schema { fields: [Field { name: "ps_partkey", data_type: Int32 }, Field { name: "ps_suppkey", data_type: Int32 }, Field { name: "ps_supplycost", data_type: Decimal128(15, 2) }], metadata: {} }, table_index: 3 }]
*/

