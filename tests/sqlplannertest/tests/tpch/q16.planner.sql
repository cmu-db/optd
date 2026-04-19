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
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ "__#9.supplier_cnt"(#9.3) DESC, "__#9.p_brand"(#9.0) ASC, "__#9.p_type"(#9.1) ASC, "__#9.p_size"(#9.2) ASC ], (.output_columns): [ "__#9.p_brand"(#9.0), "__#9.p_size"(#9.2), "__#9.p_type"(#9.1), "__#9.supplier_cnt"(#9.3) ], (.cardinality): 0.00 }
└── Project { .table_index: 9, .projections: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "__#8.count(DISTINCT partsupp.ps_suppkey)"(#8.0) ], (.output_columns): [ "__#9.p_brand"(#9.0), "__#9.p_size"(#9.2), "__#9.p_type"(#9.1), "__#9.supplier_cnt"(#9.3) ], (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 7, .aggregate_table_index: 8, .implementation: None, .exprs: count("partsupp.ps_suppkey"(#1.1)), .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5) ], (.output_columns): [ "__#7.p_brand"(#7.0), "__#7.p_size"(#7.2), "__#7.p_type"(#7.1), "__#8.count(DISTINCT partsupp.ps_suppkey)"(#8.0) ], (.cardinality): 0.00 }
        └── Aggregate { .key_table_index: 5, .aggregate_table_index: 6, .implementation: None, .exprs: [], .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "partsupp.ps_suppkey"(#1.1) ], (.output_columns): [ "__#5.p_brand"(#5.0), "__#5.p_size"(#5.2), "__#5.p_type"(#5.1), "__#5.ps_suppkey"(#5.3) ], (.cardinality): 0.00 }
            └── DependentJoin
                ├── .join_type: LeftAnti
                ├── .join_cond: "partsupp.ps_suppkey"(#1.1) = "__#4.s_suppkey"(#4.0)
                ├── (.output_columns):
                │   ┌── "part.p_brand"(#2.3)
                │   ├── "part.p_comment"(#2.8)
                │   ├── "part.p_container"(#2.6)
                │   ├── "part.p_mfgr"(#2.2)
                │   ├── "part.p_name"(#2.1)
                │   ├── "part.p_partkey"(#2.0)
                │   ├── "part.p_retailprice"(#2.7)
                │   ├── "part.p_size"(#2.5)
                │   ├── "part.p_type"(#2.4)
                │   ├── "partsupp.ps_availqty"(#1.2)
                │   ├── "partsupp.ps_comment"(#1.4)
                │   ├── "partsupp.ps_partkey"(#1.0)
                │   ├── "partsupp.ps_suppkey"(#1.1)
                │   └── "partsupp.ps_supplycost"(#1.3)
                ├── (.cardinality): 0.00
                ├── Select
                │   ├── .predicate: ("part.p_partkey"(#2.0) = "partsupp.ps_partkey"(#1.0)) AND ("part.p_brand"(#2.3) != CAST ('Brand#45'::utf8 AS Utf8View)) AND ("part.p_type"(#2.4) NOT LIKE CAST ('MEDIUM POLISHED%'::utf8 AS Utf8View)) AND (CAST ("part.p_size"(#2.5) AS Int64) IN [49::bigint, 14::bigint, 23::bigint, 45::bigint, 19::bigint, 3::bigint, 36::bigint, 9::bigint])
                │   ├── (.output_columns):
                │   │   ┌── "part.p_brand"(#2.3)
                │   │   ├── "part.p_comment"(#2.8)
                │   │   ├── "part.p_container"(#2.6)
                │   │   ├── "part.p_mfgr"(#2.2)
                │   │   ├── "part.p_name"(#2.1)
                │   │   ├── "part.p_partkey"(#2.0)
                │   │   ├── "part.p_retailprice"(#2.7)
                │   │   ├── "part.p_size"(#2.5)
                │   │   ├── "part.p_type"(#2.4)
                │   │   ├── "partsupp.ps_availqty"(#1.2)
                │   │   ├── "partsupp.ps_comment"(#1.4)
                │   │   ├── "partsupp.ps_partkey"(#1.0)
                │   │   ├── "partsupp.ps_suppkey"(#1.1)
                │   │   └── "partsupp.ps_supplycost"(#1.3)
                │   ├── (.cardinality): 0.00
                │   └── Join
                │       ├── .join_type: Inner
                │       ├── .implementation: None
                │       ├── .join_cond: 
                │       ├── (.output_columns):
                │       │   ┌── "part.p_brand"(#2.3)
                │       │   ├── "part.p_comment"(#2.8)
                │       │   ├── "part.p_container"(#2.6)
                │       │   ├── "part.p_mfgr"(#2.2)
                │       │   ├── "part.p_name"(#2.1)
                │       │   ├── "part.p_partkey"(#2.0)
                │       │   ├── "part.p_retailprice"(#2.7)
                │       │   ├── "part.p_size"(#2.5)
                │       │   ├── "part.p_type"(#2.4)
                │       │   ├── "partsupp.ps_availqty"(#1.2)
                │       │   ├── "partsupp.ps_comment"(#1.4)
                │       │   ├── "partsupp.ps_partkey"(#1.0)
                │       │   ├── "partsupp.ps_suppkey"(#1.1)
                │       │   └── "partsupp.ps_supplycost"(#1.3)
                │       ├── (.cardinality): 0.00
                │       ├── Get { .data_source_id: 5, .table_index: 1, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#1.2), "partsupp.ps_comment"(#1.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1), "partsupp.ps_supplycost"(#1.3) ], (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): [ "part.p_brand"(#2.3), "part.p_comment"(#2.8), "part.p_container"(#2.6), "part.p_mfgr"(#2.2), "part.p_name"(#2.1), "part.p_partkey"(#2.0), "part.p_retailprice"(#2.7), "part.p_size"(#2.5), "part.p_type"(#2.4) ], (.cardinality): 0.00 }
                └── Project { .table_index: 4, .projections: "supplier.s_suppkey"(#3.0), (.output_columns): "__#4.s_suppkey"(#4.0), (.cardinality): 0.00 }
                    └── Select { .predicate: "supplier.s_comment"(#3.6) LIKE CAST ('%Customer%Complaints%'::utf8 AS Utf8View), (.output_columns): [ "supplier.s_acctbal"(#3.5), "supplier.s_address"(#3.2), "supplier.s_comment"(#3.6), "supplier.s_name"(#3.1), "supplier.s_nationkey"(#3.3), "supplier.s_phone"(#3.4), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }
                        └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#3.5), "supplier.s_address"(#3.2), "supplier.s_comment"(#3.6), "supplier.s_name"(#3.1), "supplier.s_nationkey"(#3.3), "supplier.s_phone"(#3.4), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#9.3, Desc), (#9.0, Asc), (#9.1, Asc), (#9.2, Asc)], (.output_columns): [ "__#9.p_brand"(#9.0), "__#9.p_size"(#9.2), "__#9.p_type"(#9.1), "__#9.supplier_cnt"(#9.3) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 9
    ├── .projections: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "__#8.count(DISTINCT partsupp.ps_suppkey)"(#8.0) ]
    ├── (.output_columns): [ "__#9.p_brand"(#9.0), "__#9.p_size"(#9.2), "__#9.p_type"(#9.1), "__#9.supplier_cnt"(#9.3) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 7
        ├── .aggregate_table_index: 8
        ├── .implementation: None
        ├── .exprs: count("partsupp.ps_suppkey"(#1.1))
        ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5) ]
        ├── (.output_columns): [ "__#7.p_brand"(#7.0), "__#7.p_size"(#7.2), "__#7.p_type"(#7.1), "__#8.count(DISTINCT partsupp.ps_suppkey)"(#8.0) ]
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 5
            ├── .aggregate_table_index: 6
            ├── .implementation: None
            ├── .exprs: []
            ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "partsupp.ps_suppkey"(#1.1) ]
            ├── (.output_columns): [ "__#5.p_brand"(#5.0), "__#5.p_size"(#5.2), "__#5.p_type"(#5.1), "__#5.ps_suppkey"(#5.3) ]
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftAnti
                ├── .implementation: None
                ├── .join_cond: "partsupp.ps_suppkey"(#1.1) = "__#4.s_suppkey"(#4.0)
                ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ]
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: "part.p_partkey"(#2.0) = "partsupp.ps_partkey"(#1.0)
                │   ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ]
                │   ├── (.cardinality): 0.00
                │   ├── Get { .data_source_id: 5, .table_index: 1, .implementation: None, (.output_columns): [ "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ], (.cardinality): 0.00 }
                │   └── Select
                │       ├── .predicate: ("part.p_brand"(#2.3) != 'Brand#45'::utf8_view) AND ("part.p_type"(#2.4) NOT LIKE 'MEDIUM POLISHED%'::utf8_view) AND ("part.p_size"(#2.5) IN [49::integer, 14::integer, 23::integer, 45::integer, 19::integer, 3::integer, 36::integer, 9::integer])
                │       ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4) ]
                │       ├── (.cardinality): 0.00
                │       └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4) ], (.cardinality): 0.00 }
                └── Project { .table_index: 4, .projections: "supplier.s_suppkey"(#3.0), (.output_columns): "__#4.s_suppkey"(#4.0), (.cardinality): 0.00 }
                    └── Select { .predicate: "supplier.s_comment"(#3.6) LIKE '%Customer%Complaints%'::utf8_view, (.output_columns): [ "supplier.s_comment"(#3.6), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }
                        └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): [ "supplier.s_comment"(#3.6), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }
*/

