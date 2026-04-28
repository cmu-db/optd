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
OrderBy { ordering_exprs: [ "__#11.supplier_cnt"(#11.3) DESC, "__#11.p_brand"(#11.0) ASC, "__#11.p_type"(#11.1) ASC, "__#11.p_size"(#11.2) ASC ], (.output_columns): [ "__#11.p_brand"(#11.0), "__#11.p_size"(#11.2), "__#11.p_type"(#11.1), "__#11.supplier_cnt"(#11.3) ], (.cardinality): 0.00 }
└── Project { .table_index: 11, .projections: [ "__#10.p_brand"(#10.0), "__#10.p_type"(#10.1), "__#10.p_size"(#10.2), "__#10.count(DISTINCT partsupp.ps_suppkey)"(#10.3) ], (.output_columns): [ "__#11.p_brand"(#11.0), "__#11.p_size"(#11.2), "__#11.p_type"(#11.1), "__#11.supplier_cnt"(#11.3) ], (.cardinality): 0.00 }
    └── Project { .table_index: 10, .projections: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "__#9.count(alias1)"(#9.0) ], (.output_columns): [ "__#10.count(DISTINCT partsupp.ps_suppkey)"(#10.3), "__#10.p_brand"(#10.0), "__#10.p_size"(#10.2), "__#10.p_type"(#10.1) ], (.cardinality): 0.00 }
        └── Aggregate
            ├── .key_table_index: 8
            ├── .aggregate_table_index: 9
            ├── .implementation: None
            ├── .exprs: count("__#6.alias1"(#6.3))
            ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5) ]
            ├── (.output_columns): [ "__#8.p_brand"(#8.0), "__#8.p_size"(#8.2), "__#8.p_type"(#8.1), "__#9.count(alias1)"(#9.0) ]
            ├── (.cardinality): 0.00
            └── Aggregate
                ├── .key_table_index: 6
                ├── .aggregate_table_index: 7
                ├── .implementation: None
                ├── .exprs: []
                ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "partsupp.ps_suppkey"(#1.1) ]
                ├── (.output_columns): [ "__#6.alias1"(#6.3), "__#6.p_brand"(#6.0), "__#6.p_size"(#6.2), "__#6.p_type"(#6.1) ]
                ├── (.cardinality): 0.00
                └── Select
                    ├── .predicate: ("partsupp.ps_partkey"(#1.0) = "part.p_partkey"(#2.0)) AND ("part.p_brand"(#2.3) != 'Brand#45'::utf8_view) AND ("part.p_type"(#2.4) NOT LIKE 'MEDIUM POLISHED%'::utf8_view) AND ("part.p_size"(#2.5) IN [49::integer, 14::integer, 23::integer, 45::integer, 19::integer, 3::integer, 36::integer, 9::integer])
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
                    └── Join
                        ├── .join_type: LeftAnti
                        ├── .implementation: None
                        ├── .join_cond: ("partsupp.ps_suppkey"(#1.1) = "__correlated_sq_1.s_suppkey"(#5.0))
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
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: 
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
                        │   ├── Get { .data_source_id: 5, .table_index: 1, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#1.2), "partsupp.ps_comment"(#1.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1), "partsupp.ps_supplycost"(#1.3) ], (.cardinality): 0.00 }
                        │   └── Get
                        │       ├── .data_source_id: 3
                        │       ├── .table_index: 2
                        │       ├── .implementation: None
                        │       ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_comment"(#2.8), "part.p_container"(#2.6), "part.p_mfgr"(#2.2), "part.p_name"(#2.1), "part.p_partkey"(#2.0), "part.p_retailprice"(#2.7), "part.p_size"(#2.5), "part.p_type"(#2.4) ]
                        │       └── (.cardinality): 0.00
                        └── Remap { .table_index: 5, (.output_columns): "__correlated_sq_1.s_suppkey"(#5.0), (.cardinality): 0.00 }
                            └── Project { .table_index: 4, .projections: "supplier.s_suppkey"(#3.0), (.output_columns): "__#4.s_suppkey"(#4.0), (.cardinality): 0.00 }
                                └── Select
                                    ├── .predicate: "supplier.s_comment"(#3.6) LIKE '%Customer%Complaints%'::utf8_view
                                    ├── (.output_columns): [ "supplier.s_acctbal"(#3.5), "supplier.s_address"(#3.2), "supplier.s_comment"(#3.6), "supplier.s_name"(#3.1), "supplier.s_nationkey"(#3.3), "supplier.s_phone"(#3.4), "supplier.s_suppkey"(#3.0) ]
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 4
                                        ├── .table_index: 3
                                        ├── .implementation: None
                                        ├── (.output_columns): [ "supplier.s_acctbal"(#3.5), "supplier.s_address"(#3.2), "supplier.s_comment"(#3.6), "supplier.s_name"(#3.1), "supplier.s_nationkey"(#3.3), "supplier.s_phone"(#3.4), "supplier.s_suppkey"(#3.0) ]
                                        └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs: [ "__#11.supplier_cnt"(#11.3) DESC, "__#11.p_brand"(#11.0) ASC, "__#11.p_type"(#11.1) ASC, "__#11.p_size"(#11.2) ASC ]
├── (.output_columns): [ "__#11.p_brand"(#11.0), "__#11.p_size"(#11.2), "__#11.p_type"(#11.1), "__#11.supplier_cnt"(#11.3) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 11
    ├── .projections: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "__#9.count(alias1)"(#9.0) ]
    ├── (.output_columns): [ "__#11.p_brand"(#11.0), "__#11.p_size"(#11.2), "__#11.p_type"(#11.1), "__#11.supplier_cnt"(#11.3) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 8
        ├── .aggregate_table_index: 9
        ├── .implementation: None
        ├── .exprs: count("__#6.alias1"(#6.3))
        ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5) ]
        ├── (.output_columns): [ "__#8.p_brand"(#8.0), "__#8.p_size"(#8.2), "__#8.p_type"(#8.1), "__#9.count(alias1)"(#9.0) ]
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 6
            ├── .aggregate_table_index: 7
            ├── .implementation: None
            ├── .exprs: []
            ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "partsupp.ps_suppkey"(#1.1) ]
            ├── (.output_columns): [ "__#6.alias1"(#6.3), "__#6.p_brand"(#6.0), "__#6.p_size"(#6.2), "__#6.p_type"(#6.1) ]
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftAnti
                ├── .implementation: None
                ├── .join_cond: "partsupp.ps_suppkey"(#1.1) = "__correlated_sq_1.s_suppkey"(#5.0)
                ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ]
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: "partsupp.ps_partkey"(#1.0) = "part.p_partkey"(#2.0)
                │   ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ]
                │   ├── (.cardinality): 0.00
                │   ├── Get { .data_source_id: 5, .table_index: 1, .implementation: None, (.output_columns): [ "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ], (.cardinality): 0.00 }
                │   └── Select
                │       ├── .predicate: ("part.p_brand"(#2.3) != 'Brand#45'::utf8_view) AND ("part.p_type"(#2.4) NOT LIKE 'MEDIUM POLISHED%'::utf8_view) AND ("part.p_size"(#2.5) IN [49::integer, 14::integer, 23::integer, 45::integer, 19::integer, 3::integer, 36::integer, 9::integer])
                │       ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4) ]
                │       ├── (.cardinality): 0.00
                │       └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4) ], (.cardinality): 0.00 }
                └── Remap { .table_index: 5, (.output_columns): "__correlated_sq_1.s_suppkey"(#5.0), (.cardinality): 0.00 }
                    └── Project { .table_index: 4, .projections: "supplier.s_suppkey"(#3.0), (.output_columns): "__#4.s_suppkey"(#4.0), (.cardinality): 0.00 }
                        └── Select { .predicate: "supplier.s_comment"(#3.6) LIKE '%Customer%Complaints%'::utf8_view, (.output_columns): [ "supplier.s_comment"(#3.6), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): [ "supplier.s_comment"(#3.6), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#11.3, Desc), (#11.0, Asc), (#11.1, Asc), (#11.2, Asc)], (.output_columns): [ "__#11.p_brand"(#11.0), "__#11.p_size"(#11.2), "__#11.p_type"(#11.1), "__#11.supplier_cnt"(#11.3) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 11
    ├── .projections: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "__#9.count(alias1)"(#9.0) ]
    ├── (.output_columns): [ "__#11.p_brand"(#11.0), "__#11.p_size"(#11.2), "__#11.p_type"(#11.1), "__#11.supplier_cnt"(#11.3) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 8
        ├── .aggregate_table_index: 9
        ├── .implementation: None
        ├── .exprs: count("__#6.alias1"(#6.3))
        ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5) ]
        ├── (.output_columns): [ "__#8.p_brand"(#8.0), "__#8.p_size"(#8.2), "__#8.p_type"(#8.1), "__#9.count(alias1)"(#9.0) ]
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 6
            ├── .aggregate_table_index: 7
            ├── .implementation: None
            ├── .exprs: []
            ├── .keys: [ "part.p_brand"(#2.3), "part.p_type"(#2.4), "part.p_size"(#2.5), "partsupp.ps_suppkey"(#1.1) ]
            ├── (.output_columns): [ "__#6.alias1"(#6.3), "__#6.p_brand"(#6.0), "__#6.p_size"(#6.2), "__#6.p_type"(#6.1) ]
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftAnti
                ├── .implementation: None
                ├── .join_cond: "partsupp.ps_suppkey"(#1.1) = "__correlated_sq_1.s_suppkey"(#5.0)
                ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ]
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: "partsupp.ps_partkey"(#1.0) = "part.p_partkey"(#2.0)
                │   ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ]
                │   ├── (.cardinality): 0.00
                │   ├── Get { .data_source_id: 5, .table_index: 1, .implementation: None, (.output_columns): [ "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1) ], (.cardinality): 0.00 }
                │   └── Select
                │       ├── .predicate: ("part.p_brand"(#2.3) != 'Brand#45'::utf8_view) AND ("part.p_type"(#2.4) NOT LIKE 'MEDIUM POLISHED%'::utf8_view) AND ("part.p_size"(#2.5) IN [49::integer, 14::integer, 23::integer, 45::integer, 19::integer, 3::integer, 36::integer, 9::integer])
                │       ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4) ]
                │       ├── (.cardinality): 0.00
                │       └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): [ "part.p_brand"(#2.3), "part.p_partkey"(#2.0), "part.p_size"(#2.5), "part.p_type"(#2.4) ], (.cardinality): 0.00 }
                └── Remap { .table_index: 5, (.output_columns): "__correlated_sq_1.s_suppkey"(#5.0), (.cardinality): 0.00 }
                    └── Project { .table_index: 4, .projections: "supplier.s_suppkey"(#3.0), (.output_columns): "__#4.s_suppkey"(#4.0), (.cardinality): 0.00 }
                        └── Select { .predicate: "supplier.s_comment"(#3.6) LIKE '%Customer%Complaints%'::utf8_view, (.output_columns): [ "supplier.s_comment"(#3.6), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): [ "supplier.s_comment"(#3.6), "supplier.s_suppkey"(#3.0) ], (.cardinality): 0.00 }
*/

