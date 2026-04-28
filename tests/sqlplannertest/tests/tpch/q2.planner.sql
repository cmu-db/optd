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
logical_plan after optd-initial:
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): [ "__#13.n_name"(#13.2), "__#13.p_mfgr"(#13.4), "__#13.p_partkey"(#13.3), "__#13.s_acctbal"(#13.0), "__#13.s_address"(#13.5), "__#13.s_comment"(#13.7), "__#13.s_name"(#13.1), "__#13.s_phone"(#13.6) ], (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ "__#13.s_acctbal"(#13.0) DESC, "__#13.n_name"(#13.2) ASC, "__#13.s_name"(#13.1) ASC, "__#13.p_partkey"(#13.3) ASC ], (.output_columns): [ "__#13.n_name"(#13.2), "__#13.p_mfgr"(#13.4), "__#13.p_partkey"(#13.3), "__#13.s_acctbal"(#13.0), "__#13.s_address"(#13.5), "__#13.s_comment"(#13.7), "__#13.s_name"(#13.1), "__#13.s_phone"(#13.6) ], (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 13
        ├── .projections: [ "supplier.s_acctbal"(#2.5), "supplier.s_name"(#2.1), "nation.n_name"(#4.1), "part.p_partkey"(#1.0), "part.p_mfgr"(#1.2), "supplier.s_address"(#2.2), "supplier.s_phone"(#2.4), "supplier.s_comment"(#2.6) ]
        ├── (.output_columns): [ "__#13.n_name"(#13.2), "__#13.p_mfgr"(#13.4), "__#13.p_partkey"(#13.3), "__#13.s_acctbal"(#13.0), "__#13.s_address"(#13.5), "__#13.s_comment"(#13.7), "__#13.s_name"(#13.1), "__#13.s_phone"(#13.6) ]
        ├── (.cardinality): 0.00
        └── DependentJoin
            ├── .join_type: Inner
            ├── .join_cond: "partsupp.ps_supplycost"(#3.3) = "__#12.min(partsupp.ps_supplycost)"(#12.0)
            ├── (.output_columns):
            │   ┌── "__#12.min(partsupp.ps_supplycost)"(#12.0)
            │   ├── "nation.n_comment"(#4.3)
            │   ├── "nation.n_name"(#4.1)
            │   ├── "nation.n_nationkey"(#4.0)
            │   ├── "nation.n_regionkey"(#4.2)
            │   ├── "part.p_brand"(#1.3)
            │   ├── "part.p_comment"(#1.8)
            │   ├── "part.p_container"(#1.6)
            │   ├── "part.p_mfgr"(#1.2)
            │   ├── "part.p_name"(#1.1)
            │   ├── "part.p_partkey"(#1.0)
            │   ├── "part.p_retailprice"(#1.7)
            │   ├── "part.p_size"(#1.5)
            │   ├── "part.p_type"(#1.4)
            │   ├── "partsupp.ps_availqty"(#3.2)
            │   ├── "partsupp.ps_comment"(#3.4)
            │   ├── "partsupp.ps_partkey"(#3.0)
            │   ├── "partsupp.ps_suppkey"(#3.1)
            │   ├── "partsupp.ps_supplycost"(#3.3)
            │   ├── "region.r_comment"(#5.2)
            │   ├── "region.r_name"(#5.1)
            │   ├── "region.r_regionkey"(#5.0)
            │   ├── "supplier.s_acctbal"(#2.5)
            │   ├── "supplier.s_address"(#2.2)
            │   ├── "supplier.s_comment"(#2.6)
            │   ├── "supplier.s_name"(#2.1)
            │   ├── "supplier.s_nationkey"(#2.3)
            │   ├── "supplier.s_phone"(#2.4)
            │   └── "supplier.s_suppkey"(#2.0)
            ├── (.cardinality): 0.00
            ├── Select
            │   ├── .predicate: ("part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#3.0)) AND ("supplier.s_suppkey"(#2.0) = "partsupp.ps_suppkey"(#3.1)) AND (CAST ("part.p_size"(#1.5) AS Int64) = 4::bigint) AND ("part.p_type"(#1.4) LIKE CAST ('%TIN'::utf8 AS Utf8View)) AND ("supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#4.0)) AND ("nation.n_regionkey"(#4.2) = "region.r_regionkey"(#5.0)) AND ("region.r_name"(#5.1) = CAST ('AFRICA'::utf8 AS Utf8View))
            │   ├── (.output_columns):
            │   │   ┌── "nation.n_comment"(#4.3)
            │   │   ├── "nation.n_name"(#4.1)
            │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   ├── "part.p_brand"(#1.3)
            │   │   ├── "part.p_comment"(#1.8)
            │   │   ├── "part.p_container"(#1.6)
            │   │   ├── "part.p_mfgr"(#1.2)
            │   │   ├── "part.p_name"(#1.1)
            │   │   ├── "part.p_partkey"(#1.0)
            │   │   ├── "part.p_retailprice"(#1.7)
            │   │   ├── "part.p_size"(#1.5)
            │   │   ├── "part.p_type"(#1.4)
            │   │   ├── "partsupp.ps_availqty"(#3.2)
            │   │   ├── "partsupp.ps_comment"(#3.4)
            │   │   ├── "partsupp.ps_partkey"(#3.0)
            │   │   ├── "partsupp.ps_suppkey"(#3.1)
            │   │   ├── "partsupp.ps_supplycost"(#3.3)
            │   │   ├── "region.r_comment"(#5.2)
            │   │   ├── "region.r_name"(#5.1)
            │   │   ├── "region.r_regionkey"(#5.0)
            │   │   ├── "supplier.s_acctbal"(#2.5)
            │   │   ├── "supplier.s_address"(#2.2)
            │   │   ├── "supplier.s_comment"(#2.6)
            │   │   ├── "supplier.s_name"(#2.1)
            │   │   ├── "supplier.s_nationkey"(#2.3)
            │   │   ├── "supplier.s_phone"(#2.4)
            │   │   └── "supplier.s_suppkey"(#2.0)
            │   ├── (.cardinality): 0.00
            │   └── Join
            │       ├── .join_type: Inner
            │       ├── .implementation: None
            │       ├── .join_cond: 
            │       ├── (.output_columns):
            │       │   ┌── "nation.n_comment"(#4.3)
            │       │   ├── "nation.n_name"(#4.1)
            │       │   ├── "nation.n_nationkey"(#4.0)
            │       │   ├── "nation.n_regionkey"(#4.2)
            │       │   ├── "part.p_brand"(#1.3)
            │       │   ├── "part.p_comment"(#1.8)
            │       │   ├── "part.p_container"(#1.6)
            │       │   ├── "part.p_mfgr"(#1.2)
            │       │   ├── "part.p_name"(#1.1)
            │       │   ├── "part.p_partkey"(#1.0)
            │       │   ├── "part.p_retailprice"(#1.7)
            │       │   ├── "part.p_size"(#1.5)
            │       │   ├── "part.p_type"(#1.4)
            │       │   ├── "partsupp.ps_availqty"(#3.2)
            │       │   ├── "partsupp.ps_comment"(#3.4)
            │       │   ├── "partsupp.ps_partkey"(#3.0)
            │       │   ├── "partsupp.ps_suppkey"(#3.1)
            │       │   ├── "partsupp.ps_supplycost"(#3.3)
            │       │   ├── "region.r_comment"(#5.2)
            │       │   ├── "region.r_name"(#5.1)
            │       │   ├── "region.r_regionkey"(#5.0)
            │       │   ├── "supplier.s_acctbal"(#2.5)
            │       │   ├── "supplier.s_address"(#2.2)
            │       │   ├── "supplier.s_comment"(#2.6)
            │       │   ├── "supplier.s_name"(#2.1)
            │       │   ├── "supplier.s_nationkey"(#2.3)
            │       │   ├── "supplier.s_phone"(#2.4)
            │       │   └── "supplier.s_suppkey"(#2.0)
            │       ├── (.cardinality): 0.00
            │       ├── Join
            │       │   ├── .join_type: Inner
            │       │   ├── .implementation: None
            │       │   ├── .join_cond: 
            │       │   ├── (.output_columns):
            │       │   │   ┌── "nation.n_comment"(#4.3)
            │       │   │   ├── "nation.n_name"(#4.1)
            │       │   │   ├── "nation.n_nationkey"(#4.0)
            │       │   │   ├── "nation.n_regionkey"(#4.2)
            │       │   │   ├── "part.p_brand"(#1.3)
            │       │   │   ├── "part.p_comment"(#1.8)
            │       │   │   ├── "part.p_container"(#1.6)
            │       │   │   ├── "part.p_mfgr"(#1.2)
            │       │   │   ├── "part.p_name"(#1.1)
            │       │   │   ├── "part.p_partkey"(#1.0)
            │       │   │   ├── "part.p_retailprice"(#1.7)
            │       │   │   ├── "part.p_size"(#1.5)
            │       │   │   ├── "part.p_type"(#1.4)
            │       │   │   ├── "partsupp.ps_availqty"(#3.2)
            │       │   │   ├── "partsupp.ps_comment"(#3.4)
            │       │   │   ├── "partsupp.ps_partkey"(#3.0)
            │       │   │   ├── "partsupp.ps_suppkey"(#3.1)
            │       │   │   ├── "partsupp.ps_supplycost"(#3.3)
            │       │   │   ├── "supplier.s_acctbal"(#2.5)
            │       │   │   ├── "supplier.s_address"(#2.2)
            │       │   │   ├── "supplier.s_comment"(#2.6)
            │       │   │   ├── "supplier.s_name"(#2.1)
            │       │   │   ├── "supplier.s_nationkey"(#2.3)
            │       │   │   ├── "supplier.s_phone"(#2.4)
            │       │   │   └── "supplier.s_suppkey"(#2.0)
            │       │   ├── (.cardinality): 0.00
            │       │   ├── Join
            │       │   │   ├── .join_type: Inner
            │       │   │   ├── .implementation: None
            │       │   │   ├── .join_cond: 
            │       │   │   ├── (.output_columns):
            │       │   │   │   ┌── "part.p_brand"(#1.3)
            │       │   │   │   ├── "part.p_comment"(#1.8)
            │       │   │   │   ├── "part.p_container"(#1.6)
            │       │   │   │   ├── "part.p_mfgr"(#1.2)
            │       │   │   │   ├── "part.p_name"(#1.1)
            │       │   │   │   ├── "part.p_partkey"(#1.0)
            │       │   │   │   ├── "part.p_retailprice"(#1.7)
            │       │   │   │   ├── "part.p_size"(#1.5)
            │       │   │   │   ├── "part.p_type"(#1.4)
            │       │   │   │   ├── "partsupp.ps_availqty"(#3.2)
            │       │   │   │   ├── "partsupp.ps_comment"(#3.4)
            │       │   │   │   ├── "partsupp.ps_partkey"(#3.0)
            │       │   │   │   ├── "partsupp.ps_suppkey"(#3.1)
            │       │   │   │   ├── "partsupp.ps_supplycost"(#3.3)
            │       │   │   │   ├── "supplier.s_acctbal"(#2.5)
            │       │   │   │   ├── "supplier.s_address"(#2.2)
            │       │   │   │   ├── "supplier.s_comment"(#2.6)
            │       │   │   │   ├── "supplier.s_name"(#2.1)
            │       │   │   │   ├── "supplier.s_nationkey"(#2.3)
            │       │   │   │   ├── "supplier.s_phone"(#2.4)
            │       │   │   │   └── "supplier.s_suppkey"(#2.0)
            │       │   │   ├── (.cardinality): 0.00
            │       │   │   ├── Join
            │       │   │   │   ├── .join_type: Inner
            │       │   │   │   ├── .implementation: None
            │       │   │   │   ├── .join_cond: 
            │       │   │   │   ├── (.output_columns):
            │       │   │   │   │   ┌── "part.p_brand"(#1.3)
            │       │   │   │   │   ├── "part.p_comment"(#1.8)
            │       │   │   │   │   ├── "part.p_container"(#1.6)
            │       │   │   │   │   ├── "part.p_mfgr"(#1.2)
            │       │   │   │   │   ├── "part.p_name"(#1.1)
            │       │   │   │   │   ├── "part.p_partkey"(#1.0)
            │       │   │   │   │   ├── "part.p_retailprice"(#1.7)
            │       │   │   │   │   ├── "part.p_size"(#1.5)
            │       │   │   │   │   ├── "part.p_type"(#1.4)
            │       │   │   │   │   ├── "supplier.s_acctbal"(#2.5)
            │       │   │   │   │   ├── "supplier.s_address"(#2.2)
            │       │   │   │   │   ├── "supplier.s_comment"(#2.6)
            │       │   │   │   │   ├── "supplier.s_name"(#2.1)
            │       │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
            │       │   │   │   │   ├── "supplier.s_phone"(#2.4)
            │       │   │   │   │   └── "supplier.s_suppkey"(#2.0)
            │       │   │   │   ├── (.cardinality): 0.00
            │       │   │   │   ├── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): [ "part.p_brand"(#1.3), "part.p_comment"(#1.8), "part.p_container"(#1.6), "part.p_mfgr"(#1.2), "part.p_name"(#1.1), "part.p_partkey"(#1.0), "part.p_retailprice"(#1.7), "part.p_size"(#1.5), "part.p_type"(#1.4) ], (.cardinality): 0.00 }
            │       │   │   │   └── Get { .data_source_id: 4, .table_index: 2, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#2.5), "supplier.s_address"(#2.2), "supplier.s_comment"(#2.6), "supplier.s_name"(#2.1), "supplier.s_nationkey"(#2.3), "supplier.s_phone"(#2.4), "supplier.s_suppkey"(#2.0) ], (.cardinality): 0.00 }
            │       │   │   └── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_comment"(#3.4), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1), "partsupp.ps_supplycost"(#3.3) ], (.cardinality): 0.00 }
            │       │   └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): [ "nation.n_comment"(#4.3), "nation.n_name"(#4.1), "nation.n_nationkey"(#4.0), "nation.n_regionkey"(#4.2) ], (.cardinality): 0.00 }
            │       └── Get { .data_source_id: 2, .table_index: 5, .implementation: None, (.output_columns): [ "region.r_comment"(#5.2), "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ], (.cardinality): 0.00 }
            └── Project { .table_index: 12, .projections: "__#11.min(partsupp.ps_supplycost)"(#11.0), (.output_columns): "__#12.min(partsupp.ps_supplycost)"(#12.0), (.cardinality): 1.00 }
                └── Aggregate { .key_table_index: 10, .aggregate_table_index: 11, .implementation: None, .exprs: min("partsupp.ps_supplycost"(#6.3)), .keys: [], (.output_columns): "__#11.min(partsupp.ps_supplycost)"(#11.0), (.cardinality): 1.00 }
                    └── Select
                        ├── .predicate: ("part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#6.0)) AND ("supplier.s_suppkey"(#7.0) = "partsupp.ps_suppkey"(#6.1)) AND ("supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0)) AND ("nation.n_regionkey"(#8.2) = "region.r_regionkey"(#9.0)) AND ("region.r_name"(#9.1) = CAST ('AFRICA'::utf8 AS Utf8View))
                        ├── (.output_columns):
                        │   ┌── "nation.n_comment"(#8.3)
                        │   ├── "nation.n_name"(#8.1)
                        │   ├── "nation.n_nationkey"(#8.0)
                        │   ├── "nation.n_regionkey"(#8.2)
                        │   ├── "partsupp.ps_availqty"(#6.2)
                        │   ├── "partsupp.ps_comment"(#6.4)
                        │   ├── "partsupp.ps_partkey"(#6.0)
                        │   ├── "partsupp.ps_suppkey"(#6.1)
                        │   ├── "partsupp.ps_supplycost"(#6.3)
                        │   ├── "region.r_comment"(#9.2)
                        │   ├── "region.r_name"(#9.1)
                        │   ├── "region.r_regionkey"(#9.0)
                        │   ├── "supplier.s_acctbal"(#7.5)
                        │   ├── "supplier.s_address"(#7.2)
                        │   ├── "supplier.s_comment"(#7.6)
                        │   ├── "supplier.s_name"(#7.1)
                        │   ├── "supplier.s_nationkey"(#7.3)
                        │   ├── "supplier.s_phone"(#7.4)
                        │   └── "supplier.s_suppkey"(#7.0)
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: 
                            ├── (.output_columns):
                            │   ┌── "nation.n_comment"(#8.3)
                            │   ├── "nation.n_name"(#8.1)
                            │   ├── "nation.n_nationkey"(#8.0)
                            │   ├── "nation.n_regionkey"(#8.2)
                            │   ├── "partsupp.ps_availqty"(#6.2)
                            │   ├── "partsupp.ps_comment"(#6.4)
                            │   ├── "partsupp.ps_partkey"(#6.0)
                            │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   ├── "region.r_comment"(#9.2)
                            │   ├── "region.r_name"(#9.1)
                            │   ├── "region.r_regionkey"(#9.0)
                            │   ├── "supplier.s_acctbal"(#7.5)
                            │   ├── "supplier.s_address"(#7.2)
                            │   ├── "supplier.s_comment"(#7.6)
                            │   ├── "supplier.s_name"(#7.1)
                            │   ├── "supplier.s_nationkey"(#7.3)
                            │   ├── "supplier.s_phone"(#7.4)
                            │   └── "supplier.s_suppkey"(#7.0)
                            ├── (.cardinality): 0.00
                            ├── Join
                            │   ├── .join_type: Inner
                            │   ├── .implementation: None
                            │   ├── .join_cond: 
                            │   ├── (.output_columns):
                            │   │   ┌── "nation.n_comment"(#8.3)
                            │   │   ├── "nation.n_name"(#8.1)
                            │   │   ├── "nation.n_nationkey"(#8.0)
                            │   │   ├── "nation.n_regionkey"(#8.2)
                            │   │   ├── "partsupp.ps_availqty"(#6.2)
                            │   │   ├── "partsupp.ps_comment"(#6.4)
                            │   │   ├── "partsupp.ps_partkey"(#6.0)
                            │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   ├── "supplier.s_acctbal"(#7.5)
                            │   │   ├── "supplier.s_address"(#7.2)
                            │   │   ├── "supplier.s_comment"(#7.6)
                            │   │   ├── "supplier.s_name"(#7.1)
                            │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   ├── "supplier.s_phone"(#7.4)
                            │   │   └── "supplier.s_suppkey"(#7.0)
                            │   ├── (.cardinality): 0.00
                            │   ├── Join
                            │   │   ├── .join_type: Inner
                            │   │   ├── .implementation: None
                            │   │   ├── .join_cond: 
                            │   │   ├── (.output_columns): [ "partsupp.ps_availqty"(#6.2), "partsupp.ps_comment"(#6.4), "partsupp.ps_partkey"(#6.0), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3), "supplier.s_acctbal"(#7.5), "supplier.s_address"(#7.2), "supplier.s_comment"(#7.6), "supplier.s_name"(#7.1), "supplier.s_nationkey"(#7.3), "supplier.s_phone"(#7.4), "supplier.s_suppkey"(#7.0) ]
                            │   │   ├── (.cardinality): 0.00
                            │   │   ├── Get { .data_source_id: 5, .table_index: 6, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#6.2), "partsupp.ps_comment"(#6.4), "partsupp.ps_partkey"(#6.0), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ], (.cardinality): 0.00 }
                            │   │   └── Get { .data_source_id: 4, .table_index: 7, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#7.5), "supplier.s_address"(#7.2), "supplier.s_comment"(#7.6), "supplier.s_name"(#7.1), "supplier.s_nationkey"(#7.3), "supplier.s_phone"(#7.4), "supplier.s_suppkey"(#7.0) ], (.cardinality): 0.00 }
                            │   └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): [ "nation.n_comment"(#8.3), "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 2, .table_index: 9, .implementation: None, (.output_columns): [ "region.r_comment"(#9.2), "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns):
│   ┌── "__#13.n_name"(#13.2)
│   ├── "__#13.p_mfgr"(#13.4)
│   ├── "__#13.p_partkey"(#13.3)
│   ├── "__#13.s_acctbal"(#13.0)
│   ├── "__#13.s_address"(#13.5)
│   ├── "__#13.s_comment"(#13.7)
│   ├── "__#13.s_name"(#13.1)
│   └── "__#13.s_phone"(#13.6)
├── (.cardinality): 0.00
└── EnforcerSort
    ├── tuple_ordering: [(#13.0, Desc), (#13.2, Asc), (#13.1, Asc), (#13.3, Asc)]
    ├── (.output_columns):
    │   ┌── "__#13.n_name"(#13.2)
    │   ├── "__#13.p_mfgr"(#13.4)
    │   ├── "__#13.p_partkey"(#13.3)
    │   ├── "__#13.s_acctbal"(#13.0)
    │   ├── "__#13.s_address"(#13.5)
    │   ├── "__#13.s_comment"(#13.7)
    │   ├── "__#13.s_name"(#13.1)
    │   └── "__#13.s_phone"(#13.6)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 13
        ├── .projections:
        │   ┌── "supplier.s_acctbal"(#2.5)
        │   ├── "supplier.s_name"(#2.1)
        │   ├── "nation.n_name"(#4.1)
        │   ├── "part.p_partkey"(#1.0)
        │   ├── "part.p_mfgr"(#1.2)
        │   ├── "supplier.s_address"(#2.2)
        │   ├── "supplier.s_phone"(#2.4)
        │   └── "supplier.s_comment"(#2.6)
        ├── (.output_columns):
        │   ┌── "__#13.n_name"(#13.2)
        │   ├── "__#13.p_mfgr"(#13.4)
        │   ├── "__#13.p_partkey"(#13.3)
        │   ├── "__#13.s_acctbal"(#13.0)
        │   ├── "__#13.s_address"(#13.5)
        │   ├── "__#13.s_comment"(#13.7)
        │   ├── "__#13.s_name"(#13.1)
        │   └── "__#13.s_phone"(#13.6)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("part.p_partkey"(#1.0) IS NOT DISTINCT FROM "__#18.p_partkey"(#18.1)) AND ("partsupp.ps_supplycost"(#3.3) = "__#18.min"(#18.0))
            ├── (.output_columns):
            │   ┌── "__#18.min"(#18.0)
            │   ├── "__#18.p_partkey"(#18.1)
            │   ├── "nation.n_name"(#4.1)
            │   ├── "nation.n_nationkey"(#4.0)
            │   ├── "nation.n_regionkey"(#4.2)
            │   ├── "part.p_mfgr"(#1.2)
            │   ├── "part.p_partkey"(#1.0)
            │   ├── "part.p_size"(#1.5)
            │   ├── "part.p_type"(#1.4)
            │   ├── "partsupp.ps_partkey"(#3.0)
            │   ├── "partsupp.ps_suppkey"(#3.1)
            │   ├── "partsupp.ps_supplycost"(#3.3)
            │   ├── "region.r_name"(#5.1)
            │   ├── "region.r_regionkey"(#5.0)
            │   ├── "supplier.s_acctbal"(#2.5)
            │   ├── "supplier.s_address"(#2.2)
            │   ├── "supplier.s_comment"(#2.6)
            │   ├── "supplier.s_name"(#2.1)
            │   ├── "supplier.s_nationkey"(#2.3)
            │   ├── "supplier.s_phone"(#2.4)
            │   └── "supplier.s_suppkey"(#2.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: "nation.n_regionkey"(#4.2) = "region.r_regionkey"(#5.0)
            │   ├── (.output_columns):
            │   │   ┌── "nation.n_name"(#4.1)
            │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   ├── "part.p_mfgr"(#1.2)
            │   │   ├── "part.p_partkey"(#1.0)
            │   │   ├── "part.p_size"(#1.5)
            │   │   ├── "part.p_type"(#1.4)
            │   │   ├── "partsupp.ps_partkey"(#3.0)
            │   │   ├── "partsupp.ps_suppkey"(#3.1)
            │   │   ├── "partsupp.ps_supplycost"(#3.3)
            │   │   ├── "region.r_name"(#5.1)
            │   │   ├── "region.r_regionkey"(#5.0)
            │   │   ├── "supplier.s_acctbal"(#2.5)
            │   │   ├── "supplier.s_address"(#2.2)
            │   │   ├── "supplier.s_comment"(#2.6)
            │   │   ├── "supplier.s_name"(#2.1)
            │   │   ├── "supplier.s_nationkey"(#2.3)
            │   │   ├── "supplier.s_phone"(#2.4)
            │   │   └── "supplier.s_suppkey"(#2.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: "supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#4.0)
            │   │   ├── (.output_columns):
            │   │   │   ┌── "nation.n_name"(#4.1)
            │   │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   │   ├── "part.p_mfgr"(#1.2)
            │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   ├── "partsupp.ps_partkey"(#3.0)
            │   │   │   ├── "partsupp.ps_suppkey"(#3.1)
            │   │   │   ├── "partsupp.ps_supplycost"(#3.3)
            │   │   │   ├── "supplier.s_acctbal"(#2.5)
            │   │   │   ├── "supplier.s_address"(#2.2)
            │   │   │   ├── "supplier.s_comment"(#2.6)
            │   │   │   ├── "supplier.s_name"(#2.1)
            │   │   │   ├── "supplier.s_nationkey"(#2.3)
            │   │   │   ├── "supplier.s_phone"(#2.4)
            │   │   │   └── "supplier.s_suppkey"(#2.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join
            │   │   │   ├── .join_type: Inner
            │   │   │   ├── .implementation: None
            │   │   │   ├── .join_cond: ("part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#3.0)) AND ("supplier.s_suppkey"(#2.0) = "partsupp.ps_suppkey"(#3.1))
            │   │   │   ├── (.output_columns):
            │   │   │   │   ┌── "part.p_mfgr"(#1.2)
            │   │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   │   ├── "partsupp.ps_partkey"(#3.0)
            │   │   │   │   ├── "partsupp.ps_suppkey"(#3.1)
            │   │   │   │   ├── "partsupp.ps_supplycost"(#3.3)
            │   │   │   │   ├── "supplier.s_acctbal"(#2.5)
            │   │   │   │   ├── "supplier.s_address"(#2.2)
            │   │   │   │   ├── "supplier.s_comment"(#2.6)
            │   │   │   │   ├── "supplier.s_name"(#2.1)
            │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
            │   │   │   │   ├── "supplier.s_phone"(#2.4)
            │   │   │   │   └── "supplier.s_suppkey"(#2.0)
            │   │   │   ├── (.cardinality): 0.00
            │   │   │   ├── Join
            │   │   │   │   ├── .join_type: Inner
            │   │   │   │   ├── .implementation: None
            │   │   │   │   ├── .join_cond: true::boolean
            │   │   │   │   ├── (.output_columns):
            │   │   │   │   │   ┌── "part.p_mfgr"(#1.2)
            │   │   │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   │   │   ├── "supplier.s_acctbal"(#2.5)
            │   │   │   │   │   ├── "supplier.s_address"(#2.2)
            │   │   │   │   │   ├── "supplier.s_comment"(#2.6)
            │   │   │   │   │   ├── "supplier.s_name"(#2.1)
            │   │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
            │   │   │   │   │   ├── "supplier.s_phone"(#2.4)
            │   │   │   │   │   └── "supplier.s_suppkey"(#2.0)
            │   │   │   │   ├── (.cardinality): 0.00
            │   │   │   │   ├── Select
            │   │   │   │   │   ├── .predicate: ("part.p_size"(#1.5) = 4::integer) AND ("part.p_type"(#1.4) LIKE '%TIN'::utf8_view)
            │   │   │   │   │   ├── (.output_columns): [ "part.p_mfgr"(#1.2), "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
            │   │   │   │   │   ├── (.cardinality): 0.00
            │   │   │   │   │   └── Get
            │   │   │   │   │       ├── .data_source_id: 3
            │   │   │   │   │       ├── .table_index: 1
            │   │   │   │   │       ├── .implementation: None
            │   │   │   │   │       ├── (.output_columns): [ "part.p_mfgr"(#1.2), "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
            │   │   │   │   │       └── (.cardinality): 0.00
            │   │   │   │   └── Get
            │   │   │   │       ├── .data_source_id: 4
            │   │   │   │       ├── .table_index: 2
            │   │   │   │       ├── .implementation: None
            │   │   │   │       ├── (.output_columns):
            │   │   │   │       │   ┌── "supplier.s_acctbal"(#2.5)
            │   │   │   │       │   ├── "supplier.s_address"(#2.2)
            │   │   │   │       │   ├── "supplier.s_comment"(#2.6)
            │   │   │   │       │   ├── "supplier.s_name"(#2.1)
            │   │   │   │       │   ├── "supplier.s_nationkey"(#2.3)
            │   │   │   │       │   ├── "supplier.s_phone"(#2.4)
            │   │   │   │       │   └── "supplier.s_suppkey"(#2.0)
            │   │   │   │       └── (.cardinality): 0.00
            │   │   │   └── Get
            │   │   │       ├── .data_source_id: 5
            │   │   │       ├── .table_index: 3
            │   │   │       ├── .implementation: None
            │   │   │       ├── (.output_columns): [ "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1), "partsupp.ps_supplycost"(#3.3) ]
            │   │   │       └── (.cardinality): 0.00
            │   │   └── Get
            │   │       ├── .data_source_id: 1
            │   │       ├── .table_index: 4
            │   │       ├── .implementation: None
            │   │       ├── (.output_columns): [ "nation.n_name"(#4.1), "nation.n_nationkey"(#4.0), "nation.n_regionkey"(#4.2) ]
            │   │       └── (.cardinality): 0.00
            │   └── Select
            │       ├── .predicate: "region.r_name"(#5.1) = 'AFRICA'::utf8_view
            │       ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
            │       ├── (.cardinality): 0.00
            │       └── Get
            │           ├── .data_source_id: 2
            │           ├── .table_index: 5
            │           ├── .implementation: None
            │           ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
            │           └── (.cardinality): 0.00
            └── Project
                ├── .table_index: 18
                ├── .projections: [ "__#17.min"(#17.0), "__#14.p_partkey"(#14.0) ]
                ├── (.output_columns): [ "__#18.min"(#18.0), "__#18.p_partkey"(#18.1) ]
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: LeftOuter
                    ├── .implementation: None
                    ├── .join_cond: "__#14.p_partkey"(#14.0) IS NOT DISTINCT FROM "__#16.ps_partkey"(#16.0)
                    ├── (.output_columns): [ "__#14.p_partkey"(#14.0), "__#16.ps_partkey"(#16.0), "__#17.min"(#17.0) ]
                    ├── (.cardinality): 0.00
                    ├── Aggregate
                    │   ├── .key_table_index: 14
                    │   ├── .aggregate_table_index: 15
                    │   ├── .implementation: None
                    │   ├── .exprs: []
                    │   ├── .keys: "part.p_partkey"(#1.0)
                    │   ├── (.output_columns): "__#14.p_partkey"(#14.0)
                    │   ├── (.cardinality): 0.00
                    │   └── Join
                    │       ├── .join_type: Inner
                    │       ├── .implementation: None
                    │       ├── .join_cond: "nation.n_regionkey"(#4.2) = "region.r_regionkey"(#5.0)
                    │       ├── (.output_columns):
                    │       │   ┌── "nation.n_nationkey"(#4.0)
                    │       │   ├── "nation.n_regionkey"(#4.2)
                    │       │   ├── "part.p_partkey"(#1.0)
                    │       │   ├── "part.p_size"(#1.5)
                    │       │   ├── "part.p_type"(#1.4)
                    │       │   ├── "partsupp.ps_partkey"(#3.0)
                    │       │   ├── "partsupp.ps_suppkey"(#3.1)
                    │       │   ├── "region.r_name"(#5.1)
                    │       │   ├── "region.r_regionkey"(#5.0)
                    │       │   ├── "supplier.s_nationkey"(#2.3)
                    │       │   └── "supplier.s_suppkey"(#2.0)
                    │       ├── (.cardinality): 0.00
                    │       ├── Join
                    │       │   ├── .join_type: Inner
                    │       │   ├── .implementation: None
                    │       │   ├── .join_cond: "supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#4.0)
                    │       │   ├── (.output_columns):
                    │       │   │   ┌── "nation.n_nationkey"(#4.0)
                    │       │   │   ├── "nation.n_regionkey"(#4.2)
                    │       │   │   ├── "part.p_partkey"(#1.0)
                    │       │   │   ├── "part.p_size"(#1.5)
                    │       │   │   ├── "part.p_type"(#1.4)
                    │       │   │   ├── "partsupp.ps_partkey"(#3.0)
                    │       │   │   ├── "partsupp.ps_suppkey"(#3.1)
                    │       │   │   ├── "supplier.s_nationkey"(#2.3)
                    │       │   │   └── "supplier.s_suppkey"(#2.0)
                    │       │   ├── (.cardinality): 0.00
                    │       │   ├── Join
                    │       │   │   ├── .join_type: Inner
                    │       │   │   ├── .implementation: None
                    │       │   │   ├── .join_cond: ("part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#3.0)) AND ("supplier.s_suppkey"(#2.0) = "partsupp.ps_suppkey"(#3.1))
                    │       │   │   ├── (.output_columns):
                    │       │   │   │   ┌── "part.p_partkey"(#1.0)
                    │       │   │   │   ├── "part.p_size"(#1.5)
                    │       │   │   │   ├── "part.p_type"(#1.4)
                    │       │   │   │   ├── "partsupp.ps_partkey"(#3.0)
                    │       │   │   │   ├── "partsupp.ps_suppkey"(#3.1)
                    │       │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │       │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │       │   │   ├── (.cardinality): 0.00
                    │       │   │   ├── Join
                    │       │   │   │   ├── .join_type: Inner
                    │       │   │   │   ├── .implementation: None
                    │       │   │   │   ├── .join_cond: true::boolean
                    │       │   │   │   ├── (.output_columns):
                    │       │   │   │   │   ┌── "part.p_partkey"(#1.0)
                    │       │   │   │   │   ├── "part.p_size"(#1.5)
                    │       │   │   │   │   ├── "part.p_type"(#1.4)
                    │       │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │       │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │       │   │   │   ├── (.cardinality): 0.00
                    │       │   │   │   ├── Select
                    │       │   │   │   │   ├── .predicate: ("part.p_size"(#1.5) = 4::integer) AND ("part.p_type"(#1.4) LIKE '%TIN'::utf8_view)
                    │       │   │   │   │   ├── (.output_columns): [ "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
                    │       │   │   │   │   ├── (.cardinality): 0.00
                    │       │   │   │   │   └── Get
                    │       │   │   │   │       ├── .data_source_id: 3
                    │       │   │   │   │       ├── .table_index: 1
                    │       │   │   │   │       ├── .implementation: None
                    │       │   │   │   │       ├── (.output_columns): [ "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
                    │       │   │   │   │       └── (.cardinality): 0.00
                    │       │   │   │   └── Get
                    │       │   │   │       ├── .data_source_id: 4
                    │       │   │   │       ├── .table_index: 2
                    │       │   │   │       ├── .implementation: None
                    │       │   │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#2.3), "supplier.s_suppkey"(#2.0) ]
                    │       │   │   │       └── (.cardinality): 0.00
                    │       │   │   └── Get
                    │       │   │       ├── .data_source_id: 5
                    │       │   │       ├── .table_index: 3
                    │       │   │       ├── .implementation: None
                    │       │   │       ├── (.output_columns): [ "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ]
                    │       │   │       └── (.cardinality): 0.00
                    │       │   └── Get
                    │       │       ├── .data_source_id: 1
                    │       │       ├── .table_index: 4
                    │       │       ├── .implementation: None
                    │       │       ├── (.output_columns): [ "nation.n_nationkey"(#4.0), "nation.n_regionkey"(#4.2) ]
                    │       │       └── (.cardinality): 0.00
                    │       └── Select
                    │           ├── .predicate: "region.r_name"(#5.1) = 'AFRICA'::utf8_view
                    │           ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
                    │           ├── (.cardinality): 0.00
                    │           └── Get
                    │               ├── .data_source_id: 2
                    │               ├── .table_index: 5
                    │               ├── .implementation: None
                    │               ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
                    │               └── (.cardinality): 0.00
                    └── Aggregate
                        ├── .key_table_index: 16
                        ├── .aggregate_table_index: 17
                        ├── .implementation: None
                        ├── .exprs: min("partsupp.ps_supplycost"(#6.3))
                        ├── .keys: "partsupp.ps_partkey"(#6.0)
                        ├── (.output_columns): [ "__#16.ps_partkey"(#16.0), "__#17.min"(#17.0) ]
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: "nation.n_regionkey"(#8.2) = "region.r_regionkey"(#9.0)
                            ├── (.output_columns):
                            │   ┌── "nation.n_nationkey"(#8.0)
                            │   ├── "nation.n_regionkey"(#8.2)
                            │   ├── "partsupp.ps_partkey"(#6.0)
                            │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   ├── "region.r_name"(#9.1)
                            │   ├── "region.r_regionkey"(#9.0)
                            │   ├── "supplier.s_nationkey"(#7.3)
                            │   └── "supplier.s_suppkey"(#7.0)
                            ├── (.cardinality): 0.00
                            ├── Join
                            │   ├── .join_type: Inner
                            │   ├── .implementation: None
                            │   ├── .join_cond: "supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0)
                            │   ├── (.output_columns):
                            │   │   ┌── "nation.n_nationkey"(#8.0)
                            │   │   ├── "nation.n_regionkey"(#8.2)
                            │   │   ├── "partsupp.ps_partkey"(#6.0)
                            │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   └── "supplier.s_suppkey"(#7.0)
                            │   ├── (.cardinality): 0.00
                            │   ├── Join
                            │   │   ├── .join_type: Inner
                            │   │   ├── .implementation: None
                            │   │   ├── .join_cond: "supplier.s_suppkey"(#7.0) = "partsupp.ps_suppkey"(#6.1)
                            │   │   ├── (.output_columns):
                            │   │   │   ┌── "partsupp.ps_partkey"(#6.0)
                            │   │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   │   └── "supplier.s_suppkey"(#7.0)
                            │   │   ├── (.cardinality): 0.00
                            │   │   ├── Select
                            │   │   │   ├── .predicate: "partsupp.ps_partkey"(#6.0) = "partsupp.ps_partkey"(#6.0)
                            │   │   │   ├── (.output_columns): [ "partsupp.ps_partkey"(#6.0), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ]
                            │   │   │   ├── (.cardinality): 0.00
                            │   │   │   └── Get
                            │   │   │       ├── .data_source_id: 5
                            │   │   │       ├── .table_index: 6
                            │   │   │       ├── .implementation: None
                            │   │   │       ├── (.output_columns): [ "partsupp.ps_partkey"(#6.0), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ]
                            │   │   │       └── (.cardinality): 0.00
                            │   │   └── Get
                            │   │       ├── .data_source_id: 4
                            │   │       ├── .table_index: 7
                            │   │       ├── .implementation: None
                            │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ]
                            │   │       └── (.cardinality): 0.00
                            │   └── Get
                            │       ├── .data_source_id: 1
                            │       ├── .table_index: 8
                            │       ├── .implementation: None
                            │       ├── (.output_columns): [ "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ]
                            │       └── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: "region.r_name"(#9.1) = 'AFRICA'::utf8_view
                                ├── (.output_columns): [ "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 2
                                    ├── .table_index: 9
                                    ├── .implementation: None
                                    ├── (.output_columns): [ "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                    └── (.cardinality): 0.00
*/

