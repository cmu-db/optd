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
logical_plan after optd-initial:
OrderBy { ordering_exprs: "__#17.s_name"(#17.0) ASC, (.output_columns): [ "__#17.s_address"(#17.1), "__#17.s_name"(#17.0) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 17
    ├── .projections: [ "supplier.s_name"(#1.1), "supplier.s_address"(#1.2) ]
    ├── (.output_columns): [ "__#17.s_address"(#17.1), "__#17.s_name"(#17.0) ]
    ├── (.cardinality): 0.00
    └── Select
        ├── .predicate: ("supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#2.0)) AND ("nation.n_name"(#2.1) = 'IRAQ'::utf8_view)
        ├── (.output_columns):
        │   ┌── "nation.n_comment"(#2.3)
        │   ├── "nation.n_name"(#2.1)
        │   ├── "nation.n_nationkey"(#2.0)
        │   ├── "nation.n_regionkey"(#2.2)
        │   ├── "supplier.s_acctbal"(#1.5)
        │   ├── "supplier.s_address"(#1.2)
        │   ├── "supplier.s_comment"(#1.6)
        │   ├── "supplier.s_name"(#1.1)
        │   ├── "supplier.s_nationkey"(#1.3)
        │   ├── "supplier.s_phone"(#1.4)
        │   └── "supplier.s_suppkey"(#1.0)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: LeftSemi
            ├── .implementation: None
            ├── .join_cond: ("supplier.s_suppkey"(#1.0) = "__correlated_sq_2.ps_suppkey"(#16.0))
            ├── (.output_columns):
            │   ┌── "nation.n_comment"(#2.3)
            │   ├── "nation.n_name"(#2.1)
            │   ├── "nation.n_nationkey"(#2.0)
            │   ├── "nation.n_regionkey"(#2.2)
            │   ├── "supplier.s_acctbal"(#1.5)
            │   ├── "supplier.s_address"(#1.2)
            │   ├── "supplier.s_comment"(#1.6)
            │   ├── "supplier.s_name"(#1.1)
            │   ├── "supplier.s_nationkey"(#1.3)
            │   ├── "supplier.s_phone"(#1.4)
            │   └── "supplier.s_suppkey"(#1.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: 
            │   ├── (.output_columns):
            │   │   ┌── "nation.n_comment"(#2.3)
            │   │   ├── "nation.n_name"(#2.1)
            │   │   ├── "nation.n_nationkey"(#2.0)
            │   │   ├── "nation.n_regionkey"(#2.2)
            │   │   ├── "supplier.s_acctbal"(#1.5)
            │   │   ├── "supplier.s_address"(#1.2)
            │   │   ├── "supplier.s_comment"(#1.6)
            │   │   ├── "supplier.s_name"(#1.1)
            │   │   ├── "supplier.s_nationkey"(#1.3)
            │   │   ├── "supplier.s_phone"(#1.4)
            │   │   └── "supplier.s_suppkey"(#1.0)
            │   ├── (.cardinality): 0.00
            │   ├── Get
            │   │   ├── .data_source_id: 4
            │   │   ├── .table_index: 1
            │   │   ├── .implementation: None
            │   │   ├── (.output_columns):
            │   │   │   ┌── "supplier.s_acctbal"(#1.5)
            │   │   │   ├── "supplier.s_address"(#1.2)
            │   │   │   ├── "supplier.s_comment"(#1.6)
            │   │   │   ├── "supplier.s_name"(#1.1)
            │   │   │   ├── "supplier.s_nationkey"(#1.3)
            │   │   │   ├── "supplier.s_phone"(#1.4)
            │   │   │   └── "supplier.s_suppkey"(#1.0)
            │   │   └── (.cardinality): 0.00
            │   └── Get
            │       ├── .data_source_id: 1
            │       ├── .table_index: 2
            │       ├── .implementation: None
            │       ├── (.output_columns): [ "nation.n_comment"(#2.3), "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "nation.n_regionkey"(#2.2) ]
            │       └── (.cardinality): 0.00
            └── Remap { .table_index: 16, (.output_columns): "__correlated_sq_2.ps_suppkey"(#16.0), (.cardinality): 0.00 }
                └── Project { .table_index: 15, .projections: "__#14.ps_suppkey"(#14.1), (.output_columns): "__#15.ps_suppkey"(#15.0), (.cardinality): 0.00 }
                    └── Project
                        ├── .table_index: 14
                        ├── .projections: [ "__#13.ps_partkey"(#13.0), "__#13.ps_suppkey"(#13.1), "__#13.ps_availqty"(#13.2), "__#13.ps_supplycost"(#13.3), "__#13.ps_comment"(#13.4) ]
                        ├── (.output_columns):
                        │   ┌── "__#14.ps_availqty"(#14.2)
                        │   ├── "__#14.ps_comment"(#14.4)
                        │   ├── "__#14.ps_partkey"(#14.0)
                        │   ├── "__#14.ps_suppkey"(#14.1)
                        │   └── "__#14.ps_supplycost"(#14.3)
                        ├── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: CAST ("__#13.ps_availqty"(#13.2) AS Float64) > "__#13.Float64(0.5) * sum(lineitem.l_quantity)"(#13.5)
                            ├── (.output_columns):
                            │   ┌── "__#13.Float64(0.5) * sum(lineitem.l_quantity)"(#13.5)
                            │   ├── "__#13.__always_true"(#13.8)
                            │   ├── "__#13.l_partkey"(#13.6)
                            │   ├── "__#13.l_suppkey"(#13.7)
                            │   ├── "__#13.ps_availqty"(#13.2)
                            │   ├── "__#13.ps_comment"(#13.4)
                            │   ├── "__#13.ps_partkey"(#13.0)
                            │   ├── "__#13.ps_suppkey"(#13.1)
                            │   └── "__#13.ps_supplycost"(#13.3)
                            ├── (.cardinality): 0.00
                            └── Project
                                ├── .table_index: 13
                                ├── .projections:
                                │   ┌── "partsupp.ps_partkey"(#3.0)
                                │   ├── "partsupp.ps_suppkey"(#3.1)
                                │   ├── "partsupp.ps_availqty"(#3.2)
                                │   ├── "partsupp.ps_supplycost"(#3.3)
                                │   ├── "partsupp.ps_comment"(#3.4)
                                │   ├── "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0)
                                │   ├── "__scalar_sq_3.l_partkey"(#12.1)
                                │   ├── "__scalar_sq_3.l_suppkey"(#12.2)
                                │   └── "__scalar_sq_3.__always_true"(#12.3)
                                ├── (.output_columns):
                                │   ┌── "__#13.Float64(0.5) * sum(lineitem.l_quantity)"(#13.5)
                                │   ├── "__#13.__always_true"(#13.8)
                                │   ├── "__#13.l_partkey"(#13.6)
                                │   ├── "__#13.l_suppkey"(#13.7)
                                │   ├── "__#13.ps_availqty"(#13.2)
                                │   ├── "__#13.ps_comment"(#13.4)
                                │   ├── "__#13.ps_partkey"(#13.0)
                                │   ├── "__#13.ps_suppkey"(#13.1)
                                │   └── "__#13.ps_supplycost"(#13.3)
                                ├── (.cardinality): 0.00
                                └── Join
                                    ├── .join_type: Inner
                                    ├── .implementation: None
                                    ├── .join_cond: ("partsupp.ps_partkey"(#3.0) = "__scalar_sq_3.l_partkey"(#12.1)) AND ("partsupp.ps_suppkey"(#3.1) = "__scalar_sq_3.l_suppkey"(#12.2))
                                    ├── (.output_columns):
                                    │   ┌── "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0)
                                    │   ├── "__scalar_sq_3.__always_true"(#12.3)
                                    │   ├── "__scalar_sq_3.l_partkey"(#12.1)
                                    │   ├── "__scalar_sq_3.l_suppkey"(#12.2)
                                    │   ├── "partsupp.ps_availqty"(#3.2)
                                    │   ├── "partsupp.ps_comment"(#3.4)
                                    │   ├── "partsupp.ps_partkey"(#3.0)
                                    │   ├── "partsupp.ps_suppkey"(#3.1)
                                    │   └── "partsupp.ps_supplycost"(#3.3)
                                    ├── (.cardinality): 0.00
                                    ├── Join
                                    │   ├── .join_type: LeftSemi
                                    │   ├── .implementation: None
                                    │   ├── .join_cond: ("partsupp.ps_partkey"(#3.0) = "__correlated_sq_1.p_partkey"(#6.0))
                                    │   ├── (.output_columns):
                                    │   │   ┌── "partsupp.ps_availqty"(#3.2)
                                    │   │   ├── "partsupp.ps_comment"(#3.4)
                                    │   │   ├── "partsupp.ps_partkey"(#3.0)
                                    │   │   ├── "partsupp.ps_suppkey"(#3.1)
                                    │   │   └── "partsupp.ps_supplycost"(#3.3)
                                    │   ├── (.cardinality): 0.00
                                    │   ├── Get
                                    │   │   ├── .data_source_id: 5
                                    │   │   ├── .table_index: 3
                                    │   │   ├── .implementation: None
                                    │   │   ├── (.output_columns):
                                    │   │   │   ┌── "partsupp.ps_availqty"(#3.2)
                                    │   │   │   ├── "partsupp.ps_comment"(#3.4)
                                    │   │   │   ├── "partsupp.ps_partkey"(#3.0)
                                    │   │   │   ├── "partsupp.ps_suppkey"(#3.1)
                                    │   │   │   └── "partsupp.ps_supplycost"(#3.3)
                                    │   │   └── (.cardinality): 0.00
                                    │   └── Remap { .table_index: 6, (.output_columns): "__correlated_sq_1.p_partkey"(#6.0), (.cardinality): 0.00 }
                                    │       └── Project { .table_index: 5, .projections: "part.p_partkey"(#4.0), (.output_columns): "__#5.p_partkey"(#5.0), (.cardinality): 0.00 }
                                    │           └── Select
                                    │               ├── .predicate: "part.p_name"(#4.1) LIKE 'indian%'::utf8_view
                                    │               ├── (.output_columns):
                                    │               │   ┌── "part.p_brand"(#4.3)
                                    │               │   ├── "part.p_comment"(#4.8)
                                    │               │   ├── "part.p_container"(#4.6)
                                    │               │   ├── "part.p_mfgr"(#4.2)
                                    │               │   ├── "part.p_name"(#4.1)
                                    │               │   ├── "part.p_partkey"(#4.0)
                                    │               │   ├── "part.p_retailprice"(#4.7)
                                    │               │   ├── "part.p_size"(#4.5)
                                    │               │   └── "part.p_type"(#4.4)
                                    │               ├── (.cardinality): 0.00
                                    │               └── Get
                                    │                   ├── .data_source_id: 3
                                    │                   ├── .table_index: 4
                                    │                   ├── .implementation: None
                                    │                   ├── (.output_columns):
                                    │                   │   ┌── "part.p_brand"(#4.3)
                                    │                   │   ├── "part.p_comment"(#4.8)
                                    │                   │   ├── "part.p_container"(#4.6)
                                    │                   │   ├── "part.p_mfgr"(#4.2)
                                    │                   │   ├── "part.p_name"(#4.1)
                                    │                   │   ├── "part.p_partkey"(#4.0)
                                    │                   │   ├── "part.p_retailprice"(#4.7)
                                    │                   │   ├── "part.p_size"(#4.5)
                                    │                   │   └── "part.p_type"(#4.4)
                                    │                   └── (.cardinality): 0.00
                                    └── Remap
                                        ├── .table_index: 12
                                        ├── (.output_columns):
                                        │   ┌── "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0)
                                        │   ├── "__scalar_sq_3.__always_true"(#12.3)
                                        │   ├── "__scalar_sq_3.l_partkey"(#12.1)
                                        │   └── "__scalar_sq_3.l_suppkey"(#12.2)
                                        ├── (.cardinality): 0.00
                                        └── Project
                                            ├── .table_index: 11
                                            ├── .projections:
                                            │   ┌── 0.5::float64 * CAST ("__#10.sum(lineitem.l_quantity)"(#10.3) AS Float64)
                                            │   ├── "__#10.l_partkey"(#10.0)
                                            │   ├── "__#10.l_suppkey"(#10.1)
                                            │   └── "__#10.__always_true"(#10.2)
                                            ├── (.output_columns):
                                            │   ┌── "__#11.Float64(0.5) * sum(lineitem.l_quantity)"(#11.0)
                                            │   ├── "__#11.__always_true"(#11.3)
                                            │   ├── "__#11.l_partkey"(#11.1)
                                            │   └── "__#11.l_suppkey"(#11.2)
                                            ├── (.cardinality): 0.00
                                            └── Project
                                                ├── .table_index: 10
                                                ├── .projections: [ "lineitem.l_partkey"(#7.1), "lineitem.l_suppkey"(#7.2), true::boolean, "__#9.sum(lineitem.l_quantity)"(#9.0) ]
                                                ├── (.output_columns):
                                                │   ┌── "__#10.__always_true"(#10.2)
                                                │   ├── "__#10.l_partkey"(#10.0)
                                                │   ├── "__#10.l_suppkey"(#10.1)
                                                │   └── "__#10.sum(lineitem.l_quantity)"(#10.3)
                                                ├── (.cardinality): 0.00
                                                └── Aggregate
                                                    ├── .key_table_index: 8
                                                    ├── .aggregate_table_index: 9
                                                    ├── .implementation: None
                                                    ├── .exprs: sum("lineitem.l_quantity"(#7.4))
                                                    ├── .keys: [ "lineitem.l_partkey"(#7.1), "lineitem.l_suppkey"(#7.2) ]
                                                    ├── (.output_columns): [ "__#8.l_partkey"(#8.0), "__#8.l_suppkey"(#8.1), "__#9.sum(lineitem.l_quantity)"(#9.0) ]
                                                    ├── (.cardinality): 0.00
                                                    └── Select
                                                        ├── .predicate: ("lineitem.l_shipdate"(#7.10) >= 1996-01-01::date32) AND ("lineitem.l_shipdate"(#7.10) < 1997-01-01::date32)
                                                        ├── (.output_columns):
                                                        │   ┌── "lineitem.l_comment"(#7.15)
                                                        │   ├── "lineitem.l_commitdate"(#7.11)
                                                        │   ├── "lineitem.l_discount"(#7.6)
                                                        │   ├── "lineitem.l_extendedprice"(#7.5)
                                                        │   ├── "lineitem.l_linenumber"(#7.3)
                                                        │   ├── "lineitem.l_linestatus"(#7.9)
                                                        │   ├── "lineitem.l_orderkey"(#7.0)
                                                        │   ├── "lineitem.l_partkey"(#7.1)
                                                        │   ├── "lineitem.l_quantity"(#7.4)
                                                        │   ├── "lineitem.l_receiptdate"(#7.12)
                                                        │   ├── "lineitem.l_returnflag"(#7.8)
                                                        │   ├── "lineitem.l_shipdate"(#7.10)
                                                        │   ├── "lineitem.l_shipinstruct"(#7.13)
                                                        │   ├── "lineitem.l_shipmode"(#7.14)
                                                        │   ├── "lineitem.l_suppkey"(#7.2)
                                                        │   └── "lineitem.l_tax"(#7.7)
                                                        ├── (.cardinality): 0.00
                                                        └── Get
                                                            ├── .data_source_id: 8
                                                            ├── .table_index: 7
                                                            ├── .implementation: None
                                                            ├── (.output_columns):
                                                            │   ┌── "lineitem.l_comment"(#7.15)
                                                            │   ├── "lineitem.l_commitdate"(#7.11)
                                                            │   ├── "lineitem.l_discount"(#7.6)
                                                            │   ├── "lineitem.l_extendedprice"(#7.5)
                                                            │   ├── "lineitem.l_linenumber"(#7.3)
                                                            │   ├── "lineitem.l_linestatus"(#7.9)
                                                            │   ├── "lineitem.l_orderkey"(#7.0)
                                                            │   ├── "lineitem.l_partkey"(#7.1)
                                                            │   ├── "lineitem.l_quantity"(#7.4)
                                                            │   ├── "lineitem.l_receiptdate"(#7.12)
                                                            │   ├── "lineitem.l_returnflag"(#7.8)
                                                            │   ├── "lineitem.l_shipdate"(#7.10)
                                                            │   ├── "lineitem.l_shipinstruct"(#7.13)
                                                            │   ├── "lineitem.l_shipmode"(#7.14)
                                                            │   ├── "lineitem.l_suppkey"(#7.2)
                                                            │   └── "lineitem.l_tax"(#7.7)
                                                            └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy { ordering_exprs: "__#17.s_name"(#17.0) ASC, (.output_columns): [ "__#17.s_address"(#17.1), "__#17.s_name"(#17.0) ], (.cardinality): 0.00 }
└── Project { .table_index: 17, .projections: [ "supplier.s_name"(#1.1), "supplier.s_address"(#1.2) ], (.output_columns): [ "__#17.s_address"(#17.1), "__#17.s_name"(#17.0) ], (.cardinality): 0.00 }
    └── Join
        ├── .join_type: LeftSemi
        ├── .implementation: None
        ├── .join_cond: "supplier.s_suppkey"(#1.0) = "__correlated_sq_2.ps_suppkey"(#16.0)
        ├── (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ]
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: "supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#2.0)
        │   ├── (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ]
        │   ├── (.cardinality): 0.00
        │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): [ "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ], (.cardinality): 0.00 }
        │   └── Select { .predicate: "nation.n_name"(#2.1) = 'IRAQ'::utf8_view, (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0) ], (.cardinality): 0.00 }
        │       └── Get { .data_source_id: 1, .table_index: 2, .implementation: None, (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 16, (.output_columns): "__correlated_sq_2.ps_suppkey"(#16.0), (.cardinality): 0.00 }
            └── Project { .table_index: 15, .projections: "partsupp.ps_suppkey"(#3.1), (.output_columns): "__#15.ps_suppkey"(#15.0), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: ("partsupp.ps_partkey"(#3.0) = "__scalar_sq_3.l_partkey"(#12.1)) AND ("partsupp.ps_suppkey"(#3.1) = "__scalar_sq_3.l_suppkey"(#12.2)) AND (CAST ("partsupp.ps_availqty"(#3.2) AS Float64) > "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0))
                    ├── (.output_columns):
                    │   ┌── "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0)
                    │   ├── "__scalar_sq_3.__always_true"(#12.3)
                    │   ├── "__scalar_sq_3.l_partkey"(#12.1)
                    │   ├── "__scalar_sq_3.l_suppkey"(#12.2)
                    │   ├── "partsupp.ps_availqty"(#3.2)
                    │   ├── "partsupp.ps_partkey"(#3.0)
                    │   └── "partsupp.ps_suppkey"(#3.1)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: LeftSemi
                    │   ├── .implementation: None
                    │   ├── .join_cond: "partsupp.ps_partkey"(#3.0) = "__correlated_sq_1.p_partkey"(#6.0)
                    │   ├── (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ]
                    │   ├── (.cardinality): 0.00
                    │   ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ], (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 6, (.output_columns): "__correlated_sq_1.p_partkey"(#6.0), (.cardinality): 0.00 }
                    │       └── Project { .table_index: 5, .projections: "part.p_partkey"(#4.0), (.output_columns): "__#5.p_partkey"(#5.0), (.cardinality): 0.00 }
                    │           └── Select { .predicate: "part.p_name"(#4.1) LIKE 'indian%'::utf8_view, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                    │               └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                    └── Remap { .table_index: 12, (.output_columns): [ "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0), "__scalar_sq_3.__always_true"(#12.3), "__scalar_sq_3.l_partkey"(#12.1), "__scalar_sq_3.l_suppkey"(#12.2) ], (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 11
                            ├── .projections: [ 0.5::float64 * CAST ("__#9.sum(lineitem.l_quantity)"(#9.0) AS Float64), "lineitem.l_partkey"(#7.1), "lineitem.l_suppkey"(#7.2), true::boolean ]
                            ├── (.output_columns): [ "__#11.Float64(0.5) * sum(lineitem.l_quantity)"(#11.0), "__#11.__always_true"(#11.3), "__#11.l_partkey"(#11.1), "__#11.l_suppkey"(#11.2) ]
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 8
                                ├── .aggregate_table_index: 9
                                ├── .implementation: None
                                ├── .exprs: sum("lineitem.l_quantity"(#7.4))
                                ├── .keys: [ "lineitem.l_partkey"(#7.1), "lineitem.l_suppkey"(#7.2) ]
                                ├── (.output_columns): [ "__#8.l_partkey"(#8.0), "__#8.l_suppkey"(#8.1), "__#9.sum(lineitem.l_quantity)"(#9.0) ]
                                ├── (.cardinality): 0.00
                                └── Select
                                    ├── .predicate: ("lineitem.l_shipdate"(#7.10) >= 1996-01-01::date32) AND ("lineitem.l_shipdate"(#7.10) < 1997-01-01::date32)
                                    ├── (.output_columns): [ "lineitem.l_partkey"(#7.1), "lineitem.l_quantity"(#7.4), "lineitem.l_shipdate"(#7.10), "lineitem.l_suppkey"(#7.2) ]
                                    ├── (.cardinality): 0.00
                                    └── Get { .data_source_id: 8, .table_index: 7, .implementation: None, (.output_columns): [ "lineitem.l_partkey"(#7.1), "lineitem.l_quantity"(#7.4), "lineitem.l_shipdate"(#7.10), "lineitem.l_suppkey"(#7.2) ], (.cardinality): 0.00 }

physical_plan after optd-cascades:
EnforcerSort { tuple_ordering: [(#17.0, Asc)], (.output_columns): [ "__#17.s_address"(#17.1), "__#17.s_name"(#17.0) ], (.cardinality): 0.00 }
└── Project { .table_index: 17, .projections: [ "supplier.s_name"(#1.1), "supplier.s_address"(#1.2) ], (.output_columns): [ "__#17.s_address"(#17.1), "__#17.s_name"(#17.0) ], (.cardinality): 0.00 }
    └── Join
        ├── .join_type: LeftSemi
        ├── .implementation: None
        ├── .join_cond: "supplier.s_suppkey"(#1.0) = "__correlated_sq_2.ps_suppkey"(#16.0)
        ├── (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ]
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: "supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#2.0)
        │   ├── (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ]
        │   ├── (.cardinality): 0.00
        │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): [ "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ], (.cardinality): 0.00 }
        │   └── Select { .predicate: "nation.n_name"(#2.1) = 'IRAQ'::utf8_view, (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0) ], (.cardinality): 0.00 }
        │       └── Get { .data_source_id: 1, .table_index: 2, .implementation: None, (.output_columns): [ "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 16, (.output_columns): "__correlated_sq_2.ps_suppkey"(#16.0), (.cardinality): 0.00 }
            └── Project { .table_index: 15, .projections: "partsupp.ps_suppkey"(#3.1), (.output_columns): "__#15.ps_suppkey"(#15.0), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: ("partsupp.ps_partkey"(#3.0) = "__scalar_sq_3.l_partkey"(#12.1)) AND ("partsupp.ps_suppkey"(#3.1) = "__scalar_sq_3.l_suppkey"(#12.2)) AND (CAST ("partsupp.ps_availqty"(#3.2) AS Float64) > "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0))
                    ├── (.output_columns):
                    │   ┌── "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0)
                    │   ├── "__scalar_sq_3.__always_true"(#12.3)
                    │   ├── "__scalar_sq_3.l_partkey"(#12.1)
                    │   ├── "__scalar_sq_3.l_suppkey"(#12.2)
                    │   ├── "partsupp.ps_availqty"(#3.2)
                    │   ├── "partsupp.ps_partkey"(#3.0)
                    │   └── "partsupp.ps_suppkey"(#3.1)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: LeftSemi
                    │   ├── .implementation: None
                    │   ├── .join_cond: "partsupp.ps_partkey"(#3.0) = "__correlated_sq_1.p_partkey"(#6.0)
                    │   ├── (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ]
                    │   ├── (.cardinality): 0.00
                    │   ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ], (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 6, (.output_columns): "__correlated_sq_1.p_partkey"(#6.0), (.cardinality): 0.00 }
                    │       └── Project { .table_index: 5, .projections: "part.p_partkey"(#4.0), (.output_columns): "__#5.p_partkey"(#5.0), (.cardinality): 0.00 }
                    │           └── Select { .predicate: "part.p_name"(#4.1) LIKE 'indian%'::utf8_view, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                    │               └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                    └── Remap { .table_index: 12, (.output_columns): [ "__scalar_sq_3.Float64(0.5) * sum(lineitem.l_quantity)"(#12.0), "__scalar_sq_3.__always_true"(#12.3), "__scalar_sq_3.l_partkey"(#12.1), "__scalar_sq_3.l_suppkey"(#12.2) ], (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 11
                            ├── .projections: [ 0.5::float64 * CAST ("__#9.sum(lineitem.l_quantity)"(#9.0) AS Float64), "lineitem.l_partkey"(#7.1), "lineitem.l_suppkey"(#7.2), true::boolean ]
                            ├── (.output_columns): [ "__#11.Float64(0.5) * sum(lineitem.l_quantity)"(#11.0), "__#11.__always_true"(#11.3), "__#11.l_partkey"(#11.1), "__#11.l_suppkey"(#11.2) ]
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 8
                                ├── .aggregate_table_index: 9
                                ├── .implementation: None
                                ├── .exprs: sum("lineitem.l_quantity"(#7.4))
                                ├── .keys: [ "lineitem.l_partkey"(#7.1), "lineitem.l_suppkey"(#7.2) ]
                                ├── (.output_columns): [ "__#8.l_partkey"(#8.0), "__#8.l_suppkey"(#8.1), "__#9.sum(lineitem.l_quantity)"(#9.0) ]
                                ├── (.cardinality): 0.00
                                └── Select
                                    ├── .predicate: ("lineitem.l_shipdate"(#7.10) >= 1996-01-01::date32) AND ("lineitem.l_shipdate"(#7.10) < 1997-01-01::date32)
                                    ├── (.output_columns): [ "lineitem.l_partkey"(#7.1), "lineitem.l_quantity"(#7.4), "lineitem.l_shipdate"(#7.10), "lineitem.l_suppkey"(#7.2) ]
                                    ├── (.cardinality): 0.00
                                    └── Get { .data_source_id: 8, .table_index: 7, .implementation: None, (.output_columns): [ "lineitem.l_partkey"(#7.1), "lineitem.l_quantity"(#7.4), "lineitem.l_shipdate"(#7.10), "lineitem.l_suppkey"(#7.2) ], (.cardinality): 0.00 }
*/

