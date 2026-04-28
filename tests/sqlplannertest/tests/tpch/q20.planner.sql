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
OrderBy { ordering_exprs: "__#11.s_name"(#11.0) ASC, (.output_columns): [ "__#11.s_address"(#11.1), "__#11.s_name"(#11.0) ], (.cardinality): 0.00 }
└── Project { .table_index: 11, .projections: [ "supplier.s_name"(#1.1), "supplier.s_address"(#1.2) ], (.output_columns): [ "__#11.s_address"(#11.1), "__#11.s_name"(#11.0) ], (.cardinality): 0.00 }
    └── Select
        ├── .predicate: ("supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#2.0)) AND ("nation.n_name"(#2.1) = CAST ('IRAQ'::utf8 AS Utf8View))
        ├── (.output_columns): [ "nation.n_comment"(#2.3), "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "nation.n_regionkey"(#2.2), "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ]
        ├── (.cardinality): 0.00
        └── DependentJoin
            ├── .join_type: LeftSemi
            ├── .join_cond: "supplier.s_suppkey"(#1.0) = "__#10.ps_suppkey"(#10.0)
            ├── (.output_columns): [ "nation.n_comment"(#2.3), "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "nation.n_regionkey"(#2.2), "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ]
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: 
            │   ├── (.output_columns): [ "nation.n_comment"(#2.3), "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "nation.n_regionkey"(#2.2), "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ]
            │   ├── (.cardinality): 0.00
            │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ], (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 1, .table_index: 2, .implementation: None, (.output_columns): [ "nation.n_comment"(#2.3), "nation.n_name"(#2.1), "nation.n_nationkey"(#2.0), "nation.n_regionkey"(#2.2) ], (.cardinality): 0.00 }
            └── Project { .table_index: 10, .projections: "partsupp.ps_suppkey"(#3.1), (.output_columns): "__#10.ps_suppkey"(#10.0), (.cardinality): 0.00 }
                └── DependentJoin
                    ├── .join_type: Inner
                    ├── .join_cond: CAST ("partsupp.ps_availqty"(#3.2) AS Float64) > "__#9.Float64(0.5) * sum(lineitem.l_quantity)"(#9.0)
                    ├── (.output_columns): [ "__#9.Float64(0.5) * sum(lineitem.l_quantity)"(#9.0), "partsupp.ps_availqty"(#3.2), "partsupp.ps_comment"(#3.4), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1), "partsupp.ps_supplycost"(#3.3) ]
                    ├── (.cardinality): 0.00
                    ├── DependentJoin { .join_type: LeftSemi, .join_cond: "partsupp.ps_partkey"(#3.0) = "__#5.p_partkey"(#5.0), (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_comment"(#3.4), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1), "partsupp.ps_supplycost"(#3.3) ], (.cardinality): 0.00 }
                    │   ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_comment"(#3.4), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1), "partsupp.ps_supplycost"(#3.3) ], (.cardinality): 0.00 }
                    │   └── Project { .table_index: 5, .projections: "part.p_partkey"(#4.0), (.output_columns): "__#5.p_partkey"(#5.0), (.cardinality): 0.00 }
                    │       └── Select { .predicate: "part.p_name"(#4.1) LIKE CAST ('indian%'::utf8 AS Utf8View), (.output_columns): [ "part.p_brand"(#4.3), "part.p_comment"(#4.8), "part.p_container"(#4.6), "part.p_mfgr"(#4.2), "part.p_name"(#4.1), "part.p_partkey"(#4.0), "part.p_retailprice"(#4.7), "part.p_size"(#4.5), "part.p_type"(#4.4) ], (.cardinality): 0.00 }
                    │           └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): [ "part.p_brand"(#4.3), "part.p_comment"(#4.8), "part.p_container"(#4.6), "part.p_mfgr"(#4.2), "part.p_name"(#4.1), "part.p_partkey"(#4.0), "part.p_retailprice"(#4.7), "part.p_size"(#4.5), "part.p_type"(#4.4) ], (.cardinality): 0.00 }
                    └── Project { .table_index: 9, .projections: 0.5::float64 * CAST ("__#8.sum(lineitem.l_quantity)"(#8.0) AS Float64), (.output_columns): "__#9.Float64(0.5) * sum(lineitem.l_quantity)"(#9.0), (.cardinality): 1.00 }
                        └── Aggregate { .key_table_index: 7, .aggregate_table_index: 8, .implementation: None, .exprs: sum("lineitem.l_quantity"(#6.4)), .keys: [], (.output_columns): "__#8.sum(lineitem.l_quantity)"(#8.0), (.cardinality): 1.00 }
                            └── Select
                                ├── .predicate: ("lineitem.l_partkey"(#6.1) = "partsupp.ps_partkey"(#3.0)) AND ("lineitem.l_suppkey"(#6.2) = "partsupp.ps_suppkey"(#3.1)) AND ("lineitem.l_shipdate"(#6.10) >= CAST ('1996-01-01'::utf8 AS Date32)) AND ("lineitem.l_shipdate"(#6.10) < CAST ('1996-01-01'::utf8 AS Date32) + IntervalMonthDayNano { months: 12, days: 0, nanoseconds: 0 }::interval_month_day_nano)
                                ├── (.output_columns):
                                │   ┌── "lineitem.l_comment"(#6.15)
                                │   ├── "lineitem.l_commitdate"(#6.11)
                                │   ├── "lineitem.l_discount"(#6.6)
                                │   ├── "lineitem.l_extendedprice"(#6.5)
                                │   ├── "lineitem.l_linenumber"(#6.3)
                                │   ├── "lineitem.l_linestatus"(#6.9)
                                │   ├── "lineitem.l_orderkey"(#6.0)
                                │   ├── "lineitem.l_partkey"(#6.1)
                                │   ├── "lineitem.l_quantity"(#6.4)
                                │   ├── "lineitem.l_receiptdate"(#6.12)
                                │   ├── "lineitem.l_returnflag"(#6.8)
                                │   ├── "lineitem.l_shipdate"(#6.10)
                                │   ├── "lineitem.l_shipinstruct"(#6.13)
                                │   ├── "lineitem.l_shipmode"(#6.14)
                                │   ├── "lineitem.l_suppkey"(#6.2)
                                │   └── "lineitem.l_tax"(#6.7)
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 8
                                    ├── .table_index: 6
                                    ├── .implementation: None
                                    ├── (.output_columns):
                                    │   ┌── "lineitem.l_comment"(#6.15)
                                    │   ├── "lineitem.l_commitdate"(#6.11)
                                    │   ├── "lineitem.l_discount"(#6.6)
                                    │   ├── "lineitem.l_extendedprice"(#6.5)
                                    │   ├── "lineitem.l_linenumber"(#6.3)
                                    │   ├── "lineitem.l_linestatus"(#6.9)
                                    │   ├── "lineitem.l_orderkey"(#6.0)
                                    │   ├── "lineitem.l_partkey"(#6.1)
                                    │   ├── "lineitem.l_quantity"(#6.4)
                                    │   ├── "lineitem.l_receiptdate"(#6.12)
                                    │   ├── "lineitem.l_returnflag"(#6.8)
                                    │   ├── "lineitem.l_shipdate"(#6.10)
                                    │   ├── "lineitem.l_shipinstruct"(#6.13)
                                    │   ├── "lineitem.l_shipmode"(#6.14)
                                    │   ├── "lineitem.l_suppkey"(#6.2)
                                    │   └── "lineitem.l_tax"(#6.7)
                                    └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#11.0, Asc)], (.output_columns): [ "__#11.s_address"(#11.1), "__#11.s_name"(#11.0) ], (.cardinality): 0.00 }
└── Project { .table_index: 11, .projections: [ "supplier.s_name"(#1.1), "supplier.s_address"(#1.2) ], (.output_columns): [ "__#11.s_address"(#11.1), "__#11.s_name"(#11.0) ], (.cardinality): 0.00 }
    └── Join
        ├── .join_type: LeftSemi
        ├── .implementation: None
        ├── .join_cond: "supplier.s_suppkey"(#1.0) = "__#19.ps_suppkey"(#19.0)
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
        └── Project { .table_index: 19, .projections: "partsupp.ps_suppkey"(#3.1), (.output_columns): "__#19.ps_suppkey"(#19.0), (.cardinality): 0.00 }
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: ("partsupp.ps_partkey"(#3.0) IS NOT DISTINCT FROM "__#18.ps_partkey"(#18.1)) AND ("partsupp.ps_suppkey"(#3.1) IS NOT DISTINCT FROM "__#18.ps_suppkey"(#18.2)) AND (CAST ("partsupp.ps_availqty"(#3.2) AS Float64) > "__#18.expr0"(#18.0))
                ├── (.output_columns): [ "__#18.expr0"(#18.0), "__#18.ps_partkey"(#18.1), "__#18.ps_suppkey"(#18.2), "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ]
                ├── (.cardinality): 0.00
                ├── Join { .join_type: LeftSemi, .implementation: None, .join_cond: "partsupp.ps_partkey"(#3.0) = "__#5.p_partkey"(#5.0), (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ], (.cardinality): 0.00 }
                │   ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#3.2), "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ], (.cardinality): 0.00 }
                │   └── Project { .table_index: 5, .projections: "part.p_partkey"(#4.0), (.output_columns): "__#5.p_partkey"(#5.0), (.cardinality): 0.00 }
                │       └── Select { .predicate: "part.p_name"(#4.1) LIKE 'indian%'::utf8_view, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                │           └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                └── Project
                    ├── .table_index: 18
                    ├── .projections: [ 0.5::float64 * CAST ("__#17.sum"(#17.0) AS Float64), "__#14.ps_partkey"(#14.0), "__#14.ps_suppkey"(#14.1) ]
                    ├── (.output_columns): [ "__#18.expr0"(#18.0), "__#18.ps_partkey"(#18.1), "__#18.ps_suppkey"(#18.2) ]
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: LeftOuter
                        ├── .implementation: None
                        ├── .join_cond: ("__#14.ps_partkey"(#14.0) IS NOT DISTINCT FROM "__#16.l_partkey"(#16.0)) AND ("__#14.ps_suppkey"(#14.1) IS NOT DISTINCT FROM "__#16.l_suppkey"(#16.1))
                        ├── (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.ps_suppkey"(#14.1), "__#16.l_partkey"(#16.0), "__#16.l_suppkey"(#16.1), "__#17.sum"(#17.0) ]
                        ├── (.cardinality): 0.00
                        ├── Aggregate
                        │   ├── .key_table_index: 14
                        │   ├── .aggregate_table_index: 15
                        │   ├── .implementation: None
                        │   ├── .exprs: []
                        │   ├── .keys: [ "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ]
                        │   ├── (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.ps_suppkey"(#14.1) ]
                        │   ├── (.cardinality): 0.00
                        │   └── Join { .join_type: LeftSemi, .implementation: None, .join_cond: "partsupp.ps_partkey"(#3.0) = "__#5.p_partkey"(#5.0), (.output_columns): [ "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ], (.cardinality): 0.00 }
                        │       ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): [ "partsupp.ps_partkey"(#3.0), "partsupp.ps_suppkey"(#3.1) ], (.cardinality): 0.00 }
                        │       └── Project { .table_index: 5, .projections: "part.p_partkey"(#4.0), (.output_columns): "__#5.p_partkey"(#5.0), (.cardinality): 0.00 }
                        │           └── Select { .predicate: "part.p_name"(#4.1) LIKE 'indian%'::utf8_view, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                        │               └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): [ "part.p_name"(#4.1), "part.p_partkey"(#4.0) ], (.cardinality): 0.00 }
                        └── Aggregate
                            ├── .key_table_index: 16
                            ├── .aggregate_table_index: 17
                            ├── .implementation: None
                            ├── .exprs: sum("lineitem.l_quantity"(#6.4))
                            ├── .keys: [ "lineitem.l_partkey"(#6.1), "lineitem.l_suppkey"(#6.2) ]
                            ├── (.output_columns): [ "__#16.l_partkey"(#16.0), "__#16.l_suppkey"(#16.1), "__#17.sum"(#17.0) ]
                            ├── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: ("lineitem.l_partkey"(#6.1) = "lineitem.l_partkey"(#6.1)) AND ("lineitem.l_suppkey"(#6.2) = "lineitem.l_suppkey"(#6.2)) AND ("lineitem.l_shipdate"(#6.10) >= 1996-01-01::date32) AND ("lineitem.l_shipdate"(#6.10) < 1997-01-01::date32)
                                ├── (.output_columns): [ "lineitem.l_partkey"(#6.1), "lineitem.l_quantity"(#6.4), "lineitem.l_shipdate"(#6.10), "lineitem.l_suppkey"(#6.2) ]
                                ├── (.cardinality): 0.00
                                └── Get { .data_source_id: 8, .table_index: 6, .implementation: None, (.output_columns): [ "lineitem.l_partkey"(#6.1), "lineitem.l_quantity"(#6.4), "lineitem.l_shipdate"(#6.10), "lineitem.l_suppkey"(#6.2) ], (.cardinality): 0.00 }
*/

