-- TPC-H Q17
SELECT
    ROUND(SUM(l_extendedprice) / 7.0, 16) AS avg_yearly 
FROM
    lineitem,
    part 
WHERE
    p_partkey = l_partkey 
    AND p_brand = 'Brand#13' 
    AND p_container = 'JUMBO PKG' 
    AND l_quantity < ( 
        SELECT
            0.2 * AVG(l_quantity) 
        FROM
            lineitem 
        WHERE
            l_partkey = p_partkey 
    );

/*
logical_plan after optd-initial:
Project { .table_index: 13, .projections: round(CAST ("__#12.sum(lineitem.l_extendedprice)"(#12.0) AS Float64) / 7::float64, 16::integer), (.output_columns): "__#13.avg_yearly"(#13.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: sum("__#10.l_extendedprice"(#10.5)), .keys: [], (.output_columns): "__#12.sum(lineitem.l_extendedprice)"(#12.0), (.cardinality): 1.00 }
    └── Project
        ├── .table_index: 10
        ├── .projections:
        │   ┌── "__#9.l_orderkey"(#9.0)
        │   ├── "__#9.l_partkey"(#9.1)
        │   ├── "__#9.l_suppkey"(#9.2)
        │   ├── "__#9.l_linenumber"(#9.3)
        │   ├── "__#9.l_quantity"(#9.4)
        │   ├── "__#9.l_extendedprice"(#9.5)
        │   ├── "__#9.l_discount"(#9.6)
        │   ├── "__#9.l_tax"(#9.7)
        │   ├── "__#9.l_returnflag"(#9.8)
        │   ├── "__#9.l_linestatus"(#9.9)
        │   ├── "__#9.l_shipdate"(#9.10)
        │   ├── "__#9.l_commitdate"(#9.11)
        │   ├── "__#9.l_receiptdate"(#9.12)
        │   ├── "__#9.l_shipinstruct"(#9.13)
        │   ├── "__#9.l_shipmode"(#9.14)
        │   ├── "__#9.l_comment"(#9.15)
        │   ├── "__#9.p_partkey"(#9.16)
        │   ├── "__#9.p_name"(#9.17)
        │   ├── "__#9.p_mfgr"(#9.18)
        │   ├── "__#9.p_brand"(#9.19)
        │   ├── "__#9.p_type"(#9.20)
        │   ├── "__#9.p_size"(#9.21)
        │   ├── "__#9.p_container"(#9.22)
        │   ├── "__#9.p_retailprice"(#9.23)
        │   └── "__#9.p_comment"(#9.24)
        ├── (.output_columns):
        │   ┌── "__#10.l_comment"(#10.15)
        │   ├── "__#10.l_commitdate"(#10.11)
        │   ├── "__#10.l_discount"(#10.6)
        │   ├── "__#10.l_extendedprice"(#10.5)
        │   ├── "__#10.l_linenumber"(#10.3)
        │   ├── "__#10.l_linestatus"(#10.9)
        │   ├── "__#10.l_orderkey"(#10.0)
        │   ├── "__#10.l_partkey"(#10.1)
        │   ├── "__#10.l_quantity"(#10.4)
        │   ├── "__#10.l_receiptdate"(#10.12)
        │   ├── "__#10.l_returnflag"(#10.8)
        │   ├── "__#10.l_shipdate"(#10.10)
        │   ├── "__#10.l_shipinstruct"(#10.13)
        │   ├── "__#10.l_shipmode"(#10.14)
        │   ├── "__#10.l_suppkey"(#10.2)
        │   ├── "__#10.l_tax"(#10.7)
        │   ├── "__#10.p_brand"(#10.19)
        │   ├── "__#10.p_comment"(#10.24)
        │   ├── "__#10.p_container"(#10.22)
        │   ├── "__#10.p_mfgr"(#10.18)
        │   ├── "__#10.p_name"(#10.17)
        │   ├── "__#10.p_partkey"(#10.16)
        │   ├── "__#10.p_retailprice"(#10.23)
        │   ├── "__#10.p_size"(#10.21)
        │   └── "__#10.p_type"(#10.20)
        ├── (.cardinality): 0.00
        └── Select
            ├── .predicate: ("__#9.p_brand"(#9.19) = 'Brand#13'::utf8_view) AND ("__#9.p_container"(#9.22) = 'JUMBO PKG'::utf8_view) AND (CAST ("__#9.l_quantity"(#9.4) AS Decimal128(30, 15)) < "__#9.Float64(0.2) * avg(lineitem.l_quantity)"(#9.25))
            ├── (.output_columns):
            │   ┌── "__#9.Float64(0.2) * avg(lineitem.l_quantity)"(#9.25)
            │   ├── "__#9.__always_true"(#9.27)
            │   ├── "__#9.l_comment"(#9.15)
            │   ├── "__#9.l_commitdate"(#9.11)
            │   ├── "__#9.l_discount"(#9.6)
            │   ├── "__#9.l_extendedprice"(#9.5)
            │   ├── "__#9.l_linenumber"(#9.3)
            │   ├── "__#9.l_linestatus"(#9.9)
            │   ├── "__#9.l_orderkey"(#9.0)
            │   ├── "__#9.l_partkey"(#9.1)
            │   ├── "__#9.l_partkey"(#9.26)
            │   ├── "__#9.l_quantity"(#9.4)
            │   ├── "__#9.l_receiptdate"(#9.12)
            │   ├── "__#9.l_returnflag"(#9.8)
            │   ├── "__#9.l_shipdate"(#9.10)
            │   ├── "__#9.l_shipinstruct"(#9.13)
            │   ├── "__#9.l_shipmode"(#9.14)
            │   ├── "__#9.l_suppkey"(#9.2)
            │   ├── "__#9.l_tax"(#9.7)
            │   ├── "__#9.p_brand"(#9.19)
            │   ├── "__#9.p_comment"(#9.24)
            │   ├── "__#9.p_container"(#9.22)
            │   ├── "__#9.p_mfgr"(#9.18)
            │   ├── "__#9.p_name"(#9.17)
            │   ├── "__#9.p_partkey"(#9.16)
            │   ├── "__#9.p_retailprice"(#9.23)
            │   ├── "__#9.p_size"(#9.21)
            │   └── "__#9.p_type"(#9.20)
            ├── (.cardinality): 0.00
            └── Project
                ├── .table_index: 9
                ├── .projections:
                │   ┌── "lineitem.l_orderkey"(#1.0)
                │   ├── "lineitem.l_partkey"(#1.1)
                │   ├── "lineitem.l_suppkey"(#1.2)
                │   ├── "lineitem.l_linenumber"(#1.3)
                │   ├── "lineitem.l_quantity"(#1.4)
                │   ├── "lineitem.l_extendedprice"(#1.5)
                │   ├── "lineitem.l_discount"(#1.6)
                │   ├── "lineitem.l_tax"(#1.7)
                │   ├── "lineitem.l_returnflag"(#1.8)
                │   ├── "lineitem.l_linestatus"(#1.9)
                │   ├── "lineitem.l_shipdate"(#1.10)
                │   ├── "lineitem.l_commitdate"(#1.11)
                │   ├── "lineitem.l_receiptdate"(#1.12)
                │   ├── "lineitem.l_shipinstruct"(#1.13)
                │   ├── "lineitem.l_shipmode"(#1.14)
                │   ├── "lineitem.l_comment"(#1.15)
                │   ├── "part.p_partkey"(#2.0)
                │   ├── "part.p_name"(#2.1)
                │   ├── "part.p_mfgr"(#2.2)
                │   ├── "part.p_brand"(#2.3)
                │   ├── "part.p_type"(#2.4)
                │   ├── "part.p_size"(#2.5)
                │   ├── "part.p_container"(#2.6)
                │   ├── "part.p_retailprice"(#2.7)
                │   ├── "part.p_comment"(#2.8)
                │   ├── "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0)
                │   ├── "__scalar_sq_1.l_partkey"(#8.1)
                │   └── "__scalar_sq_1.__always_true"(#8.2)
                ├── (.output_columns):
                │   ┌── "__#9.Float64(0.2) * avg(lineitem.l_quantity)"(#9.25)
                │   ├── "__#9.__always_true"(#9.27)
                │   ├── "__#9.l_comment"(#9.15)
                │   ├── "__#9.l_commitdate"(#9.11)
                │   ├── "__#9.l_discount"(#9.6)
                │   ├── "__#9.l_extendedprice"(#9.5)
                │   ├── "__#9.l_linenumber"(#9.3)
                │   ├── "__#9.l_linestatus"(#9.9)
                │   ├── "__#9.l_orderkey"(#9.0)
                │   ├── "__#9.l_partkey"(#9.1)
                │   ├── "__#9.l_partkey"(#9.26)
                │   ├── "__#9.l_quantity"(#9.4)
                │   ├── "__#9.l_receiptdate"(#9.12)
                │   ├── "__#9.l_returnflag"(#9.8)
                │   ├── "__#9.l_shipdate"(#9.10)
                │   ├── "__#9.l_shipinstruct"(#9.13)
                │   ├── "__#9.l_shipmode"(#9.14)
                │   ├── "__#9.l_suppkey"(#9.2)
                │   ├── "__#9.l_tax"(#9.7)
                │   ├── "__#9.p_brand"(#9.19)
                │   ├── "__#9.p_comment"(#9.24)
                │   ├── "__#9.p_container"(#9.22)
                │   ├── "__#9.p_mfgr"(#9.18)
                │   ├── "__#9.p_name"(#9.17)
                │   ├── "__#9.p_partkey"(#9.16)
                │   ├── "__#9.p_retailprice"(#9.23)
                │   ├── "__#9.p_size"(#9.21)
                │   └── "__#9.p_type"(#9.20)
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: ("part.p_partkey"(#2.0) = "__scalar_sq_1.l_partkey"(#8.1))
                    ├── (.output_columns):
                    │   ┌── "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0)
                    │   ├── "__scalar_sq_1.__always_true"(#8.2)
                    │   ├── "__scalar_sq_1.l_partkey"(#8.1)
                    │   ├── "lineitem.l_comment"(#1.15)
                    │   ├── "lineitem.l_commitdate"(#1.11)
                    │   ├── "lineitem.l_discount"(#1.6)
                    │   ├── "lineitem.l_extendedprice"(#1.5)
                    │   ├── "lineitem.l_linenumber"(#1.3)
                    │   ├── "lineitem.l_linestatus"(#1.9)
                    │   ├── "lineitem.l_orderkey"(#1.0)
                    │   ├── "lineitem.l_partkey"(#1.1)
                    │   ├── "lineitem.l_quantity"(#1.4)
                    │   ├── "lineitem.l_receiptdate"(#1.12)
                    │   ├── "lineitem.l_returnflag"(#1.8)
                    │   ├── "lineitem.l_shipdate"(#1.10)
                    │   ├── "lineitem.l_shipinstruct"(#1.13)
                    │   ├── "lineitem.l_shipmode"(#1.14)
                    │   ├── "lineitem.l_suppkey"(#1.2)
                    │   ├── "lineitem.l_tax"(#1.7)
                    │   ├── "part.p_brand"(#2.3)
                    │   ├── "part.p_comment"(#2.8)
                    │   ├── "part.p_container"(#2.6)
                    │   ├── "part.p_mfgr"(#2.2)
                    │   ├── "part.p_name"(#2.1)
                    │   ├── "part.p_partkey"(#2.0)
                    │   ├── "part.p_retailprice"(#2.7)
                    │   ├── "part.p_size"(#2.5)
                    │   └── "part.p_type"(#2.4)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: ("lineitem.l_partkey"(#1.1) = "part.p_partkey"(#2.0))
                    │   ├── (.output_columns):
                    │   │   ┌── "lineitem.l_comment"(#1.15)
                    │   │   ├── "lineitem.l_commitdate"(#1.11)
                    │   │   ├── "lineitem.l_discount"(#1.6)
                    │   │   ├── "lineitem.l_extendedprice"(#1.5)
                    │   │   ├── "lineitem.l_linenumber"(#1.3)
                    │   │   ├── "lineitem.l_linestatus"(#1.9)
                    │   │   ├── "lineitem.l_orderkey"(#1.0)
                    │   │   ├── "lineitem.l_partkey"(#1.1)
                    │   │   ├── "lineitem.l_quantity"(#1.4)
                    │   │   ├── "lineitem.l_receiptdate"(#1.12)
                    │   │   ├── "lineitem.l_returnflag"(#1.8)
                    │   │   ├── "lineitem.l_shipdate"(#1.10)
                    │   │   ├── "lineitem.l_shipinstruct"(#1.13)
                    │   │   ├── "lineitem.l_shipmode"(#1.14)
                    │   │   ├── "lineitem.l_suppkey"(#1.2)
                    │   │   ├── "lineitem.l_tax"(#1.7)
                    │   │   ├── "part.p_brand"(#2.3)
                    │   │   ├── "part.p_comment"(#2.8)
                    │   │   ├── "part.p_container"(#2.6)
                    │   │   ├── "part.p_mfgr"(#2.2)
                    │   │   ├── "part.p_name"(#2.1)
                    │   │   ├── "part.p_partkey"(#2.0)
                    │   │   ├── "part.p_retailprice"(#2.7)
                    │   │   ├── "part.p_size"(#2.5)
                    │   │   └── "part.p_type"(#2.4)
                    │   ├── (.cardinality): 0.00
                    │   ├── Get
                    │   │   ├── .data_source_id: 8
                    │   │   ├── .table_index: 1
                    │   │   ├── .implementation: None
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "lineitem.l_comment"(#1.15)
                    │   │   │   ├── "lineitem.l_commitdate"(#1.11)
                    │   │   │   ├── "lineitem.l_discount"(#1.6)
                    │   │   │   ├── "lineitem.l_extendedprice"(#1.5)
                    │   │   │   ├── "lineitem.l_linenumber"(#1.3)
                    │   │   │   ├── "lineitem.l_linestatus"(#1.9)
                    │   │   │   ├── "lineitem.l_orderkey"(#1.0)
                    │   │   │   ├── "lineitem.l_partkey"(#1.1)
                    │   │   │   ├── "lineitem.l_quantity"(#1.4)
                    │   │   │   ├── "lineitem.l_receiptdate"(#1.12)
                    │   │   │   ├── "lineitem.l_returnflag"(#1.8)
                    │   │   │   ├── "lineitem.l_shipdate"(#1.10)
                    │   │   │   ├── "lineitem.l_shipinstruct"(#1.13)
                    │   │   │   ├── "lineitem.l_shipmode"(#1.14)
                    │   │   │   ├── "lineitem.l_suppkey"(#1.2)
                    │   │   │   └── "lineitem.l_tax"(#1.7)
                    │   │   └── (.cardinality): 0.00
                    │   └── Get
                    │       ├── .data_source_id: 3
                    │       ├── .table_index: 2
                    │       ├── .implementation: None
                    │       ├── (.output_columns):
                    │       │   ┌── "part.p_brand"(#2.3)
                    │       │   ├── "part.p_comment"(#2.8)
                    │       │   ├── "part.p_container"(#2.6)
                    │       │   ├── "part.p_mfgr"(#2.2)
                    │       │   ├── "part.p_name"(#2.1)
                    │       │   ├── "part.p_partkey"(#2.0)
                    │       │   ├── "part.p_retailprice"(#2.7)
                    │       │   ├── "part.p_size"(#2.5)
                    │       │   └── "part.p_type"(#2.4)
                    │       └── (.cardinality): 0.00
                    └── Remap { .table_index: 8, (.output_columns): [ "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0), "__scalar_sq_1.__always_true"(#8.2), "__scalar_sq_1.l_partkey"(#8.1) ], (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 7
                            ├── .projections: [ CAST (0.2::float64 * CAST ("__#6.avg(lineitem.l_quantity)"(#6.2) AS Float64) AS Decimal128(30, 15)), "lineitem.l_partkey"(#1.1), "__#6.__always_true"(#6.1) ]
                            ├── (.output_columns): [ "__#7.Float64(0.2) * avg(lineitem.l_quantity)"(#7.0), "__#7.__always_true"(#7.2), "__#7.l_partkey"(#7.1) ]
                            ├── (.cardinality): 0.00
                            └── Project
                                ├── .table_index: 6
                                ├── .projections: [ "lineitem.l_partkey"(#3.1), true::boolean, "__#5.avg(lineitem.l_quantity)"(#5.0) ]
                                ├── (.output_columns): [ "__#6.__always_true"(#6.1), "__#6.avg(lineitem.l_quantity)"(#6.2), "__#6.l_partkey"(#6.0) ]
                                ├── (.cardinality): 0.00
                                └── Aggregate
                                    ├── .key_table_index: 4
                                    ├── .aggregate_table_index: 5
                                    ├── .implementation: None
                                    ├── .exprs: avg("lineitem.l_quantity"(#3.4))
                                    ├── .keys: "lineitem.l_partkey"(#3.1)
                                    ├── (.output_columns): [ "__#4.l_partkey"(#4.0), "__#5.avg(lineitem.l_quantity)"(#5.0) ]
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 3
                                        ├── .implementation: None
                                        ├── (.output_columns):
                                        │   ┌── "lineitem.l_comment"(#3.15)
                                        │   ├── "lineitem.l_commitdate"(#3.11)
                                        │   ├── "lineitem.l_discount"(#3.6)
                                        │   ├── "lineitem.l_extendedprice"(#3.5)
                                        │   ├── "lineitem.l_linenumber"(#3.3)
                                        │   ├── "lineitem.l_linestatus"(#3.9)
                                        │   ├── "lineitem.l_orderkey"(#3.0)
                                        │   ├── "lineitem.l_partkey"(#3.1)
                                        │   ├── "lineitem.l_quantity"(#3.4)
                                        │   ├── "lineitem.l_receiptdate"(#3.12)
                                        │   ├── "lineitem.l_returnflag"(#3.8)
                                        │   ├── "lineitem.l_shipdate"(#3.10)
                                        │   ├── "lineitem.l_shipinstruct"(#3.13)
                                        │   ├── "lineitem.l_shipmode"(#3.14)
                                        │   ├── "lineitem.l_suppkey"(#3.2)
                                        │   └── "lineitem.l_tax"(#3.7)
                                        └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
Project { .table_index: 13, .projections: round(CAST ("__#12.sum(lineitem.l_extendedprice)"(#12.0) AS Float64) / 7::float64, 16::integer), (.output_columns): "__#13.avg_yearly"(#13.0), (.cardinality): 1.00 }
└── Aggregate
    ├── .key_table_index: 11
    ├── .aggregate_table_index: 12
    ├── .implementation: None
    ├── .exprs: sum("__#10.l_extendedprice"(#10.5))
    ├── .keys: []
    ├── (.output_columns): "__#12.sum(lineitem.l_extendedprice)"(#12.0)
    ├── (.cardinality): 1.00
    └── Project
        ├── .table_index: 10
        ├── .projections:
        │   ┌── "lineitem.l_orderkey"(#1.0)
        │   ├── "lineitem.l_partkey"(#1.1)
        │   ├── "lineitem.l_suppkey"(#1.2)
        │   ├── "lineitem.l_linenumber"(#1.3)
        │   ├── "lineitem.l_quantity"(#1.4)
        │   ├── "lineitem.l_extendedprice"(#1.5)
        │   ├── "lineitem.l_discount"(#1.6)
        │   ├── "lineitem.l_tax"(#1.7)
        │   ├── "lineitem.l_returnflag"(#1.8)
        │   ├── "lineitem.l_linestatus"(#1.9)
        │   ├── "lineitem.l_shipdate"(#1.10)
        │   ├── "lineitem.l_commitdate"(#1.11)
        │   ├── "lineitem.l_receiptdate"(#1.12)
        │   ├── "lineitem.l_shipinstruct"(#1.13)
        │   ├── "lineitem.l_shipmode"(#1.14)
        │   ├── "lineitem.l_comment"(#1.15)
        │   ├── "part.p_partkey"(#2.0)
        │   ├── "part.p_name"(#2.1)
        │   ├── "part.p_mfgr"(#2.2)
        │   ├── "part.p_brand"(#2.3)
        │   ├── "part.p_type"(#2.4)
        │   ├── "part.p_size"(#2.5)
        │   ├── "part.p_container"(#2.6)
        │   ├── "part.p_retailprice"(#2.7)
        │   └── "part.p_comment"(#2.8)
        ├── (.output_columns):
        │   ┌── "__#10.l_comment"(#10.15)
        │   ├── "__#10.l_commitdate"(#10.11)
        │   ├── "__#10.l_discount"(#10.6)
        │   ├── "__#10.l_extendedprice"(#10.5)
        │   ├── "__#10.l_linenumber"(#10.3)
        │   ├── "__#10.l_linestatus"(#10.9)
        │   ├── "__#10.l_orderkey"(#10.0)
        │   ├── "__#10.l_partkey"(#10.1)
        │   ├── "__#10.l_quantity"(#10.4)
        │   ├── "__#10.l_receiptdate"(#10.12)
        │   ├── "__#10.l_returnflag"(#10.8)
        │   ├── "__#10.l_shipdate"(#10.10)
        │   ├── "__#10.l_shipinstruct"(#10.13)
        │   ├── "__#10.l_shipmode"(#10.14)
        │   ├── "__#10.l_suppkey"(#10.2)
        │   ├── "__#10.l_tax"(#10.7)
        │   ├── "__#10.p_brand"(#10.19)
        │   ├── "__#10.p_comment"(#10.24)
        │   ├── "__#10.p_container"(#10.22)
        │   ├── "__#10.p_mfgr"(#10.18)
        │   ├── "__#10.p_name"(#10.17)
        │   ├── "__#10.p_partkey"(#10.16)
        │   ├── "__#10.p_retailprice"(#10.23)
        │   ├── "__#10.p_size"(#10.21)
        │   └── "__#10.p_type"(#10.20)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("part.p_partkey"(#2.0) = "__scalar_sq_1.l_partkey"(#8.1)) AND (CAST ("lineitem.l_quantity"(#1.4) AS Decimal128(30, 15)) < "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0))
            ├── (.output_columns):
            │   ┌── "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0)
            │   ├── "__scalar_sq_1.__always_true"(#8.2)
            │   ├── "__scalar_sq_1.l_partkey"(#8.1)
            │   ├── "lineitem.l_comment"(#1.15)
            │   ├── "lineitem.l_commitdate"(#1.11)
            │   ├── "lineitem.l_discount"(#1.6)
            │   ├── "lineitem.l_extendedprice"(#1.5)
            │   ├── "lineitem.l_linenumber"(#1.3)
            │   ├── "lineitem.l_linestatus"(#1.9)
            │   ├── "lineitem.l_orderkey"(#1.0)
            │   ├── "lineitem.l_partkey"(#1.1)
            │   ├── "lineitem.l_quantity"(#1.4)
            │   ├── "lineitem.l_receiptdate"(#1.12)
            │   ├── "lineitem.l_returnflag"(#1.8)
            │   ├── "lineitem.l_shipdate"(#1.10)
            │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   ├── "lineitem.l_shipmode"(#1.14)
            │   ├── "lineitem.l_suppkey"(#1.2)
            │   ├── "lineitem.l_tax"(#1.7)
            │   ├── "part.p_brand"(#2.3)
            │   ├── "part.p_comment"(#2.8)
            │   ├── "part.p_container"(#2.6)
            │   ├── "part.p_mfgr"(#2.2)
            │   ├── "part.p_name"(#2.1)
            │   ├── "part.p_partkey"(#2.0)
            │   ├── "part.p_retailprice"(#2.7)
            │   ├── "part.p_size"(#2.5)
            │   └── "part.p_type"(#2.4)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: "lineitem.l_partkey"(#1.1) = "part.p_partkey"(#2.0)
            │   ├── (.output_columns):
            │   │   ┌── "lineitem.l_comment"(#1.15)
            │   │   ├── "lineitem.l_commitdate"(#1.11)
            │   │   ├── "lineitem.l_discount"(#1.6)
            │   │   ├── "lineitem.l_extendedprice"(#1.5)
            │   │   ├── "lineitem.l_linenumber"(#1.3)
            │   │   ├── "lineitem.l_linestatus"(#1.9)
            │   │   ├── "lineitem.l_orderkey"(#1.0)
            │   │   ├── "lineitem.l_partkey"(#1.1)
            │   │   ├── "lineitem.l_quantity"(#1.4)
            │   │   ├── "lineitem.l_receiptdate"(#1.12)
            │   │   ├── "lineitem.l_returnflag"(#1.8)
            │   │   ├── "lineitem.l_shipdate"(#1.10)
            │   │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   │   ├── "lineitem.l_shipmode"(#1.14)
            │   │   ├── "lineitem.l_suppkey"(#1.2)
            │   │   ├── "lineitem.l_tax"(#1.7)
            │   │   ├── "part.p_brand"(#2.3)
            │   │   ├── "part.p_comment"(#2.8)
            │   │   ├── "part.p_container"(#2.6)
            │   │   ├── "part.p_mfgr"(#2.2)
            │   │   ├── "part.p_name"(#2.1)
            │   │   ├── "part.p_partkey"(#2.0)
            │   │   ├── "part.p_retailprice"(#2.7)
            │   │   ├── "part.p_size"(#2.5)
            │   │   └── "part.p_type"(#2.4)
            │   ├── (.cardinality): 0.00
            │   ├── Get
            │   │   ├── .data_source_id: 8
            │   │   ├── .table_index: 1
            │   │   ├── .implementation: None
            │   │   ├── (.output_columns):
            │   │   │   ┌── "lineitem.l_comment"(#1.15)
            │   │   │   ├── "lineitem.l_commitdate"(#1.11)
            │   │   │   ├── "lineitem.l_discount"(#1.6)
            │   │   │   ├── "lineitem.l_extendedprice"(#1.5)
            │   │   │   ├── "lineitem.l_linenumber"(#1.3)
            │   │   │   ├── "lineitem.l_linestatus"(#1.9)
            │   │   │   ├── "lineitem.l_orderkey"(#1.0)
            │   │   │   ├── "lineitem.l_partkey"(#1.1)
            │   │   │   ├── "lineitem.l_quantity"(#1.4)
            │   │   │   ├── "lineitem.l_receiptdate"(#1.12)
            │   │   │   ├── "lineitem.l_returnflag"(#1.8)
            │   │   │   ├── "lineitem.l_shipdate"(#1.10)
            │   │   │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   │   │   ├── "lineitem.l_shipmode"(#1.14)
            │   │   │   ├── "lineitem.l_suppkey"(#1.2)
            │   │   │   └── "lineitem.l_tax"(#1.7)
            │   │   └── (.cardinality): 0.00
            │   └── Select
            │       ├── .predicate: ("part.p_brand"(#2.3) = 'Brand#13'::utf8_view) AND ("part.p_container"(#2.6) = 'JUMBO PKG'::utf8_view)
            │       ├── (.output_columns):
            │       │   ┌── "part.p_brand"(#2.3)
            │       │   ├── "part.p_comment"(#2.8)
            │       │   ├── "part.p_container"(#2.6)
            │       │   ├── "part.p_mfgr"(#2.2)
            │       │   ├── "part.p_name"(#2.1)
            │       │   ├── "part.p_partkey"(#2.0)
            │       │   ├── "part.p_retailprice"(#2.7)
            │       │   ├── "part.p_size"(#2.5)
            │       │   └── "part.p_type"(#2.4)
            │       ├── (.cardinality): 0.00
            │       └── Get
            │           ├── .data_source_id: 3
            │           ├── .table_index: 2
            │           ├── .implementation: None
            │           ├── (.output_columns):
            │           │   ┌── "part.p_brand"(#2.3)
            │           │   ├── "part.p_comment"(#2.8)
            │           │   ├── "part.p_container"(#2.6)
            │           │   ├── "part.p_mfgr"(#2.2)
            │           │   ├── "part.p_name"(#2.1)
            │           │   ├── "part.p_partkey"(#2.0)
            │           │   ├── "part.p_retailprice"(#2.7)
            │           │   ├── "part.p_size"(#2.5)
            │           │   └── "part.p_type"(#2.4)
            │           └── (.cardinality): 0.00
            └── Remap
                ├── .table_index: 8
                ├── (.output_columns): [ "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0), "__scalar_sq_1.__always_true"(#8.2), "__scalar_sq_1.l_partkey"(#8.1) ]
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 7
                    ├── .projections: [ CAST (0.2::float64 * CAST ("__#5.avg(lineitem.l_quantity)"(#5.0) AS Float64) AS Decimal128(30, 15)), "lineitem.l_partkey"(#1.1), true::boolean ]
                    ├── (.output_columns): [ "__#7.Float64(0.2) * avg(lineitem.l_quantity)"(#7.0), "__#7.__always_true"(#7.2), "__#7.l_partkey"(#7.1) ]
                    ├── (.cardinality): 0.00
                    └── Aggregate
                        ├── .key_table_index: 4
                        ├── .aggregate_table_index: 5
                        ├── .implementation: None
                        ├── .exprs: avg("lineitem.l_quantity"(#3.4))
                        ├── .keys: "lineitem.l_partkey"(#3.1)
                        ├── (.output_columns): [ "__#4.l_partkey"(#4.0), "__#5.avg(lineitem.l_quantity)"(#5.0) ]
                        ├── (.cardinality): 0.00
                        └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): [ "lineitem.l_partkey"(#3.1), "lineitem.l_quantity"(#3.4) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
Project { .table_index: 13, .projections: round(CAST ("__#12.sum(lineitem.l_extendedprice)"(#12.0) AS Float64) / 7::float64, 16::integer), (.output_columns): "__#13.avg_yearly"(#13.0), (.cardinality): 1.00 }
└── Aggregate
    ├── .key_table_index: 11
    ├── .aggregate_table_index: 12
    ├── .implementation: None
    ├── .exprs: sum("__#10.l_extendedprice"(#10.5))
    ├── .keys: []
    ├── (.output_columns): "__#12.sum(lineitem.l_extendedprice)"(#12.0)
    ├── (.cardinality): 1.00
    └── Project
        ├── .table_index: 10
        ├── .projections:
        │   ┌── "lineitem.l_orderkey"(#1.0)
        │   ├── "lineitem.l_partkey"(#1.1)
        │   ├── "lineitem.l_suppkey"(#1.2)
        │   ├── "lineitem.l_linenumber"(#1.3)
        │   ├── "lineitem.l_quantity"(#1.4)
        │   ├── "lineitem.l_extendedprice"(#1.5)
        │   ├── "lineitem.l_discount"(#1.6)
        │   ├── "lineitem.l_tax"(#1.7)
        │   ├── "lineitem.l_returnflag"(#1.8)
        │   ├── "lineitem.l_linestatus"(#1.9)
        │   ├── "lineitem.l_shipdate"(#1.10)
        │   ├── "lineitem.l_commitdate"(#1.11)
        │   ├── "lineitem.l_receiptdate"(#1.12)
        │   ├── "lineitem.l_shipinstruct"(#1.13)
        │   ├── "lineitem.l_shipmode"(#1.14)
        │   ├── "lineitem.l_comment"(#1.15)
        │   ├── "part.p_partkey"(#2.0)
        │   ├── "part.p_name"(#2.1)
        │   ├── "part.p_mfgr"(#2.2)
        │   ├── "part.p_brand"(#2.3)
        │   ├── "part.p_type"(#2.4)
        │   ├── "part.p_size"(#2.5)
        │   ├── "part.p_container"(#2.6)
        │   ├── "part.p_retailprice"(#2.7)
        │   └── "part.p_comment"(#2.8)
        ├── (.output_columns):
        │   ┌── "__#10.l_comment"(#10.15)
        │   ├── "__#10.l_commitdate"(#10.11)
        │   ├── "__#10.l_discount"(#10.6)
        │   ├── "__#10.l_extendedprice"(#10.5)
        │   ├── "__#10.l_linenumber"(#10.3)
        │   ├── "__#10.l_linestatus"(#10.9)
        │   ├── "__#10.l_orderkey"(#10.0)
        │   ├── "__#10.l_partkey"(#10.1)
        │   ├── "__#10.l_quantity"(#10.4)
        │   ├── "__#10.l_receiptdate"(#10.12)
        │   ├── "__#10.l_returnflag"(#10.8)
        │   ├── "__#10.l_shipdate"(#10.10)
        │   ├── "__#10.l_shipinstruct"(#10.13)
        │   ├── "__#10.l_shipmode"(#10.14)
        │   ├── "__#10.l_suppkey"(#10.2)
        │   ├── "__#10.l_tax"(#10.7)
        │   ├── "__#10.p_brand"(#10.19)
        │   ├── "__#10.p_comment"(#10.24)
        │   ├── "__#10.p_container"(#10.22)
        │   ├── "__#10.p_mfgr"(#10.18)
        │   ├── "__#10.p_name"(#10.17)
        │   ├── "__#10.p_partkey"(#10.16)
        │   ├── "__#10.p_retailprice"(#10.23)
        │   ├── "__#10.p_size"(#10.21)
        │   └── "__#10.p_type"(#10.20)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("part.p_partkey"(#2.0) = "__scalar_sq_1.l_partkey"(#8.1)) AND (CAST ("lineitem.l_quantity"(#1.4) AS Decimal128(30, 15)) < "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0))
            ├── (.output_columns):
            │   ┌── "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0)
            │   ├── "__scalar_sq_1.__always_true"(#8.2)
            │   ├── "__scalar_sq_1.l_partkey"(#8.1)
            │   ├── "lineitem.l_comment"(#1.15)
            │   ├── "lineitem.l_commitdate"(#1.11)
            │   ├── "lineitem.l_discount"(#1.6)
            │   ├── "lineitem.l_extendedprice"(#1.5)
            │   ├── "lineitem.l_linenumber"(#1.3)
            │   ├── "lineitem.l_linestatus"(#1.9)
            │   ├── "lineitem.l_orderkey"(#1.0)
            │   ├── "lineitem.l_partkey"(#1.1)
            │   ├── "lineitem.l_quantity"(#1.4)
            │   ├── "lineitem.l_receiptdate"(#1.12)
            │   ├── "lineitem.l_returnflag"(#1.8)
            │   ├── "lineitem.l_shipdate"(#1.10)
            │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   ├── "lineitem.l_shipmode"(#1.14)
            │   ├── "lineitem.l_suppkey"(#1.2)
            │   ├── "lineitem.l_tax"(#1.7)
            │   ├── "part.p_brand"(#2.3)
            │   ├── "part.p_comment"(#2.8)
            │   ├── "part.p_container"(#2.6)
            │   ├── "part.p_mfgr"(#2.2)
            │   ├── "part.p_name"(#2.1)
            │   ├── "part.p_partkey"(#2.0)
            │   ├── "part.p_retailprice"(#2.7)
            │   ├── "part.p_size"(#2.5)
            │   └── "part.p_type"(#2.4)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: "lineitem.l_partkey"(#1.1) = "part.p_partkey"(#2.0)
            │   ├── (.output_columns):
            │   │   ┌── "lineitem.l_comment"(#1.15)
            │   │   ├── "lineitem.l_commitdate"(#1.11)
            │   │   ├── "lineitem.l_discount"(#1.6)
            │   │   ├── "lineitem.l_extendedprice"(#1.5)
            │   │   ├── "lineitem.l_linenumber"(#1.3)
            │   │   ├── "lineitem.l_linestatus"(#1.9)
            │   │   ├── "lineitem.l_orderkey"(#1.0)
            │   │   ├── "lineitem.l_partkey"(#1.1)
            │   │   ├── "lineitem.l_quantity"(#1.4)
            │   │   ├── "lineitem.l_receiptdate"(#1.12)
            │   │   ├── "lineitem.l_returnflag"(#1.8)
            │   │   ├── "lineitem.l_shipdate"(#1.10)
            │   │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   │   ├── "lineitem.l_shipmode"(#1.14)
            │   │   ├── "lineitem.l_suppkey"(#1.2)
            │   │   ├── "lineitem.l_tax"(#1.7)
            │   │   ├── "part.p_brand"(#2.3)
            │   │   ├── "part.p_comment"(#2.8)
            │   │   ├── "part.p_container"(#2.6)
            │   │   ├── "part.p_mfgr"(#2.2)
            │   │   ├── "part.p_name"(#2.1)
            │   │   ├── "part.p_partkey"(#2.0)
            │   │   ├── "part.p_retailprice"(#2.7)
            │   │   ├── "part.p_size"(#2.5)
            │   │   └── "part.p_type"(#2.4)
            │   ├── (.cardinality): 0.00
            │   ├── Get
            │   │   ├── .data_source_id: 8
            │   │   ├── .table_index: 1
            │   │   ├── .implementation: None
            │   │   ├── (.output_columns):
            │   │   │   ┌── "lineitem.l_comment"(#1.15)
            │   │   │   ├── "lineitem.l_commitdate"(#1.11)
            │   │   │   ├── "lineitem.l_discount"(#1.6)
            │   │   │   ├── "lineitem.l_extendedprice"(#1.5)
            │   │   │   ├── "lineitem.l_linenumber"(#1.3)
            │   │   │   ├── "lineitem.l_linestatus"(#1.9)
            │   │   │   ├── "lineitem.l_orderkey"(#1.0)
            │   │   │   ├── "lineitem.l_partkey"(#1.1)
            │   │   │   ├── "lineitem.l_quantity"(#1.4)
            │   │   │   ├── "lineitem.l_receiptdate"(#1.12)
            │   │   │   ├── "lineitem.l_returnflag"(#1.8)
            │   │   │   ├── "lineitem.l_shipdate"(#1.10)
            │   │   │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   │   │   ├── "lineitem.l_shipmode"(#1.14)
            │   │   │   ├── "lineitem.l_suppkey"(#1.2)
            │   │   │   └── "lineitem.l_tax"(#1.7)
            │   │   └── (.cardinality): 0.00
            │   └── Select
            │       ├── .predicate: ("part.p_brand"(#2.3) = 'Brand#13'::utf8_view) AND ("part.p_container"(#2.6) = 'JUMBO PKG'::utf8_view)
            │       ├── (.output_columns):
            │       │   ┌── "part.p_brand"(#2.3)
            │       │   ├── "part.p_comment"(#2.8)
            │       │   ├── "part.p_container"(#2.6)
            │       │   ├── "part.p_mfgr"(#2.2)
            │       │   ├── "part.p_name"(#2.1)
            │       │   ├── "part.p_partkey"(#2.0)
            │       │   ├── "part.p_retailprice"(#2.7)
            │       │   ├── "part.p_size"(#2.5)
            │       │   └── "part.p_type"(#2.4)
            │       ├── (.cardinality): 0.00
            │       └── Get
            │           ├── .data_source_id: 3
            │           ├── .table_index: 2
            │           ├── .implementation: None
            │           ├── (.output_columns):
            │           │   ┌── "part.p_brand"(#2.3)
            │           │   ├── "part.p_comment"(#2.8)
            │           │   ├── "part.p_container"(#2.6)
            │           │   ├── "part.p_mfgr"(#2.2)
            │           │   ├── "part.p_name"(#2.1)
            │           │   ├── "part.p_partkey"(#2.0)
            │           │   ├── "part.p_retailprice"(#2.7)
            │           │   ├── "part.p_size"(#2.5)
            │           │   └── "part.p_type"(#2.4)
            │           └── (.cardinality): 0.00
            └── Remap
                ├── .table_index: 8
                ├── (.output_columns): [ "__scalar_sq_1.Float64(0.2) * avg(lineitem.l_quantity)"(#8.0), "__scalar_sq_1.__always_true"(#8.2), "__scalar_sq_1.l_partkey"(#8.1) ]
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 7
                    ├── .projections: [ CAST (0.2::float64 * CAST ("__#5.avg(lineitem.l_quantity)"(#5.0) AS Float64) AS Decimal128(30, 15)), "lineitem.l_partkey"(#1.1), true::boolean ]
                    ├── (.output_columns): [ "__#7.Float64(0.2) * avg(lineitem.l_quantity)"(#7.0), "__#7.__always_true"(#7.2), "__#7.l_partkey"(#7.1) ]
                    ├── (.cardinality): 0.00
                    └── Aggregate
                        ├── .key_table_index: 4
                        ├── .aggregate_table_index: 5
                        ├── .implementation: None
                        ├── .exprs: avg("lineitem.l_quantity"(#3.4))
                        ├── .keys: "lineitem.l_partkey"(#3.1)
                        ├── (.output_columns): [ "__#4.l_partkey"(#4.0), "__#5.avg(lineitem.l_quantity)"(#5.0) ]
                        ├── (.cardinality): 0.00
                        └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): [ "lineitem.l_partkey"(#3.1), "lineitem.l_quantity"(#3.4) ], (.cardinality): 0.00 }

NULL
*/

