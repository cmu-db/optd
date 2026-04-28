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
Project
├── .table_index: 9
├── .projections: round(CAST ("__#8.sum(lineitem.l_extendedprice)"(#8.0) AS Float64) / 7::float64, CAST (16::bigint AS Int32))
├── (.output_columns): "__#9.avg_yearly"(#9.0)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 7
    ├── .aggregate_table_index: 8
    ├── .implementation: None
    ├── .exprs: sum("lineitem.l_extendedprice"(#1.5))
    ├── .keys: []
    ├── (.output_columns): "__#8.sum(lineitem.l_extendedprice)"(#8.0)
    ├── (.cardinality): 1.00
    └── DependentJoin
        ├── .join_type: Inner
        ├── .join_cond: CAST ("lineitem.l_quantity"(#1.4) AS Decimal128(30, 15)) < "__#6.Float64(0.2) * avg(lineitem.l_quantity)"(#6.0)
        ├── (.output_columns):
        │   ┌── "__#6.Float64(0.2) * avg(lineitem.l_quantity)"(#6.0)
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
        ├── Select
        │   ├── .predicate: ("part.p_partkey"(#2.0) = "lineitem.l_partkey"(#1.1)) AND ("part.p_brand"(#2.3) = CAST ('Brand#13'::utf8 AS Utf8View)) AND ("part.p_container"(#2.6) = CAST ('JUMBO PKG'::utf8 AS Utf8View))
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
        │   └── Join
        │       ├── .join_type: Inner
        │       ├── .implementation: None
        │       ├── .join_cond: 
        │       ├── (.output_columns):
        │       │   ┌── "lineitem.l_comment"(#1.15)
        │       │   ├── "lineitem.l_commitdate"(#1.11)
        │       │   ├── "lineitem.l_discount"(#1.6)
        │       │   ├── "lineitem.l_extendedprice"(#1.5)
        │       │   ├── "lineitem.l_linenumber"(#1.3)
        │       │   ├── "lineitem.l_linestatus"(#1.9)
        │       │   ├── "lineitem.l_orderkey"(#1.0)
        │       │   ├── "lineitem.l_partkey"(#1.1)
        │       │   ├── "lineitem.l_quantity"(#1.4)
        │       │   ├── "lineitem.l_receiptdate"(#1.12)
        │       │   ├── "lineitem.l_returnflag"(#1.8)
        │       │   ├── "lineitem.l_shipdate"(#1.10)
        │       │   ├── "lineitem.l_shipinstruct"(#1.13)
        │       │   ├── "lineitem.l_shipmode"(#1.14)
        │       │   ├── "lineitem.l_suppkey"(#1.2)
        │       │   ├── "lineitem.l_tax"(#1.7)
        │       │   ├── "part.p_brand"(#2.3)
        │       │   ├── "part.p_comment"(#2.8)
        │       │   ├── "part.p_container"(#2.6)
        │       │   ├── "part.p_mfgr"(#2.2)
        │       │   ├── "part.p_name"(#2.1)
        │       │   ├── "part.p_partkey"(#2.0)
        │       │   ├── "part.p_retailprice"(#2.7)
        │       │   ├── "part.p_size"(#2.5)
        │       │   └── "part.p_type"(#2.4)
        │       ├── (.cardinality): 0.00
        │       ├── Get
        │       │   ├── .data_source_id: 8
        │       │   ├── .table_index: 1
        │       │   ├── .implementation: None
        │       │   ├── (.output_columns):
        │       │   │   ┌── "lineitem.l_comment"(#1.15)
        │       │   │   ├── "lineitem.l_commitdate"(#1.11)
        │       │   │   ├── "lineitem.l_discount"(#1.6)
        │       │   │   ├── "lineitem.l_extendedprice"(#1.5)
        │       │   │   ├── "lineitem.l_linenumber"(#1.3)
        │       │   │   ├── "lineitem.l_linestatus"(#1.9)
        │       │   │   ├── "lineitem.l_orderkey"(#1.0)
        │       │   │   ├── "lineitem.l_partkey"(#1.1)
        │       │   │   ├── "lineitem.l_quantity"(#1.4)
        │       │   │   ├── "lineitem.l_receiptdate"(#1.12)
        │       │   │   ├── "lineitem.l_returnflag"(#1.8)
        │       │   │   ├── "lineitem.l_shipdate"(#1.10)
        │       │   │   ├── "lineitem.l_shipinstruct"(#1.13)
        │       │   │   ├── "lineitem.l_shipmode"(#1.14)
        │       │   │   ├── "lineitem.l_suppkey"(#1.2)
        │       │   │   └── "lineitem.l_tax"(#1.7)
        │       │   └── (.cardinality): 0.00
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
        └── Project
            ├── .table_index: 6
            ├── .projections: CAST (0.2::float64 * CAST ("__#5.avg(lineitem.l_quantity)"(#5.0) AS Float64) AS Decimal128(30, 15))
            ├── (.output_columns): "__#6.Float64(0.2) * avg(lineitem.l_quantity)"(#6.0)
            ├── (.cardinality): 1.00
            └── Aggregate
                ├── .key_table_index: 4
                ├── .aggregate_table_index: 5
                ├── .implementation: None
                ├── .exprs: avg("lineitem.l_quantity"(#3.4))
                ├── .keys: []
                ├── (.output_columns): "__#5.avg(lineitem.l_quantity)"(#5.0)
                ├── (.cardinality): 1.00
                └── Select
                    ├── .predicate: "lineitem.l_partkey"(#3.1) = "part.p_partkey"(#2.0)
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

physical_plan after optd-finalized:
Project
├── .table_index: 9
├── .projections: round(CAST ("__#8.sum(lineitem.l_extendedprice)"(#8.0) AS Float64) / 7::float64, 16::integer)
├── (.output_columns): "__#9.avg_yearly"(#9.0)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 7
    ├── .aggregate_table_index: 8
    ├── .implementation: None
    ├── .exprs: sum("lineitem.l_extendedprice"(#1.5))
    ├── .keys: []
    ├── (.output_columns): "__#8.sum(lineitem.l_extendedprice)"(#8.0)
    ├── (.cardinality): 1.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: ("part.p_partkey"(#2.0) IS NOT DISTINCT FROM "__#14.p_partkey"(#14.1)) AND (CAST ("lineitem.l_quantity"(#1.4) AS Decimal128(30, 15)) < "__#14.expr0"(#14.0))
        ├── (.output_columns):
        │   ┌── "__#14.expr0"(#14.0)
        │   ├── "__#14.p_partkey"(#14.1)
        │   ├── "lineitem.l_extendedprice"(#1.5)
        │   ├── "lineitem.l_partkey"(#1.1)
        │   ├── "lineitem.l_quantity"(#1.4)
        │   ├── "part.p_brand"(#2.3)
        │   ├── "part.p_container"(#2.6)
        │   └── "part.p_partkey"(#2.0)
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: "part.p_partkey"(#2.0) = "lineitem.l_partkey"(#1.1)
        │   ├── (.output_columns):
        │   │   ┌── "lineitem.l_extendedprice"(#1.5)
        │   │   ├── "lineitem.l_partkey"(#1.1)
        │   │   ├── "lineitem.l_quantity"(#1.4)
        │   │   ├── "part.p_brand"(#2.3)
        │   │   ├── "part.p_container"(#2.6)
        │   │   └── "part.p_partkey"(#2.0)
        │   ├── (.cardinality): 0.00
        │   ├── Get
        │   │   ├── .data_source_id: 8
        │   │   ├── .table_index: 1
        │   │   ├── .implementation: None
        │   │   ├── (.output_columns): [ "lineitem.l_extendedprice"(#1.5), "lineitem.l_partkey"(#1.1), "lineitem.l_quantity"(#1.4) ]
        │   │   └── (.cardinality): 0.00
        │   └── Select
        │       ├── .predicate: ("part.p_brand"(#2.3) = 'Brand#13'::utf8_view) AND ("part.p_container"(#2.6) = 'JUMBO PKG'::utf8_view)
        │       ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_container"(#2.6), "part.p_partkey"(#2.0) ]
        │       ├── (.cardinality): 0.00
        │       └── Get
        │           ├── .data_source_id: 3
        │           ├── .table_index: 2
        │           ├── .implementation: None
        │           ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_container"(#2.6), "part.p_partkey"(#2.0) ]
        │           └── (.cardinality): 0.00
        └── Project
            ├── .table_index: 14
            ├── .projections: [ CAST (0.2::float64 * CAST ("__#13.avg"(#13.0) AS Float64) AS Decimal128(30, 15)), "__#10.p_partkey"(#10.0) ]
            ├── (.output_columns): [ "__#14.expr0"(#14.0), "__#14.p_partkey"(#14.1) ]
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftOuter
                ├── .implementation: None
                ├── .join_cond: "__#10.p_partkey"(#10.0) IS NOT DISTINCT FROM "__#12.l_partkey"(#12.0)
                ├── (.output_columns): [ "__#10.p_partkey"(#10.0), "__#12.l_partkey"(#12.0), "__#13.avg"(#13.0) ]
                ├── (.cardinality): 0.00
                ├── Aggregate
                │   ├── .key_table_index: 10
                │   ├── .aggregate_table_index: 11
                │   ├── .implementation: None
                │   ├── .exprs: []
                │   ├── .keys: "part.p_partkey"(#2.0)
                │   ├── (.output_columns): "__#10.p_partkey"(#10.0)
                │   ├── (.cardinality): 0.00
                │   └── Join
                │       ├── .join_type: Inner
                │       ├── .implementation: None
                │       ├── .join_cond: "part.p_partkey"(#2.0) = "lineitem.l_partkey"(#1.1)
                │       ├── (.output_columns): [ "lineitem.l_partkey"(#1.1), "part.p_brand"(#2.3), "part.p_container"(#2.6), "part.p_partkey"(#2.0) ]
                │       ├── (.cardinality): 0.00
                │       ├── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): "lineitem.l_partkey"(#1.1), (.cardinality): 0.00 }
                │       └── Select
                │           ├── .predicate: ("part.p_brand"(#2.3) = 'Brand#13'::utf8_view) AND ("part.p_container"(#2.6) = 'JUMBO PKG'::utf8_view)
                │           ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_container"(#2.6), "part.p_partkey"(#2.0) ]
                │           ├── (.cardinality): 0.00
                │           └── Get
                │               ├── .data_source_id: 3
                │               ├── .table_index: 2
                │               ├── .implementation: None
                │               ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_container"(#2.6), "part.p_partkey"(#2.0) ]
                │               └── (.cardinality): 0.00
                └── Aggregate
                    ├── .key_table_index: 12
                    ├── .aggregate_table_index: 13
                    ├── .implementation: None
                    ├── .exprs: avg("lineitem.l_quantity"(#3.4))
                    ├── .keys: "lineitem.l_partkey"(#3.1)
                    ├── (.output_columns): [ "__#12.l_partkey"(#12.0), "__#13.avg"(#13.0) ]
                    ├── (.cardinality): 0.00
                    └── Select
                        ├── .predicate: "lineitem.l_partkey"(#3.1) = "lineitem.l_partkey"(#3.1)
                        ├── (.output_columns): [ "lineitem.l_partkey"(#3.1), "lineitem.l_quantity"(#3.4) ]
                        ├── (.cardinality): 0.00
                        └── Get
                            ├── .data_source_id: 8
                            ├── .table_index: 3
                            ├── .implementation: None
                            ├── (.output_columns): [ "lineitem.l_partkey"(#3.1), "lineitem.l_quantity"(#3.4) ]
                            └── (.cardinality): 0.00

NULL
*/

