-- (no id or description)
WITH revenue0 (supplier_no, total_revenue) AS 
(
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount)) 
    FROM
        lineitem 
    WHERE
        l_shipdate >= DATE '1993-01-01' 
        AND l_shipdate < DATE '1993-01-01' + INTERVAL '3' MONTH 
    GROUP BY
        l_suppkey 
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue 
FROM
    supplier,
    revenue0 
WHERE
    s_suppkey = supplier_no 
    AND total_revenue = 
    (
        SELECT
            MAX(total_revenue) 
        FROM
            revenue0 
    )
ORDER BY
    s_suppkey;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: "__#17.s_suppkey"(#17.0) ASC, (.output_columns): [ "__#17.s_address"(#17.2), "__#17.s_name"(#17.1), "__#17.s_phone"(#17.3), "__#17.s_suppkey"(#17.0), "__#17.total_revenue"(#17.4) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 17
    ├── .projections: [ "supplier.s_suppkey"(#1.0), "supplier.s_name"(#1.1), "supplier.s_address"(#1.2), "supplier.s_phone"(#1.4), "revenue0.total_revenue"(#7.1) ]
    ├── (.output_columns): [ "__#17.s_address"(#17.2), "__#17.s_name"(#17.1), "__#17.s_phone"(#17.3), "__#17.s_suppkey"(#17.0), "__#17.total_revenue"(#17.4) ]
    ├── (.cardinality): 0.00
    └── DependentJoin
        ├── .join_type: Inner
        ├── .join_cond: "revenue0.total_revenue"(#7.1) = "__#16.max(revenue0.total_revenue)"(#16.0)
        ├── (.output_columns):
        │   ┌── "__#16.max(revenue0.total_revenue)"(#16.0)
        │   ├── "revenue0.supplier_no"(#7.0)
        │   ├── "revenue0.total_revenue"(#7.1)
        │   ├── "supplier.s_acctbal"(#1.5)
        │   ├── "supplier.s_address"(#1.2)
        │   ├── "supplier.s_comment"(#1.6)
        │   ├── "supplier.s_name"(#1.1)
        │   ├── "supplier.s_nationkey"(#1.3)
        │   ├── "supplier.s_phone"(#1.4)
        │   └── "supplier.s_suppkey"(#1.0)
        ├── (.cardinality): 0.00
        ├── Select
        │   ├── .predicate: "supplier.s_suppkey"(#1.0) = "revenue0.supplier_no"(#7.0)
        │   ├── (.output_columns):
        │   │   ┌── "revenue0.supplier_no"(#7.0)
        │   │   ├── "revenue0.total_revenue"(#7.1)
        │   │   ├── "supplier.s_acctbal"(#1.5)
        │   │   ├── "supplier.s_address"(#1.2)
        │   │   ├── "supplier.s_comment"(#1.6)
        │   │   ├── "supplier.s_name"(#1.1)
        │   │   ├── "supplier.s_nationkey"(#1.3)
        │   │   ├── "supplier.s_phone"(#1.4)
        │   │   └── "supplier.s_suppkey"(#1.0)
        │   ├── (.cardinality): 0.00
        │   └── Join
        │       ├── .join_type: Inner
        │       ├── .implementation: None
        │       ├── .join_cond: 
        │       ├── (.output_columns):
        │       │   ┌── "revenue0.supplier_no"(#7.0)
        │       │   ├── "revenue0.total_revenue"(#7.1)
        │       │   ├── "supplier.s_acctbal"(#1.5)
        │       │   ├── "supplier.s_address"(#1.2)
        │       │   ├── "supplier.s_comment"(#1.6)
        │       │   ├── "supplier.s_name"(#1.1)
        │       │   ├── "supplier.s_nationkey"(#1.3)
        │       │   ├── "supplier.s_phone"(#1.4)
        │       │   └── "supplier.s_suppkey"(#1.0)
        │       ├── (.cardinality): 0.00
        │       ├── Get
        │       │   ├── .data_source_id: 4
        │       │   ├── .table_index: 1
        │       │   ├── .implementation: None
        │       │   ├── (.output_columns): [ "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ]
        │       │   └── (.cardinality): 0.00
        │       └── Remap { .table_index: 7, (.output_columns): [ "revenue0.supplier_no"(#7.0), "revenue0.total_revenue"(#7.1) ], (.cardinality): 0.00 }
        │           └── Project { .table_index: 6, .projections: [ "__#5.l_suppkey"(#5.0), "__#5.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#5.1) ], (.output_columns): [ "__#6.supplier_no"(#6.0), "__#6.total_revenue"(#6.1) ], (.cardinality): 0.00 }
        │               └── Project
        │                   ├── .table_index: 5
        │                   ├── .projections: [ "lineitem.l_suppkey"(#2.2), "__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.0) ]
        │                   ├── (.output_columns): [ "__#5.l_suppkey"(#5.0), "__#5.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#5.1) ]
        │                   ├── (.cardinality): 0.00
        │                   └── Aggregate
        │                       ├── .key_table_index: 3
        │                       ├── .aggregate_table_index: 4
        │                       ├── .implementation: None
        │                       ├── .exprs: sum("lineitem.l_extendedprice"(#2.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#2.6))
        │                       ├── .keys: "lineitem.l_suppkey"(#2.2)
        │                       ├── (.output_columns): [ "__#3.l_suppkey"(#3.0), "__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.0) ]
        │                       ├── (.cardinality): 0.00
        │                       └── Select
        │                           ├── .predicate: ("lineitem.l_shipdate"(#2.10) >= CAST ('1993-01-01'::utf8 AS Date32)) AND ("lineitem.l_shipdate"(#2.10) < CAST ('1993-01-01'::utf8 AS Date32) + IntervalMonthDayNano { months: 3, days: 0, nanoseconds: 0 }::interval_month_day_nano)
        │                           ├── (.output_columns):
        │                           │   ┌── "lineitem.l_comment"(#2.15)
        │                           │   ├── "lineitem.l_commitdate"(#2.11)
        │                           │   ├── "lineitem.l_discount"(#2.6)
        │                           │   ├── "lineitem.l_extendedprice"(#2.5)
        │                           │   ├── "lineitem.l_linenumber"(#2.3)
        │                           │   ├── "lineitem.l_linestatus"(#2.9)
        │                           │   ├── "lineitem.l_orderkey"(#2.0)
        │                           │   ├── "lineitem.l_partkey"(#2.1)
        │                           │   ├── "lineitem.l_quantity"(#2.4)
        │                           │   ├── "lineitem.l_receiptdate"(#2.12)
        │                           │   ├── "lineitem.l_returnflag"(#2.8)
        │                           │   ├── "lineitem.l_shipdate"(#2.10)
        │                           │   ├── "lineitem.l_shipinstruct"(#2.13)
        │                           │   ├── "lineitem.l_shipmode"(#2.14)
        │                           │   ├── "lineitem.l_suppkey"(#2.2)
        │                           │   └── "lineitem.l_tax"(#2.7)
        │                           ├── (.cardinality): 0.00
        │                           └── Get
        │                               ├── .data_source_id: 8
        │                               ├── .table_index: 2
        │                               ├── .implementation: None
        │                               ├── (.output_columns):
        │                               │   ┌── "lineitem.l_comment"(#2.15)
        │                               │   ├── "lineitem.l_commitdate"(#2.11)
        │                               │   ├── "lineitem.l_discount"(#2.6)
        │                               │   ├── "lineitem.l_extendedprice"(#2.5)
        │                               │   ├── "lineitem.l_linenumber"(#2.3)
        │                               │   ├── "lineitem.l_linestatus"(#2.9)
        │                               │   ├── "lineitem.l_orderkey"(#2.0)
        │                               │   ├── "lineitem.l_partkey"(#2.1)
        │                               │   ├── "lineitem.l_quantity"(#2.4)
        │                               │   ├── "lineitem.l_receiptdate"(#2.12)
        │                               │   ├── "lineitem.l_returnflag"(#2.8)
        │                               │   ├── "lineitem.l_shipdate"(#2.10)
        │                               │   ├── "lineitem.l_shipinstruct"(#2.13)
        │                               │   ├── "lineitem.l_shipmode"(#2.14)
        │                               │   ├── "lineitem.l_suppkey"(#2.2)
        │                               │   └── "lineitem.l_tax"(#2.7)
        │                               └── (.cardinality): 0.00
        └── Project { .table_index: 16, .projections: "__#15.max(revenue0.total_revenue)"(#15.0), (.output_columns): "__#16.max(revenue0.total_revenue)"(#16.0), (.cardinality): 1.00 }
            └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: max("revenue0.total_revenue"(#13.1)), .keys: [], (.output_columns): "__#15.max(revenue0.total_revenue)"(#15.0), (.cardinality): 1.00 }
                └── Remap { .table_index: 13, (.output_columns): [ "revenue0.supplier_no"(#13.0), "revenue0.total_revenue"(#13.1) ], (.cardinality): 0.00 }
                    └── Project { .table_index: 12, .projections: [ "__#11.l_suppkey"(#11.0), "__#11.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#11.1) ], (.output_columns): [ "__#12.supplier_no"(#12.0), "__#12.total_revenue"(#12.1) ], (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 11
                            ├── .projections: [ "lineitem.l_suppkey"(#8.2), "__#10.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#10.0) ]
                            ├── (.output_columns): [ "__#11.l_suppkey"(#11.0), "__#11.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#11.1) ]
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 9
                                ├── .aggregate_table_index: 10
                                ├── .implementation: None
                                ├── .exprs: sum("lineitem.l_extendedprice"(#8.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#8.6))
                                ├── .keys: "lineitem.l_suppkey"(#8.2)
                                ├── (.output_columns): [ "__#10.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#10.0), "__#9.l_suppkey"(#9.0) ]
                                ├── (.cardinality): 0.00
                                └── Select
                                    ├── .predicate: ("lineitem.l_shipdate"(#8.10) >= CAST ('1993-01-01'::utf8 AS Date32)) AND ("lineitem.l_shipdate"(#8.10) < CAST ('1993-01-01'::utf8 AS Date32) + IntervalMonthDayNano { months: 3, days: 0, nanoseconds: 0 }::interval_month_day_nano)
                                    ├── (.output_columns):
                                    │   ┌── "lineitem.l_comment"(#8.15)
                                    │   ├── "lineitem.l_commitdate"(#8.11)
                                    │   ├── "lineitem.l_discount"(#8.6)
                                    │   ├── "lineitem.l_extendedprice"(#8.5)
                                    │   ├── "lineitem.l_linenumber"(#8.3)
                                    │   ├── "lineitem.l_linestatus"(#8.9)
                                    │   ├── "lineitem.l_orderkey"(#8.0)
                                    │   ├── "lineitem.l_partkey"(#8.1)
                                    │   ├── "lineitem.l_quantity"(#8.4)
                                    │   ├── "lineitem.l_receiptdate"(#8.12)
                                    │   ├── "lineitem.l_returnflag"(#8.8)
                                    │   ├── "lineitem.l_shipdate"(#8.10)
                                    │   ├── "lineitem.l_shipinstruct"(#8.13)
                                    │   ├── "lineitem.l_shipmode"(#8.14)
                                    │   ├── "lineitem.l_suppkey"(#8.2)
                                    │   └── "lineitem.l_tax"(#8.7)
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 8
                                        ├── .implementation: None
                                        ├── (.output_columns):
                                        │   ┌── "lineitem.l_comment"(#8.15)
                                        │   ├── "lineitem.l_commitdate"(#8.11)
                                        │   ├── "lineitem.l_discount"(#8.6)
                                        │   ├── "lineitem.l_extendedprice"(#8.5)
                                        │   ├── "lineitem.l_linenumber"(#8.3)
                                        │   ├── "lineitem.l_linestatus"(#8.9)
                                        │   ├── "lineitem.l_orderkey"(#8.0)
                                        │   ├── "lineitem.l_partkey"(#8.1)
                                        │   ├── "lineitem.l_quantity"(#8.4)
                                        │   ├── "lineitem.l_receiptdate"(#8.12)
                                        │   ├── "lineitem.l_returnflag"(#8.8)
                                        │   ├── "lineitem.l_shipdate"(#8.10)
                                        │   ├── "lineitem.l_shipinstruct"(#8.13)
                                        │   ├── "lineitem.l_shipmode"(#8.14)
                                        │   ├── "lineitem.l_suppkey"(#8.2)
                                        │   └── "lineitem.l_tax"(#8.7)
                                        └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#17.0, Asc)]
├── (.output_columns): [ "__#17.s_address"(#17.2), "__#17.s_name"(#17.1), "__#17.s_phone"(#17.3), "__#17.s_suppkey"(#17.0), "__#17.total_revenue"(#17.4) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 17
    ├── .projections:
    │   ┌── "supplier.s_suppkey"(#1.0)
    │   ├── "supplier.s_name"(#1.1)
    │   ├── "supplier.s_address"(#1.2)
    │   ├── "supplier.s_phone"(#1.4)
    │   └── "revenue0.total_revenue"(#7.1)
    ├── (.output_columns):
    │   ┌── "__#17.s_address"(#17.2)
    │   ├── "__#17.s_name"(#17.1)
    │   ├── "__#17.s_phone"(#17.3)
    │   ├── "__#17.s_suppkey"(#17.0)
    │   └── "__#17.total_revenue"(#17.4)
    ├── (.cardinality): 0.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: "revenue0.total_revenue"(#7.1) = "__#25.max"(#25.0)
        ├── (.output_columns):
        │   ┌── "__#25.max"(#25.0)
        │   ├── "revenue0.supplier_no"(#7.0)
        │   ├── "revenue0.total_revenue"(#7.1)
        │   ├── "supplier.s_address"(#1.2)
        │   ├── "supplier.s_name"(#1.1)
        │   ├── "supplier.s_phone"(#1.4)
        │   └── "supplier.s_suppkey"(#1.0)
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: "supplier.s_suppkey"(#1.0) = "revenue0.supplier_no"(#7.0)
        │   ├── (.output_columns):
        │   │   ┌── "revenue0.supplier_no"(#7.0)
        │   │   ├── "revenue0.total_revenue"(#7.1)
        │   │   ├── "supplier.s_address"(#1.2)
        │   │   ├── "supplier.s_name"(#1.1)
        │   │   ├── "supplier.s_phone"(#1.4)
        │   │   └── "supplier.s_suppkey"(#1.0)
        │   ├── (.cardinality): 0.00
        │   ├── Get
        │   │   ├── .data_source_id: 4
        │   │   ├── .table_index: 1
        │   │   ├── .implementation: None
        │   │   ├── (.output_columns): [ "supplier.s_address"(#1.2), "supplier.s_name"(#1.1), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ]
        │   │   └── (.cardinality): 0.00
        │   └── Remap { .table_index: 7, (.output_columns): [ "revenue0.supplier_no"(#7.0), "revenue0.total_revenue"(#7.1) ], (.cardinality): 0.00 }
        │       └── Project
        │           ├── .table_index: 6
        │           ├── .projections: [ "lineitem.l_suppkey"(#2.2), "__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.0) ]
        │           ├── (.output_columns): [ "__#6.supplier_no"(#6.0), "__#6.total_revenue"(#6.1) ]
        │           ├── (.cardinality): 0.00
        │           └── Aggregate
        │               ├── .key_table_index: 3
        │               ├── .aggregate_table_index: 4
        │               ├── .implementation: None
        │               ├── .exprs: sum("lineitem.l_extendedprice"(#2.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#2.6))
        │               ├── .keys: "lineitem.l_suppkey"(#2.2)
        │               ├── (.output_columns): [ "__#3.l_suppkey"(#3.0), "__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.0) ]
        │               ├── (.cardinality): 0.00
        │               └── Select
        │                   ├── .predicate: ("lineitem.l_shipdate"(#2.10) >= 1993-01-01::date32) AND ("lineitem.l_shipdate"(#2.10) < 1993-04-01::date32)
        │                   ├── (.output_columns):
        │                   │   ┌── "lineitem.l_discount"(#2.6)
        │                   │   ├── "lineitem.l_extendedprice"(#2.5)
        │                   │   ├── "lineitem.l_shipdate"(#2.10)
        │                   │   └── "lineitem.l_suppkey"(#2.2)
        │                   ├── (.cardinality): 0.00
        │                   └── Get
        │                       ├── .data_source_id: 8
        │                       ├── .table_index: 2
        │                       ├── .implementation: None
        │                       ├── (.output_columns):
        │                       │   ┌── "lineitem.l_discount"(#2.6)
        │                       │   ├── "lineitem.l_extendedprice"(#2.5)
        │                       │   ├── "lineitem.l_shipdate"(#2.10)
        │                       │   └── "lineitem.l_suppkey"(#2.2)
        │                       └── (.cardinality): 0.00
        └── Project { .table_index: 25, .projections: "__#24.max"(#24.0), (.output_columns): "__#25.max"(#25.0), (.cardinality): 1.00 }
            └── Aggregate
                ├── .key_table_index: 23
                ├── .aggregate_table_index: 24
                ├── .implementation: None
                ├── .exprs: max("__#22.total_revenue"(#22.1))
                ├── .keys: []
                ├── (.output_columns): "__#24.max"(#24.0)
                ├── (.cardinality): 1.00
                └── Remap { .table_index: 22, (.output_columns): [ "__#22.supplier_no"(#22.0), "__#22.total_revenue"(#22.1) ], (.cardinality): 0.00 }
                    └── Project
                        ├── .table_index: 21
                        ├── .projections: [ "lineitem.l_suppkey"(#8.2), "__#10.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#10.0) ]
                        ├── (.output_columns): [ "__#21.l_suppkey"(#21.0), "__#21.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#21.1) ]
                        ├── (.cardinality): 0.00
                        └── Aggregate
                            ├── .key_table_index: 9
                            ├── .aggregate_table_index: 10
                            ├── .implementation: None
                            ├── .exprs: sum("lineitem.l_extendedprice"(#8.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#8.6))
                            ├── .keys: "lineitem.l_suppkey"(#8.2)
                            ├── (.output_columns): [ "__#10.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#10.0), "__#9.l_suppkey"(#9.0) ]
                            ├── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: ("lineitem.l_shipdate"(#8.10) >= 1993-01-01::date32) AND ("lineitem.l_shipdate"(#8.10) < 1993-04-01::date32)
                                ├── (.output_columns):
                                │   ┌── "lineitem.l_discount"(#8.6)
                                │   ├── "lineitem.l_extendedprice"(#8.5)
                                │   ├── "lineitem.l_shipdate"(#8.10)
                                │   └── "lineitem.l_suppkey"(#8.2)
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 8
                                    ├── .table_index: 8
                                    ├── .implementation: None
                                    ├── (.output_columns):
                                    │   ┌── "lineitem.l_discount"(#8.6)
                                    │   ├── "lineitem.l_extendedprice"(#8.5)
                                    │   ├── "lineitem.l_shipdate"(#8.10)
                                    │   └── "lineitem.l_suppkey"(#8.2)
                                    └── (.cardinality): 0.00
*/

