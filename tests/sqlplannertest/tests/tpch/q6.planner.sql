-- TPC-H Q6
SELECT
    SUM(l_extendedprice * l_discount) AS revenue_loss
FROM
    lineitem
WHERE
    l_shipdate >= DATE '2023-01-01'
    AND l_shipdate < DATE '2024-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

/*
logical_plan after optd-initial:
Project { .table_index: 4, .projections: "__#3.sum(lineitem.l_extendedprice * lineitem.l_discount)"(#3.0), (.output_columns): "__#4.revenue_loss"(#4.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 2, .aggregate_table_index: 3, .implementation: None, .exprs: sum("lineitem.l_extendedprice"(#1.5) * "lineitem.l_discount"(#1.6)), .keys: [], (.output_columns): "__#3.sum(lineitem.l_extendedprice * lineitem.l_discount)"(#3.0), (.cardinality): 1.00 }
    └── Select
        ├── .predicate: ("lineitem.l_shipdate"(#1.10) >= 2023-01-01::date32) AND ("lineitem.l_shipdate"(#1.10) < 2024-01-01::date32) AND ("lineitem.l_discount"(#1.6) >= 5::decimal128(15, 2)) AND ("lineitem.l_discount"(#1.6) <= 7::decimal128(15, 2)) AND ("lineitem.l_quantity"(#1.4) < 2400::decimal128(15, 2))
        ├── (.output_columns):
        │   ┌── "lineitem.l_comment"(#1.15)
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
        │   └── "lineitem.l_tax"(#1.7)
        ├── (.cardinality): 0.00
        └── Get
            ├── .data_source_id: 8
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "lineitem.l_comment"(#1.15)
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
            │   └── "lineitem.l_tax"(#1.7)
            └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
Project { .table_index: 4, .projections: "__#3.sum(lineitem.l_extendedprice * lineitem.l_discount)"(#3.0), (.output_columns): "__#4.revenue_loss"(#4.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 2, .aggregate_table_index: 3, .implementation: None, .exprs: sum("lineitem.l_extendedprice"(#1.5) * "lineitem.l_discount"(#1.6)), .keys: [], (.output_columns): "__#3.sum(lineitem.l_extendedprice * lineitem.l_discount)"(#3.0), (.cardinality): 1.00 }
    └── Select
        ├── .predicate: ("lineitem.l_shipdate"(#1.10) >= 2023-01-01::date32) AND ("lineitem.l_shipdate"(#1.10) < 2024-01-01::date32) AND ("lineitem.l_discount"(#1.6) >= 5::decimal128(15, 2)) AND ("lineitem.l_discount"(#1.6) <= 7::decimal128(15, 2)) AND ("lineitem.l_quantity"(#1.4) < 2400::decimal128(15, 2))
        ├── (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_quantity"(#1.4), "lineitem.l_shipdate"(#1.10) ]
        ├── (.cardinality): 0.00
        └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_quantity"(#1.4), "lineitem.l_shipdate"(#1.10) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
Project { .table_index: 4, .projections: "__#3.sum(lineitem.l_extendedprice * lineitem.l_discount)"(#3.0), (.output_columns): "__#4.revenue_loss"(#4.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 2, .aggregate_table_index: 3, .implementation: None, .exprs: sum("lineitem.l_extendedprice"(#1.5) * "lineitem.l_discount"(#1.6)), .keys: [], (.output_columns): "__#3.sum(lineitem.l_extendedprice * lineitem.l_discount)"(#3.0), (.cardinality): 1.00 }
    └── Select
        ├── .predicate: ("lineitem.l_shipdate"(#1.10) >= 2023-01-01::date32) AND ("lineitem.l_shipdate"(#1.10) < 2024-01-01::date32) AND ("lineitem.l_discount"(#1.6) >= 5::decimal128(15, 2)) AND ("lineitem.l_discount"(#1.6) <= 7::decimal128(15, 2)) AND ("lineitem.l_quantity"(#1.4) < 2400::decimal128(15, 2))
        ├── (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_quantity"(#1.4), "lineitem.l_shipdate"(#1.10) ]
        ├── (.cardinality): 0.00
        └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_quantity"(#1.4), "lineitem.l_shipdate"(#1.10) ], (.cardinality): 0.00 }

NULL
*/

