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
Project { .table_index: 3, .projections: sum(lineitem.l_extendedprice * lineitem.l_discount)(#2.0), (.output_columns): revenue_loss(#3.0), (.cardinality): 1.00 }
└── Aggregate { .aggregate_table_index: 2, .implementation: None, .exprs: [ sum(l_extendedprice(#1.5) * l_discount(#1.6)) ], .keys: [], (.output_columns): sum(lineitem.l_extendedprice * lineitem.l_discount)(#2.0), (.cardinality): 1.00 }
    └── Select
        ├── .predicate: (l_shipdate(#1.10) >= 2023-01-01::date32) AND (l_shipdate(#1.10) < 2024-01-01::date32) AND (l_discount(#1.6) >= 5::decimal128(15, 2)) AND (l_discount(#1.6) <= 7::decimal128(15, 2)) AND (l_quantity(#1.4) < 2400::decimal128(15, 2))
        ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7)
        ├── (.cardinality): 0.00
        └── Get
            ├── .data_source_id: 8
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7)
            └── (.cardinality): 0.00

physical_plan after optd-finalized:
SAME TEXT AS ABOVE

NULL
*/

