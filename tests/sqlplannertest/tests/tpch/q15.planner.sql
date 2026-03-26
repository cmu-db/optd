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
OrderBy { ordering_exprs: [ s_suppkey(#19.0) ASC ], (.output_columns): s_address(#19.2), s_name(#19.1), s_phone(#19.3), s_suppkey(#19.0), total_revenue(#19.4), (.cardinality): 0.00 }
└── Project { .table_index: 19, .projections: [ s_suppkey(#18.0), s_name(#18.1), s_address(#18.2), s_phone(#18.4), total_revenue(#18.8) ], (.output_columns): s_address(#19.2), s_name(#19.1), s_phone(#19.3), s_suppkey(#19.0), total_revenue(#19.4), (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 18
        ├── .projections: [ s_suppkey(#1.0), s_name(#1.1), s_address(#1.2), s_nationkey(#1.3), s_phone(#1.4), s_acctbal(#1.5), s_comment(#1.6), supplier_no(#7.0), total_revenue(#7.1) ]
        ├── (.output_columns): s_acctbal(#18.5), s_address(#18.2), s_comment(#18.6), s_name(#18.1), s_nationkey(#18.3), s_phone(#18.4), s_suppkey(#18.0), supplier_no(#18.7), total_revenue(#18.8)
        ├── (.cardinality): 0.00
        └── Join { .join_type: Inner, .implementation: None, .join_cond: (total_revenue(#7.1) = max(revenue0.total_revenue)(#17.0)), (.output_columns): max(revenue0.total_revenue)(#17.0), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), supplier_no(#7.0), total_revenue(#7.1), (.cardinality): 0.00 }
            ├── Join { .join_type: Inner, .implementation: None, .join_cond: (s_suppkey(#1.0) = supplier_no(#7.0)), (.output_columns): s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), supplier_no(#7.0), total_revenue(#7.1), (.cardinality): 0.00 }
            │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
            │   └── Remap { .table_index: 7, (.output_columns): supplier_no(#7.0), total_revenue(#7.1), (.cardinality): 0.00 }
            │       └── Project { .table_index: 6, .projections: [ l_suppkey(#5.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.1) ], (.output_columns): supplier_no(#6.0), total_revenue(#6.1), (.cardinality): 0.00 }
            │           └── Project { .table_index: 5, .projections: [ l_suppkey(#2.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0) ], (.output_columns): l_suppkey(#5.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.1), (.cardinality): 0.00 }
            │               └── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: sum(l_extendedprice(#2.5) * 1::decimal128(20, 0) - l_discount(#2.6)), .keys: [ l_suppkey(#2.2) ], (.output_columns): lineitem.l_suppkey(#3.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0), (.cardinality): 0.00 }
            │                   └── Select
            │                       ├── .predicate: (l_shipdate(#2.10) >= 1993-01-01::date32) AND (l_shipdate(#2.10) < 1993-04-01::date32)
            │                       ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
            │                       ├── (.cardinality): 0.00
            │                       └── Get
            │                           ├── .data_source_id: 8
            │                           ├── .table_index: 2
            │                           ├── .implementation: None
            │                           ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
            │                           └── (.cardinality): 0.00
            └── Remap { .table_index: 17, (.output_columns): max(revenue0.total_revenue)(#17.0), (.cardinality): 1.00 }
                └── Project { .table_index: 16, .projections: max(revenue0.total_revenue)(#15.0), (.output_columns): max(revenue0.total_revenue)(#16.0), (.cardinality): 1.00 }
                    └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: [ max(total_revenue(#13.1)) ], .keys: [], (.output_columns): max(revenue0.total_revenue)(#15.0), (.cardinality): 1.00 }
                        └── Remap { .table_index: 13, (.output_columns): supplier_no(#13.0), total_revenue(#13.1), (.cardinality): 0.00 }
                            └── Project { .table_index: 12, .projections: [ l_suppkey(#11.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#11.1) ], (.output_columns): supplier_no(#12.0), total_revenue(#12.1), (.cardinality): 0.00 }
                                └── Project { .table_index: 11, .projections: [ l_suppkey(#8.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#10.0) ], (.output_columns): l_suppkey(#11.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#11.1), (.cardinality): 0.00 }
                                    └── Aggregate { .key_table_index: 9, .aggregate_table_index: 10, .implementation: None, .exprs: sum(l_extendedprice(#8.5) * 1::decimal128(20, 0) - l_discount(#8.6)), .keys: [ l_suppkey(#8.2) ], (.output_columns): lineitem.l_suppkey(#9.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#10.0), (.cardinality): 0.00 }
                                        └── Select
                                            ├── .predicate: (l_shipdate(#8.10) >= 1993-01-01::date32) AND (l_shipdate(#8.10) < 1993-04-01::date32)
                                            ├── (.output_columns): l_comment(#8.15), l_commitdate(#8.11), l_discount(#8.6), l_extendedprice(#8.5), l_linenumber(#8.3), l_linestatus(#8.9), l_orderkey(#8.0), l_partkey(#8.1), l_quantity(#8.4), l_receiptdate(#8.12), l_returnflag(#8.8), l_shipdate(#8.10), l_shipinstruct(#8.13), l_shipmode(#8.14), l_suppkey(#8.2), l_tax(#8.7)
                                            ├── (.cardinality): 0.00
                                            └── Get
                                                ├── .data_source_id: 8
                                                ├── .table_index: 8
                                                ├── .implementation: None
                                                ├── (.output_columns): l_comment(#8.15), l_commitdate(#8.11), l_discount(#8.6), l_extendedprice(#8.5), l_linenumber(#8.3), l_linestatus(#8.9), l_orderkey(#8.0), l_partkey(#8.1), l_quantity(#8.4), l_receiptdate(#8.12), l_returnflag(#8.8), l_shipdate(#8.10), l_shipinstruct(#8.13), l_shipmode(#8.14), l_suppkey(#8.2), l_tax(#8.7)
                                                └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#19.0, Asc)]
├── (.output_columns): s_address(#19.2), s_name(#19.1), s_phone(#19.3), s_suppkey(#19.0), total_revenue(#19.4)
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 19
    ├── .projections: [ s_suppkey(#1.0), s_name(#1.1), s_address(#1.2), s_phone(#1.4), total_revenue(#7.1) ]
    ├── (.output_columns): s_address(#19.2), s_name(#19.1), s_phone(#19.3), s_suppkey(#19.0), total_revenue(#19.4)
    ├── (.cardinality): 0.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: total_revenue(#7.1) = max(revenue0.total_revenue)(#17.0)
        ├── (.output_columns): max(revenue0.total_revenue)(#17.0), s_address(#1.2), s_name(#1.1), s_phone(#1.4), s_suppkey(#1.0), supplier_no(#7.0), total_revenue(#7.1)
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: s_suppkey(#1.0) = supplier_no(#7.0)
        │   ├── (.output_columns): s_address(#1.2), s_name(#1.1), s_phone(#1.4), s_suppkey(#1.0), supplier_no(#7.0), total_revenue(#7.1)
        │   ├── (.cardinality): 0.00
        │   ├── Get
        │   │   ├── .data_source_id: 4
        │   │   ├── .table_index: 1
        │   │   ├── .implementation: None
        │   │   ├── (.output_columns): s_address(#1.2), s_name(#1.1), s_phone(#1.4), s_suppkey(#1.0)
        │   │   └── (.cardinality): 0.00
        │   └── Remap { .table_index: 7, (.output_columns): supplier_no(#7.0), total_revenue(#7.1), (.cardinality): 0.00 }
        │       └── Project
        │           ├── .table_index: 6
        │           ├── .projections: [ l_suppkey(#2.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0) ]
        │           ├── (.output_columns): supplier_no(#6.0), total_revenue(#6.1)
        │           ├── (.cardinality): 0.00
        │           └── Aggregate
        │               ├── .key_table_index: 3
        │               ├── .aggregate_table_index: 4
        │               ├── .implementation: None
        │               ├── .exprs: sum(l_extendedprice(#2.5) * 1::decimal128(20, 0) - l_discount(#2.6))
        │               ├── .keys: [ l_suppkey(#2.2) ]
        │               ├── (.output_columns): lineitem.l_suppkey(#3.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0)
        │               ├── (.cardinality): 0.00
        │               └── Select
        │                   ├── .predicate: (l_shipdate(#2.10) >= 1993-01-01::date32) AND (l_shipdate(#2.10) < 1993-04-01::date32)
        │                   ├── (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_shipdate(#2.10), l_suppkey(#2.2)
        │                   ├── (.cardinality): 0.00
        │                   └── Get
        │                       ├── .data_source_id: 8
        │                       ├── .table_index: 2
        │                       ├── .implementation: None
        │                       ├── (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_shipdate(#2.10), l_suppkey(#2.2)
        │                       └── (.cardinality): 0.00
        └── Remap { .table_index: 17, (.output_columns): max(revenue0.total_revenue)(#17.0), (.cardinality): 1.00 }
            └── Project
                ├── .table_index: 16
                ├── .projections: max(revenue0.total_revenue)(#15.0)
                ├── (.output_columns): max(revenue0.total_revenue)(#16.0)
                ├── (.cardinality): 1.00
                └── Aggregate
                    ├── .key_table_index: 14
                    ├── .aggregate_table_index: 15
                    ├── .implementation: None
                    ├── .exprs: [ max(total_revenue(#13.1)) ]
                    ├── .keys: []
                    ├── (.output_columns): max(revenue0.total_revenue)(#15.0)
                    ├── (.cardinality): 1.00
                    └── Remap { .table_index: 13, (.output_columns): supplier_no(#13.0), total_revenue(#13.1), (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 12
                            ├── .projections: [ l_suppkey(#8.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#10.0) ]
                            ├── (.output_columns): supplier_no(#12.0), total_revenue(#12.1)
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 9
                                ├── .aggregate_table_index: 10
                                ├── .implementation: None
                                ├── .exprs: sum(l_extendedprice(#8.5) * 1::decimal128(20, 0) - l_discount(#8.6))
                                ├── .keys: [ l_suppkey(#8.2) ]
                                ├── (.output_columns): lineitem.l_suppkey(#9.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#10.0)
                                ├── (.cardinality): 0.00
                                └── Select
                                    ├── .predicate: (l_shipdate(#8.10) >= 1993-01-01::date32) AND (l_shipdate(#8.10) < 1993-04-01::date32)
                                    ├── (.output_columns): l_discount(#8.6), l_extendedprice(#8.5), l_shipdate(#8.10), l_suppkey(#8.2)
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 8
                                        ├── .implementation: None
                                        ├── (.output_columns): l_discount(#8.6), l_extendedprice(#8.5), l_shipdate(#8.10), l_suppkey(#8.2)
                                        └── (.cardinality): 0.00
*/

