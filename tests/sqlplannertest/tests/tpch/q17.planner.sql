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
Project { .table_index: 13, .projections: round(CAST (sum(lineitem.l_extendedprice)(#12.0) AS Float64) / 7::float64, 16::integer), (.output_columns): avg_yearly(#13.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: [ sum(l_extendedprice(#10.5)) ], .keys: [], (.output_columns): sum(lineitem.l_extendedprice)(#12.0), (.cardinality): 1.00 }
    └── Project
        ├── .table_index: 10
        ├── .projections: [ l_orderkey(#9.0), l_partkey(#9.1), l_suppkey(#9.2), l_linenumber(#9.3), l_quantity(#9.4), l_extendedprice(#9.5), l_discount(#9.6), l_tax(#9.7), l_returnflag(#9.8), l_linestatus(#9.9), l_shipdate(#9.10), l_commitdate(#9.11), l_receiptdate(#9.12), l_shipinstruct(#9.13), l_shipmode(#9.14), l_comment(#9.15), p_partkey(#9.16), p_name(#9.17), p_mfgr(#9.18), p_brand(#9.19), p_type(#9.20), p_size(#9.21), p_container(#9.22), p_retailprice(#9.23), p_comment(#9.24) ]
        ├── (.output_columns): l_comment(#10.15), l_commitdate(#10.11), l_discount(#10.6), l_extendedprice(#10.5), l_linenumber(#10.3), l_linestatus(#10.9), l_orderkey(#10.0), l_partkey(#10.1), l_quantity(#10.4), l_receiptdate(#10.12), l_returnflag(#10.8), l_shipdate(#10.10), l_shipinstruct(#10.13), l_shipmode(#10.14), l_suppkey(#10.2), l_tax(#10.7), p_brand(#10.19), p_comment(#10.24), p_container(#10.22), p_mfgr(#10.18), p_name(#10.17), p_partkey(#10.16), p_retailprice(#10.23), p_size(#10.21), p_type(#10.20)
        ├── (.cardinality): 0.00
        └── Project
            ├── .table_index: 9
            ├── .projections: [ l_orderkey(#1.0), l_partkey(#1.1), l_suppkey(#1.2), l_linenumber(#1.3), l_quantity(#1.4), l_extendedprice(#1.5), l_discount(#1.6), l_tax(#1.7), l_returnflag(#1.8), l_linestatus(#1.9), l_shipdate(#1.10), l_commitdate(#1.11), l_receiptdate(#1.12), l_shipinstruct(#1.13), l_shipmode(#1.14), l_comment(#1.15), p_partkey(#2.0), p_name(#2.1), p_mfgr(#2.2), p_brand(#2.3), p_type(#2.4), p_size(#2.5), p_container(#2.6), p_retailprice(#2.7), p_comment(#2.8), Float64(0.2) * avg(lineitem.l_quantity)(#8.0), l_partkey(#8.1), __always_true(#8.2) ]
            ├── (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#9.25), __always_true(#9.27), l_comment(#9.15), l_commitdate(#9.11), l_discount(#9.6), l_extendedprice(#9.5), l_linenumber(#9.3), l_linestatus(#9.9), l_orderkey(#9.0), l_partkey(#9.1), l_partkey(#9.26), l_quantity(#9.4), l_receiptdate(#9.12), l_returnflag(#9.8), l_shipdate(#9.10), l_shipinstruct(#9.13), l_shipmode(#9.14), l_suppkey(#9.2), l_tax(#9.7), p_brand(#9.19), p_comment(#9.24), p_container(#9.22), p_mfgr(#9.18), p_name(#9.17), p_partkey(#9.16), p_retailprice(#9.23), p_size(#9.21), p_type(#9.20)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: (p_partkey(#2.0) = l_partkey(#8.1)) AND (CAST (l_quantity(#1.4) AS Decimal128(30, 15)) < Float64(0.2) * avg(lineitem.l_quantity)(#8.0))
                ├── (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#8.0), __always_true(#8.2), l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_partkey(#8.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: (l_partkey(#1.1) = p_partkey(#2.0))
                │   ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
                │   ├── (.cardinality): 0.00
                │   ├── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
                │   └── Select { .predicate: (p_brand(#2.3) = Brand#13::utf8_view) AND (p_container(#2.6) = JUMBO PKG::utf8_view), (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }
                └── Remap { .table_index: 8, (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#8.0), __always_true(#8.2), l_partkey(#8.1), (.cardinality): 0.00 }
                    └── Project { .table_index: 7, .projections: [ CAST (0.2::float64 * CAST (avg(lineitem.l_quantity)(#6.2) AS Float64) AS Decimal128(30, 15)), l_partkey(#1.1), __always_true(#6.1) ], (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#7.0), __always_true(#7.2), l_partkey(#7.1), (.cardinality): 0.00 }
                        └── Project { .table_index: 6, .projections: [ l_partkey(#3.1), true::boolean, avg(lineitem.l_quantity)(#5.0) ], (.output_columns): __always_true(#6.1), avg(lineitem.l_quantity)(#6.2), l_partkey(#6.0), (.cardinality): 0.00 }
                            └── Aggregate { .key_table_index: 4, .aggregate_table_index: 5, .implementation: None, .exprs: [ avg(l_quantity(#3.4)) ], .keys: [ l_partkey(#3.1) ], (.output_columns): avg(lineitem.l_quantity)(#5.0), lineitem.l_partkey(#4.0), (.cardinality): 0.00 }
                                └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Project { .table_index: 13, .projections: round(CAST (sum(lineitem.l_extendedprice)(#12.0) AS Float64) / 7::float64, 16::integer), (.output_columns): avg_yearly(#13.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: [ sum(l_extendedprice(#10.5)) ], .keys: [], (.output_columns): sum(lineitem.l_extendedprice)(#12.0), (.cardinality): 1.00 }
    └── Project
        ├── .table_index: 10
        ├── .projections: [ l_orderkey(#1.0), l_partkey(#1.1), l_suppkey(#1.2), l_linenumber(#1.3), l_quantity(#1.4), l_extendedprice(#1.5), l_discount(#1.6), l_tax(#1.7), l_returnflag(#1.8), l_linestatus(#1.9), l_shipdate(#1.10), l_commitdate(#1.11), l_receiptdate(#1.12), l_shipinstruct(#1.13), l_shipmode(#1.14), l_comment(#1.15), p_partkey(#2.0), p_name(#2.1), p_mfgr(#2.2), p_brand(#2.3), p_type(#2.4), p_size(#2.5), p_container(#2.6), p_retailprice(#2.7), p_comment(#2.8) ]
        ├── (.output_columns): l_comment(#10.15), l_commitdate(#10.11), l_discount(#10.6), l_extendedprice(#10.5), l_linenumber(#10.3), l_linestatus(#10.9), l_orderkey(#10.0), l_partkey(#10.1), l_quantity(#10.4), l_receiptdate(#10.12), l_returnflag(#10.8), l_shipdate(#10.10), l_shipinstruct(#10.13), l_shipmode(#10.14), l_suppkey(#10.2), l_tax(#10.7), p_brand(#10.19), p_comment(#10.24), p_container(#10.22), p_mfgr(#10.18), p_name(#10.17), p_partkey(#10.16), p_retailprice(#10.23), p_size(#10.21), p_type(#10.20)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: (p_partkey(#2.0) = l_partkey(#8.1)) AND (CAST (l_quantity(#1.4) AS Decimal128(30, 15)) < Float64(0.2) * avg(lineitem.l_quantity)(#8.0))
            ├── (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#8.0), __always_true(#8.2), l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_partkey(#8.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: l_partkey(#1.1) = p_partkey(#2.0)
            │   ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
            │   ├── (.cardinality): 0.00
            │   ├── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
            │   └── Select { .predicate: (p_brand(#2.3) = Brand#13::utf8_view) AND (p_container(#2.6) = JUMBO PKG::utf8_view), (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }
            │       └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }
            └── Remap { .table_index: 8, (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#8.0), __always_true(#8.2), l_partkey(#8.1), (.cardinality): 0.00 }
                └── Project { .table_index: 7, .projections: [ CAST (0.2::float64 * CAST (avg(lineitem.l_quantity)(#5.0) AS Float64) AS Decimal128(30, 15)), l_partkey(#1.1), true::boolean ], (.output_columns): Float64(0.2) * avg(lineitem.l_quantity)(#7.0), __always_true(#7.2), l_partkey(#7.1), (.cardinality): 0.00 }
                    └── Aggregate { .key_table_index: 4, .aggregate_table_index: 5, .implementation: None, .exprs: [ avg(l_quantity(#3.4)) ], .keys: [ l_partkey(#3.1) ], (.output_columns): avg(lineitem.l_quantity)(#5.0), lineitem.l_partkey(#4.0), (.cardinality): 0.00 }
                        └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_partkey(#3.1), l_quantity(#3.4), (.cardinality): 0.00 }

NULL
*/

