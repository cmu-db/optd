-- TPC-H Q14
SELECT
    100.00 * sum(case when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;

/*
logical_plan after optd-initial:
Project { .table_index: 6, .projections: 100::float64 * CAST (sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)(#5.0) AS Float64) / CAST (sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.1) AS Float64), (.output_columns): promo_revenue(#6.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 4, .aggregate_table_index: 5, .implementation: None, .exprs: [ sum(CASE WHEN p_type(#3.21) LIKE PROMO%::utf8_view THEN __common_expr_1(#3.0) ELSE 0::decimal128(38, 4) END), sum(__common_expr_1(#3.0)) ], .keys: [], (.output_columns): sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)(#5.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.1), (.cardinality): 1.00 }
    └── Project
        ├── .table_index: 3
        ├── .projections:
        │   ┌── l_extendedprice(#1.5) * 1::decimal128(20, 0) - l_discount(#1.6)
        │   ├── l_orderkey(#1.0)
        │   ├── l_partkey(#1.1)
        │   ├── l_suppkey(#1.2)
        │   ├── l_linenumber(#1.3)
        │   ├── l_quantity(#1.4)
        │   ├── l_extendedprice(#1.5)
        │   ├── l_discount(#1.6)
        │   ├── l_tax(#1.7)
        │   ├── l_returnflag(#1.8)
        │   ├── l_linestatus(#1.9)
        │   ├── l_shipdate(#1.10)
        │   ├── l_commitdate(#1.11)
        │   ├── l_receiptdate(#1.12)
        │   ├── l_shipinstruct(#1.13)
        │   ├── l_shipmode(#1.14)
        │   ├── l_comment(#1.15)
        │   ├── p_partkey(#2.0)
        │   ├── p_name(#2.1)
        │   ├── p_mfgr(#2.2)
        │   ├── p_brand(#2.3)
        │   ├── p_type(#2.4)
        │   ├── p_size(#2.5)
        │   ├── p_container(#2.6)
        │   ├── p_retailprice(#2.7)
        │   └── p_comment(#2.8)
        ├── (.output_columns): __common_expr_1(#3.0), l_comment(#3.16), l_commitdate(#3.12), l_discount(#3.7), l_extendedprice(#3.6), l_linenumber(#3.4), l_linestatus(#3.10), l_orderkey(#3.1), l_partkey(#3.2), l_quantity(#3.5), l_receiptdate(#3.13), l_returnflag(#3.9), l_shipdate(#3.11), l_shipinstruct(#3.14), l_shipmode(#3.15), l_suppkey(#3.3), l_tax(#3.8), p_brand(#3.20), p_comment(#3.25), p_container(#3.23), p_mfgr(#3.19), p_name(#3.18), p_partkey(#3.17), p_retailprice(#3.24), p_size(#3.22), p_type(#3.21)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: (l_partkey(#1.1) = p_partkey(#2.0))
            ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
            ├── (.cardinality): 0.00
            ├── Select { .predicate: (l_shipdate(#1.10) >= 1995-09-01::date32) AND (l_shipdate(#1.10) < 1995-10-01::date32), (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
            └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Project { .table_index: 6, .projections: 100::float64 * CAST (sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)(#5.0) AS Float64) / CAST (sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.1) AS Float64), (.output_columns): promo_revenue(#6.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 4, .aggregate_table_index: 5, .implementation: None, .exprs: [ sum(CASE WHEN p_type(#3.21) LIKE PROMO%::utf8_view THEN __common_expr_1(#3.0) ELSE 0::decimal128(38, 4) END), sum(__common_expr_1(#3.0)) ], .keys: [], (.output_columns): sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)(#5.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.1), (.cardinality): 1.00 }
    └── Project
        ├── .table_index: 3
        ├── .projections:
        │   ┌── l_extendedprice(#1.5) * 1::decimal128(20, 0) - l_discount(#1.6)
        │   ├── l_orderkey(#1.0)
        │   ├── l_partkey(#1.1)
        │   ├── l_suppkey(#1.2)
        │   ├── l_linenumber(#1.3)
        │   ├── l_quantity(#1.4)
        │   ├── l_extendedprice(#1.5)
        │   ├── l_discount(#1.6)
        │   ├── l_tax(#1.7)
        │   ├── l_returnflag(#1.8)
        │   ├── l_linestatus(#1.9)
        │   ├── l_shipdate(#1.10)
        │   ├── l_commitdate(#1.11)
        │   ├── l_receiptdate(#1.12)
        │   ├── l_shipinstruct(#1.13)
        │   ├── l_shipmode(#1.14)
        │   ├── l_comment(#1.15)
        │   ├── p_partkey(#2.0)
        │   ├── p_name(#2.1)
        │   ├── p_mfgr(#2.2)
        │   ├── p_brand(#2.3)
        │   ├── p_type(#2.4)
        │   ├── p_size(#2.5)
        │   ├── p_container(#2.6)
        │   ├── p_retailprice(#2.7)
        │   └── p_comment(#2.8)
        ├── (.output_columns): __common_expr_1(#3.0), l_comment(#3.16), l_commitdate(#3.12), l_discount(#3.7), l_extendedprice(#3.6), l_linenumber(#3.4), l_linestatus(#3.10), l_orderkey(#3.1), l_partkey(#3.2), l_quantity(#3.5), l_receiptdate(#3.13), l_returnflag(#3.9), l_shipdate(#3.11), l_shipinstruct(#3.14), l_shipmode(#3.15), l_suppkey(#3.3), l_tax(#3.8), p_brand(#3.20), p_comment(#3.25), p_container(#3.23), p_mfgr(#3.19), p_name(#3.18), p_partkey(#3.17), p_retailprice(#3.24), p_size(#3.22), p_type(#3.21)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: l_partkey(#1.1) = p_partkey(#2.0)
            ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
            ├── (.cardinality): 0.00
            ├── Select { .predicate: (l_shipdate(#1.10) >= 1995-09-01::date32) AND (l_shipdate(#1.10) < 1995-10-01::date32), (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
            └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }

NULL
*/

