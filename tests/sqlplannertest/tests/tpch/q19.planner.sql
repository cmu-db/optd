-- TPC-H Q19
SELECT
    sum(l_extendedprice* (1 - l_discount)) as revenue
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    ) OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    ) OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )

/*
logical_plan after optd-initial:
Project { .table_index: 4, .projections: sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.0), (.output_columns): revenue(#4.0), (.cardinality): 1.00 }
└── Aggregate { .aggregate_table_index: 3, .implementation: None, .exprs: sum(l_extendedprice(#1.5) * 1::decimal128(20, 0) - l_discount(#1.6)), .keys: [], (.output_columns): sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.0), (.cardinality): 1.00 }
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: (l_partkey(#1.1) = p_partkey(#2.0)) AND (((p_brand(#2.3) = Brand#12::utf8_view) AND (p_container(#2.6) IN [SM CASE::utf8_view, SM BOX::utf8_view, SM PACK::utf8_view, SM PKG::utf8_view]) AND (l_quantity(#1.4) >= 100::decimal128(15, 2)) AND (l_quantity(#1.4) <= 1100::decimal128(15, 2)) AND (p_size(#2.5) <= 5::integer)) OR ((p_brand(#2.3) = Brand#23::utf8_view) AND (p_container(#2.6) IN [MED BAG::utf8_view, MED BOX::utf8_view, MED PKG::utf8_view, MED PACK::utf8_view]) AND (l_quantity(#1.4) >= 1000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 2000::decimal128(15, 2)) AND (p_size(#2.5) <= 10::integer)) OR ((p_brand(#2.3) = Brand#34::utf8_view) AND (p_container(#2.6) IN [LG CASE::utf8_view, LG BOX::utf8_view, LG PACK::utf8_view, LG PKG::utf8_view]) AND (l_quantity(#1.4) >= 2000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 3000::decimal128(15, 2)) AND (p_size(#2.5) <= 15::integer)))
        ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4)
        ├── (.cardinality): 0.00
        ├── Select { .predicate: (((l_quantity(#1.4) >= 100::decimal128(15, 2)) AND (l_quantity(#1.4) <= 1100::decimal128(15, 2))) OR ((l_quantity(#1.4) >= 1000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 2000::decimal128(15, 2))) OR ((l_quantity(#1.4) >= 2000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 3000::decimal128(15, 2)))) AND ((l_shipmode(#1.14) = AIR::utf8_view) OR (l_shipmode(#1.14) = AIR REG::utf8_view)) AND (l_shipinstruct(#1.13) = DELIVER IN PERSON::utf8_view), (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
        │   └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
        └── Select { .predicate: (((p_brand(#2.3) = Brand#12::utf8_view) AND (p_container(#2.6) IN [SM CASE::utf8_view, SM BOX::utf8_view, SM PACK::utf8_view, SM PKG::utf8_view]) AND (p_size(#2.5) <= 5::integer)) OR ((p_brand(#2.3) = Brand#23::utf8_view) AND (p_container(#2.6) IN [MED BAG::utf8_view, MED BOX::utf8_view, MED PKG::utf8_view, MED PACK::utf8_view]) AND (p_size(#2.5) <= 10::integer)) OR ((p_brand(#2.3) = Brand#34::utf8_view) AND (p_container(#2.6) IN [LG CASE::utf8_view, LG BOX::utf8_view, LG PACK::utf8_view, LG PKG::utf8_view]) AND (p_size(#2.5) <= 15::integer))) AND (p_size(#2.5) >= 1::integer), (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }
            └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): p_brand(#2.3), p_comment(#2.8), p_container(#2.6), p_mfgr(#2.2), p_name(#2.1), p_partkey(#2.0), p_retailprice(#2.7), p_size(#2.5), p_type(#2.4), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Project { .table_index: 4, .projections: sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.0), (.output_columns): revenue(#4.0), (.cardinality): 1.00 }
└── Aggregate { .aggregate_table_index: 3, .implementation: None, .exprs: sum(l_extendedprice(#1.5) * 1::decimal128(20, 0) - l_discount(#1.6)), .keys: [], (.output_columns): sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.0), (.cardinality): 1.00 }
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: (l_partkey(#1.1) = p_partkey(#2.0)) AND (((p_brand(#2.3) = Brand#12::utf8_view) AND (p_container(#2.6) IN [SM CASE::utf8_view, SM BOX::utf8_view, SM PACK::utf8_view, SM PKG::utf8_view]) AND (l_quantity(#1.4) >= 100::decimal128(15, 2)) AND (l_quantity(#1.4) <= 1100::decimal128(15, 2)) AND (p_size(#2.5) <= 5::integer)) OR ((p_brand(#2.3) = Brand#23::utf8_view) AND (p_container(#2.6) IN [MED BAG::utf8_view, MED BOX::utf8_view, MED PKG::utf8_view, MED PACK::utf8_view]) AND (l_quantity(#1.4) >= 1000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 2000::decimal128(15, 2)) AND (p_size(#2.5) <= 10::integer)) OR ((p_brand(#2.3) = Brand#34::utf8_view) AND (p_container(#2.6) IN [LG CASE::utf8_view, LG BOX::utf8_view, LG PACK::utf8_view, LG PKG::utf8_view]) AND (l_quantity(#1.4) >= 2000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 3000::decimal128(15, 2)) AND (p_size(#2.5) <= 15::integer)))
        ├── (.output_columns): l_discount(#1.6), l_extendedprice(#1.5), l_partkey(#1.1), l_quantity(#1.4), l_shipinstruct(#1.13), l_shipmode(#1.14), p_brand(#2.3), p_container(#2.6), p_partkey(#2.0), p_size(#2.5)
        ├── (.cardinality): 0.00
        ├── Select { .predicate: (((l_quantity(#1.4) >= 100::decimal128(15, 2)) AND (l_quantity(#1.4) <= 1100::decimal128(15, 2))) OR ((l_quantity(#1.4) >= 1000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 2000::decimal128(15, 2))) OR ((l_quantity(#1.4) >= 2000::decimal128(15, 2)) AND (l_quantity(#1.4) <= 3000::decimal128(15, 2)))) AND ((l_shipmode(#1.14) = AIR::utf8_view) OR (l_shipmode(#1.14) = AIR REG::utf8_view)) AND (l_shipinstruct(#1.13) = DELIVER IN PERSON::utf8_view), (.output_columns): l_discount(#1.6), l_extendedprice(#1.5), l_partkey(#1.1), l_quantity(#1.4), l_shipinstruct(#1.13), l_shipmode(#1.14), (.cardinality): 0.00 }
        │   └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): l_discount(#1.6), l_extendedprice(#1.5), l_partkey(#1.1), l_quantity(#1.4), l_shipinstruct(#1.13), l_shipmode(#1.14), (.cardinality): 0.00 }
        └── Select { .predicate: (((p_brand(#2.3) = Brand#12::utf8_view) AND (p_container(#2.6) IN [SM CASE::utf8_view, SM BOX::utf8_view, SM PACK::utf8_view, SM PKG::utf8_view]) AND (p_size(#2.5) <= 5::integer)) OR ((p_brand(#2.3) = Brand#23::utf8_view) AND (p_container(#2.6) IN [MED BAG::utf8_view, MED BOX::utf8_view, MED PKG::utf8_view, MED PACK::utf8_view]) AND (p_size(#2.5) <= 10::integer)) OR ((p_brand(#2.3) = Brand#34::utf8_view) AND (p_container(#2.6) IN [LG CASE::utf8_view, LG BOX::utf8_view, LG PACK::utf8_view, LG PKG::utf8_view]) AND (p_size(#2.5) <= 15::integer))) AND (p_size(#2.5) >= 1::integer), (.output_columns): p_brand(#2.3), p_container(#2.6), p_partkey(#2.0), p_size(#2.5), (.cardinality): 0.00 }
            └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): p_brand(#2.3), p_container(#2.6), p_partkey(#2.0), p_size(#2.5), (.cardinality): 0.00 }

NULL
*/

