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
Project { .table_index: 5, .projections: `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0), (.output_columns): `__internal_#5`.`revenue`(#5.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#1.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#1.6)), .keys: [], (.output_columns): `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0), (.cardinality): 1.00 }
    └── Select
        ├── .predicate: (`part`.`p_size`(#2.5) >= 1::integer) AND ((`lineitem`.`l_shipmode`(#1.14) = AIR::utf8_view) OR (`lineitem`.`l_shipmode`(#1.14) = AIR REG::utf8_view)) AND (`lineitem`.`l_shipinstruct`(#1.13) = DELIVER IN PERSON::utf8_view) AND (((`part`.`p_brand`(#2.3) = Brand#12::utf8_view) AND (`part`.`p_container`(#2.6) IN [SM CASE::utf8_view, SM BOX::utf8_view, SM PACK::utf8_view, SM PKG::utf8_view]) AND (`lineitem`.`l_quantity`(#1.4) >= 100::decimal128(15, 2)) AND (`lineitem`.`l_quantity`(#1.4) <= 1100::decimal128(15, 2)) AND (`part`.`p_size`(#2.5) <= 5::integer)) OR ((`part`.`p_brand`(#2.3) = Brand#23::utf8_view) AND (`part`.`p_container`(#2.6) IN [MED BAG::utf8_view, MED BOX::utf8_view, MED PKG::utf8_view, MED PACK::utf8_view]) AND (`lineitem`.`l_quantity`(#1.4) >= 1000::decimal128(15, 2)) AND (`lineitem`.`l_quantity`(#1.4) <= 2000::decimal128(15, 2)) AND (`part`.`p_size`(#2.5) <= 10::integer)) OR ((`part`.`p_brand`(#2.3) = Brand#34::utf8_view) AND (`part`.`p_container`(#2.6) IN [LG CASE::utf8_view, LG BOX::utf8_view, LG PACK::utf8_view, LG PKG::utf8_view]) AND (`lineitem`.`l_quantity`(#1.4) >= 2000::decimal128(15, 2)) AND (`lineitem`.`l_quantity`(#1.4) <= 3000::decimal128(15, 2)) AND (`part`.`p_size`(#2.5) <= 15::integer)))
        ├── (.output_columns): `lineitem`.`l_comment`(#1.15), `lineitem`.`l_commitdate`(#1.11), `lineitem`.`l_discount`(#1.6), `lineitem`.`l_extendedprice`(#1.5), `lineitem`.`l_linenumber`(#1.3), `lineitem`.`l_linestatus`(#1.9), `lineitem`.`l_orderkey`(#1.0), `lineitem`.`l_partkey`(#1.1), `lineitem`.`l_quantity`(#1.4), `lineitem`.`l_receiptdate`(#1.12), `lineitem`.`l_returnflag`(#1.8), `lineitem`.`l_shipdate`(#1.10), `lineitem`.`l_shipinstruct`(#1.13), `lineitem`.`l_shipmode`(#1.14), `lineitem`.`l_suppkey`(#1.2), `lineitem`.`l_tax`(#1.7), `part`.`p_brand`(#2.3), `part`.`p_comment`(#2.8), `part`.`p_container`(#2.6), `part`.`p_mfgr`(#2.2), `part`.`p_name`(#2.1), `part`.`p_partkey`(#2.0), `part`.`p_retailprice`(#2.7), `part`.`p_size`(#2.5), `part`.`p_type`(#2.4)
        ├── (.cardinality): 0.00
        └── Join { .join_type: Inner, .implementation: None, .join_cond: (`lineitem`.`l_partkey`(#1.1) = `part`.`p_partkey`(#2.0)), (.output_columns): `lineitem`.`l_comment`(#1.15), `lineitem`.`l_commitdate`(#1.11), `lineitem`.`l_discount`(#1.6), `lineitem`.`l_extendedprice`(#1.5), `lineitem`.`l_linenumber`(#1.3), `lineitem`.`l_linestatus`(#1.9), `lineitem`.`l_orderkey`(#1.0), `lineitem`.`l_partkey`(#1.1), `lineitem`.`l_quantity`(#1.4), `lineitem`.`l_receiptdate`(#1.12), `lineitem`.`l_returnflag`(#1.8), `lineitem`.`l_shipdate`(#1.10), `lineitem`.`l_shipinstruct`(#1.13), `lineitem`.`l_shipmode`(#1.14), `lineitem`.`l_suppkey`(#1.2), `lineitem`.`l_tax`(#1.7), `part`.`p_brand`(#2.3), `part`.`p_comment`(#2.8), `part`.`p_container`(#2.6), `part`.`p_mfgr`(#2.2), `part`.`p_name`(#2.1), `part`.`p_partkey`(#2.0), `part`.`p_retailprice`(#2.7), `part`.`p_size`(#2.5), `part`.`p_type`(#2.4), (.cardinality): 0.00 }
            ├── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): `lineitem`.`l_comment`(#1.15), `lineitem`.`l_commitdate`(#1.11), `lineitem`.`l_discount`(#1.6), `lineitem`.`l_extendedprice`(#1.5), `lineitem`.`l_linenumber`(#1.3), `lineitem`.`l_linestatus`(#1.9), `lineitem`.`l_orderkey`(#1.0), `lineitem`.`l_partkey`(#1.1), `lineitem`.`l_quantity`(#1.4), `lineitem`.`l_receiptdate`(#1.12), `lineitem`.`l_returnflag`(#1.8), `lineitem`.`l_shipdate`(#1.10), `lineitem`.`l_shipinstruct`(#1.13), `lineitem`.`l_shipmode`(#1.14), `lineitem`.`l_suppkey`(#1.2), `lineitem`.`l_tax`(#1.7), (.cardinality): 0.00 }
            └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): `part`.`p_brand`(#2.3), `part`.`p_comment`(#2.8), `part`.`p_container`(#2.6), `part`.`p_mfgr`(#2.2), `part`.`p_name`(#2.1), `part`.`p_partkey`(#2.0), `part`.`p_retailprice`(#2.7), `part`.`p_size`(#2.5), `part`.`p_type`(#2.4), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Project { .table_index: 5, .projections: `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0), (.output_columns): `__internal_#5`.`revenue`(#5.0), (.cardinality): 1.00 }
└── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#1.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#1.6)), .keys: [], (.output_columns): `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0), (.cardinality): 1.00 }
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: (`lineitem`.`l_partkey`(#1.1) = `part`.`p_partkey`(#2.0)) AND (((`part`.`p_brand`(#2.3) = Brand#12::utf8_view) AND (`part`.`p_container`(#2.6) IN [SM CASE::utf8_view, SM BOX::utf8_view, SM PACK::utf8_view, SM PKG::utf8_view]) AND (`lineitem`.`l_quantity`(#1.4) >= 100::decimal128(15, 2)) AND (`lineitem`.`l_quantity`(#1.4) <= 1100::decimal128(15, 2)) AND (`part`.`p_size`(#2.5) <= 5::integer)) OR ((`part`.`p_brand`(#2.3) = Brand#23::utf8_view) AND (`part`.`p_container`(#2.6) IN [MED BAG::utf8_view, MED BOX::utf8_view, MED PKG::utf8_view, MED PACK::utf8_view]) AND (`lineitem`.`l_quantity`(#1.4) >= 1000::decimal128(15, 2)) AND (`lineitem`.`l_quantity`(#1.4) <= 2000::decimal128(15, 2)) AND (`part`.`p_size`(#2.5) <= 10::integer)) OR ((`part`.`p_brand`(#2.3) = Brand#34::utf8_view) AND (`part`.`p_container`(#2.6) IN [LG CASE::utf8_view, LG BOX::utf8_view, LG PACK::utf8_view, LG PKG::utf8_view]) AND (`lineitem`.`l_quantity`(#1.4) >= 2000::decimal128(15, 2)) AND (`lineitem`.`l_quantity`(#1.4) <= 3000::decimal128(15, 2)) AND (`part`.`p_size`(#2.5) <= 15::integer)))
        ├── (.output_columns): `lineitem`.`l_discount`(#1.6), `lineitem`.`l_extendedprice`(#1.5), `lineitem`.`l_partkey`(#1.1), `lineitem`.`l_quantity`(#1.4), `lineitem`.`l_shipinstruct`(#1.13), `lineitem`.`l_shipmode`(#1.14), `part`.`p_brand`(#2.3), `part`.`p_container`(#2.6), `part`.`p_partkey`(#2.0), `part`.`p_size`(#2.5)
        ├── (.cardinality): 0.00
        ├── Select { .predicate: ((`lineitem`.`l_shipmode`(#1.14) = AIR::utf8_view) OR (`lineitem`.`l_shipmode`(#1.14) = AIR REG::utf8_view)) AND (`lineitem`.`l_shipinstruct`(#1.13) = DELIVER IN PERSON::utf8_view), (.output_columns): `lineitem`.`l_discount`(#1.6), `lineitem`.`l_extendedprice`(#1.5), `lineitem`.`l_partkey`(#1.1), `lineitem`.`l_quantity`(#1.4), `lineitem`.`l_shipinstruct`(#1.13), `lineitem`.`l_shipmode`(#1.14), (.cardinality): 0.00 }
        │   └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): `lineitem`.`l_discount`(#1.6), `lineitem`.`l_extendedprice`(#1.5), `lineitem`.`l_partkey`(#1.1), `lineitem`.`l_quantity`(#1.4), `lineitem`.`l_shipinstruct`(#1.13), `lineitem`.`l_shipmode`(#1.14), (.cardinality): 0.00 }
        └── Select { .predicate: `part`.`p_size`(#2.5) >= 1::integer, (.output_columns): `part`.`p_brand`(#2.3), `part`.`p_container`(#2.6), `part`.`p_partkey`(#2.0), `part`.`p_size`(#2.5), (.cardinality): 0.00 }
            └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): `part`.`p_brand`(#2.3), `part`.`p_container`(#2.6), `part`.`p_partkey`(#2.0), `part`.`p_size`(#2.5), (.cardinality): 0.00 }

NULL
*/

