-- TPC-H Q5
SELECT
    n_name AS nation,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'Asia' -- Specified region
    AND o_orderdate >= DATE '2023-01-01'
    AND o_orderdate < DATE '2024-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ `__internal_#9`.`revenue`(#9.1) DESC ], (.output_columns): `__internal_#9`.`nation`(#9.0), `__internal_#9`.`revenue`(#9.1), (.cardinality): 0.00 }
└── Project { .table_index: 9, .projections: [ `nation`.`n_name`(#5.1), `__internal_#8`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#8.0) ], (.output_columns): `__internal_#9`.`nation`(#9.0), `__internal_#9`.`revenue`(#9.1), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 7, .aggregate_table_index: 8, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#3.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#3.6)), .keys: [ `nation`.`n_name`(#5.1) ], (.output_columns): `__internal_#7`.`n_name`(#7.0), `__internal_#8`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#8.0), (.cardinality): 0.00 }
        └── Select
            ├── .predicate: (`region`.`r_name`(#6.1) = Asia::utf8_view) AND (`orders`.`o_orderdate`(#2.4) >= 2023-01-01::date32) AND (`orders`.`o_orderdate`(#2.4) < 2024-01-01::date32)
            ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `nation`.`n_comment`(#5.3), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), `region`.`r_comment`(#6.2), `region`.`r_name`(#6.1), `region`.`r_regionkey`(#6.0), `supplier`.`s_acctbal`(#4.5), `supplier`.`s_address`(#4.2), `supplier`.`s_comment`(#4.6), `supplier`.`s_name`(#4.1), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_phone`(#4.4), `supplier`.`s_suppkey`(#4.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: (`nation`.`n_regionkey`(#5.2) = `region`.`r_regionkey`(#6.0))
                ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `nation`.`n_comment`(#5.3), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), `region`.`r_comment`(#6.2), `region`.`r_name`(#6.1), `region`.`r_regionkey`(#6.0), `supplier`.`s_acctbal`(#4.5), `supplier`.`s_address`(#4.2), `supplier`.`s_comment`(#4.6), `supplier`.`s_name`(#4.1), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_phone`(#4.4), `supplier`.`s_suppkey`(#4.0)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: (`supplier`.`s_nationkey`(#4.3) = `nation`.`n_nationkey`(#5.0))
                │   ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `nation`.`n_comment`(#5.3), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), `supplier`.`s_acctbal`(#4.5), `supplier`.`s_address`(#4.2), `supplier`.`s_comment`(#4.6), `supplier`.`s_name`(#4.1), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_phone`(#4.4), `supplier`.`s_suppkey`(#4.0)
                │   ├── (.cardinality): 0.00
                │   ├── Join
                │   │   ├── .join_type: Inner
                │   │   ├── .implementation: None
                │   │   ├── .join_cond: (`lineitem`.`l_suppkey`(#3.2) = `supplier`.`s_suppkey`(#4.0)) AND (`customer`.`c_nationkey`(#1.3) = `supplier`.`s_nationkey`(#4.3))
                │   │   ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), `supplier`.`s_acctbal`(#4.5), `supplier`.`s_address`(#4.2), `supplier`.`s_comment`(#4.6), `supplier`.`s_name`(#4.1), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_phone`(#4.4), `supplier`.`s_suppkey`(#4.0)
                │   │   ├── (.cardinality): 0.00
                │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`orders`.`o_orderkey`(#2.0) = `lineitem`.`l_orderkey`(#3.0)), (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`customer`.`c_custkey`(#1.0) = `orders`.`o_custkey`(#2.1)), (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                │   │   │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), (.cardinality): 0.00 }
                │   │   │   │   └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                │   │   │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), (.cardinality): 0.00 }
                │   │   └── Get { .data_source_id: 4, .table_index: 4, .implementation: None, (.output_columns): `supplier`.`s_acctbal`(#4.5), `supplier`.`s_address`(#4.2), `supplier`.`s_comment`(#4.6), `supplier`.`s_name`(#4.1), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_phone`(#4.4), `supplier`.`s_suppkey`(#4.0), (.cardinality): 0.00 }
                │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): `nation`.`n_comment`(#5.3), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), (.cardinality): 0.00 }
                └── Get { .data_source_id: 2, .table_index: 6, .implementation: None, (.output_columns): `region`.`r_comment`(#6.2), `region`.`r_name`(#6.1), `region`.`r_regionkey`(#6.0), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#9.1, Desc)], (.output_columns): `__internal_#9`.`nation`(#9.0), `__internal_#9`.`revenue`(#9.1), (.cardinality): 0.00 }
└── Project { .table_index: 9, .projections: [ `nation`.`n_name`(#5.1), `__internal_#8`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#8.0) ], (.output_columns): `__internal_#9`.`nation`(#9.0), `__internal_#9`.`revenue`(#9.1), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 7, .aggregate_table_index: 8, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#3.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#3.6)), .keys: [ `nation`.`n_name`(#5.1) ], (.output_columns): `__internal_#7`.`n_name`(#7.0), `__internal_#8`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#8.0), (.cardinality): 0.00 }
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: `nation`.`n_regionkey`(#5.2) = `region`.`r_regionkey`(#6.0)
            ├── (.output_columns): `customer`.`c_custkey`(#1.0), `customer`.`c_nationkey`(#1.3), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_suppkey`(#3.2), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `region`.`r_name`(#6.1), `region`.`r_regionkey`(#6.0), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_suppkey`(#4.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: `supplier`.`s_nationkey`(#4.3) = `nation`.`n_nationkey`(#5.0)
            │   ├── (.output_columns): `customer`.`c_custkey`(#1.0), `customer`.`c_nationkey`(#1.3), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_suppkey`(#3.2), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_suppkey`(#4.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: (`lineitem`.`l_suppkey`(#3.2) = `supplier`.`s_suppkey`(#4.0)) AND (`customer`.`c_nationkey`(#1.3) = `supplier`.`s_nationkey`(#4.3))
            │   │   ├── (.output_columns): `customer`.`c_custkey`(#1.0), `customer`.`c_nationkey`(#1.3), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_suppkey`(#3.2), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `supplier`.`s_nationkey`(#4.3), `supplier`.`s_suppkey`(#4.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: `orders`.`o_orderkey`(#2.0) = `lineitem`.`l_orderkey`(#3.0), (.output_columns): `customer`.`c_custkey`(#1.0), `customer`.`c_nationkey`(#1.3), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_suppkey`(#3.2), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
            │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: `customer`.`c_custkey`(#1.0) = `orders`.`o_custkey`(#2.1), (.output_columns): `customer`.`c_custkey`(#1.0), `customer`.`c_nationkey`(#1.3), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
            │   │   │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): `customer`.`c_custkey`(#1.0), `customer`.`c_nationkey`(#1.3), (.cardinality): 0.00 }
            │   │   │   │   └── Select { .predicate: (`orders`.`o_orderdate`(#2.4) >= 2023-01-01::date32) AND (`orders`.`o_orderdate`(#2.4) < 2024-01-01::date32), (.output_columns): `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
            │   │   │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
            │   │   │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_suppkey`(#3.2), (.cardinality): 0.00 }
            │   │   └── Get { .data_source_id: 4, .table_index: 4, .implementation: None, (.output_columns): `supplier`.`s_nationkey`(#4.3), `supplier`.`s_suppkey`(#4.0), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), (.cardinality): 0.00 }
            └── Select { .predicate: `region`.`r_name`(#6.1) = Asia::utf8_view, (.output_columns): `region`.`r_name`(#6.1), `region`.`r_regionkey`(#6.0), (.cardinality): 0.00 }
                └── Get { .data_source_id: 2, .table_index: 6, .implementation: None, (.output_columns): `region`.`r_name`(#6.1), `region`.`r_regionkey`(#6.0), (.cardinality): 0.00 }
*/

