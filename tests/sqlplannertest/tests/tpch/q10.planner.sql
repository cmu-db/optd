-- TPC-H Q10
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;

/*
logical_plan after optd-initial:
Limit { .skip: 0::bigint, .fetch: 20::bigint, (.output_columns): `__internal_#7`.`c_acctbal`(#7.3), `__internal_#7`.`c_address`(#7.5), `__internal_#7`.`c_comment`(#7.7), `__internal_#7`.`c_custkey`(#7.0), `__internal_#7`.`c_name`(#7.1), `__internal_#7`.`c_phone`(#7.6), `__internal_#7`.`n_name`(#7.4), `__internal_#7`.`revenue`(#7.2), (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ `__internal_#7`.`revenue`(#7.2) DESC ], (.output_columns): `__internal_#7`.`c_acctbal`(#7.3), `__internal_#7`.`c_address`(#7.5), `__internal_#7`.`c_comment`(#7.7), `__internal_#7`.`c_custkey`(#7.0), `__internal_#7`.`c_name`(#7.1), `__internal_#7`.`c_phone`(#7.6), `__internal_#7`.`n_name`(#7.4), `__internal_#7`.`revenue`(#7.2), (.cardinality): 0.00 }
    └── Project { .table_index: 7, .projections: [ `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `__internal_#6`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#6.0), `customer`.`c_acctbal`(#1.5), `nation`.`n_name`(#4.1), `customer`.`c_address`(#1.2), `customer`.`c_phone`(#1.4), `customer`.`c_comment`(#1.7) ], (.output_columns): `__internal_#7`.`c_acctbal`(#7.3), `__internal_#7`.`c_address`(#7.5), `__internal_#7`.`c_comment`(#7.7), `__internal_#7`.`c_custkey`(#7.0), `__internal_#7`.`c_name`(#7.1), `__internal_#7`.`c_phone`(#7.6), `__internal_#7`.`n_name`(#7.4), `__internal_#7`.`revenue`(#7.2), (.cardinality): 0.00 }
        └── Aggregate { .key_table_index: 5, .aggregate_table_index: 6, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#3.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#3.6)), .keys: [ `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `customer`.`c_acctbal`(#1.5), `customer`.`c_phone`(#1.4), `nation`.`n_name`(#4.1), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7) ], (.output_columns): `__internal_#5`.`c_acctbal`(#5.2), `__internal_#5`.`c_address`(#5.5), `__internal_#5`.`c_comment`(#5.6), `__internal_#5`.`c_custkey`(#5.0), `__internal_#5`.`c_name`(#5.1), `__internal_#5`.`c_phone`(#5.3), `__internal_#5`.`n_name`(#5.4), `__internal_#6`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#6.0), (.cardinality): 0.00 }
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: (`customer`.`c_nationkey`(#1.3) = `nation`.`n_nationkey`(#4.0))
                ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `nation`.`n_comment`(#4.3), `nation`.`n_name`(#4.1), `nation`.`n_nationkey`(#4.0), `nation`.`n_regionkey`(#4.2), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: (`orders`.`o_orderkey`(#2.0) = `lineitem`.`l_orderkey`(#3.0))
                │   ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3)
                │   ├── (.cardinality): 0.00
                │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`customer`.`c_custkey`(#1.0) = `orders`.`o_custkey`(#2.1)), (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), (.cardinality): 0.00 }
                │   │   └── Select { .predicate: (`orders`.`o_orderdate`(#2.4) >= 1993-07-01::date32) AND (`orders`.`o_orderdate`(#2.4) < 1993-10-01::date32), (.output_columns): `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                │   └── Select { .predicate: `lineitem`.`l_returnflag`(#3.8) = R::utf8_view, (.output_columns): `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): `lineitem`.`l_comment`(#3.15), `lineitem`.`l_commitdate`(#3.11), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_linenumber`(#3.3), `lineitem`.`l_linestatus`(#3.9), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_partkey`(#3.1), `lineitem`.`l_quantity`(#3.4), `lineitem`.`l_receiptdate`(#3.12), `lineitem`.`l_returnflag`(#3.8), `lineitem`.`l_shipdate`(#3.10), `lineitem`.`l_shipinstruct`(#3.13), `lineitem`.`l_shipmode`(#3.14), `lineitem`.`l_suppkey`(#3.2), `lineitem`.`l_tax`(#3.7), (.cardinality): 0.00 }
                └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): `nation`.`n_comment`(#4.3), `nation`.`n_name`(#4.1), `nation`.`n_nationkey`(#4.0), `nation`.`n_regionkey`(#4.2), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 20::bigint, (.output_columns): `__internal_#7`.`c_acctbal`(#7.3), `__internal_#7`.`c_address`(#7.5), `__internal_#7`.`c_comment`(#7.7), `__internal_#7`.`c_custkey`(#7.0), `__internal_#7`.`c_name`(#7.1), `__internal_#7`.`c_phone`(#7.6), `__internal_#7`.`n_name`(#7.4), `__internal_#7`.`revenue`(#7.2), (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#7.2, Desc)], (.output_columns): `__internal_#7`.`c_acctbal`(#7.3), `__internal_#7`.`c_address`(#7.5), `__internal_#7`.`c_comment`(#7.7), `__internal_#7`.`c_custkey`(#7.0), `__internal_#7`.`c_name`(#7.1), `__internal_#7`.`c_phone`(#7.6), `__internal_#7`.`n_name`(#7.4), `__internal_#7`.`revenue`(#7.2), (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 7
        ├── .projections: [ `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `__internal_#6`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#6.0), `customer`.`c_acctbal`(#1.5), `nation`.`n_name`(#4.1), `customer`.`c_address`(#1.2), `customer`.`c_phone`(#1.4), `customer`.`c_comment`(#1.7) ]
        ├── (.output_columns): `__internal_#7`.`c_acctbal`(#7.3), `__internal_#7`.`c_address`(#7.5), `__internal_#7`.`c_comment`(#7.7), `__internal_#7`.`c_custkey`(#7.0), `__internal_#7`.`c_name`(#7.1), `__internal_#7`.`c_phone`(#7.6), `__internal_#7`.`n_name`(#7.4), `__internal_#7`.`revenue`(#7.2)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 5
            ├── .aggregate_table_index: 6
            ├── .implementation: None
            ├── .exprs: sum(`lineitem`.`l_extendedprice`(#3.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#3.6))
            ├── .keys: [ `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `customer`.`c_acctbal`(#1.5), `customer`.`c_phone`(#1.4), `nation`.`n_name`(#4.1), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7) ]
            ├── (.output_columns): `__internal_#5`.`c_acctbal`(#5.2), `__internal_#5`.`c_address`(#5.5), `__internal_#5`.`c_comment`(#5.6), `__internal_#5`.`c_custkey`(#5.0), `__internal_#5`.`c_name`(#5.1), `__internal_#5`.`c_phone`(#5.3), `__internal_#5`.`n_name`(#5.4), `__internal_#6`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#6.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: `customer`.`c_nationkey`(#1.3) = `nation`.`n_nationkey`(#4.0)
                ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_returnflag`(#3.8), `nation`.`n_name`(#4.1), `nation`.`n_nationkey`(#4.0), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: `orders`.`o_orderkey`(#2.0) = `lineitem`.`l_orderkey`(#3.0)
                │   ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_returnflag`(#3.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0)
                │   ├── (.cardinality): 0.00
                │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: `customer`.`c_custkey`(#1.0) = `orders`.`o_custkey`(#2.1), (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
                │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), (.cardinality): 0.00 }
                │   │   └── Select { .predicate: (`orders`.`o_orderdate`(#2.4) >= 1993-07-01::date32) AND (`orders`.`o_orderdate`(#2.4) < 1993-10-01::date32), (.output_columns): `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
                │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), (.cardinality): 0.00 }
                │   └── Select { .predicate: `lineitem`.`l_returnflag`(#3.8) = R::utf8_view, (.output_columns): `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_returnflag`(#3.8), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): `lineitem`.`l_discount`(#3.6), `lineitem`.`l_extendedprice`(#3.5), `lineitem`.`l_orderkey`(#3.0), `lineitem`.`l_returnflag`(#3.8), (.cardinality): 0.00 }
                └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): `nation`.`n_name`(#4.1), `nation`.`n_nationkey`(#4.0), (.cardinality): 0.00 }
*/

