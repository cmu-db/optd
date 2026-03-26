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
OrderBy { ordering_exprs: [ revenue(#8.1) DESC ], (.output_columns): nation(#8.0), revenue(#8.1), (.cardinality): 0.00 }
└── Project { .table_index: 8, .projections: [ n_name(#5.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#7.0) ], (.output_columns): nation(#8.0), revenue(#8.1), (.cardinality): 0.00 }
    └── Aggregate { .aggregate_table_index: 7, .implementation: None, .exprs: sum(l_extendedprice(#3.5) * 1::decimal128(20, 0) - l_discount(#3.6)), .keys: [ n_name(#5.1) ], (.output_columns): n_name(#5.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#7.0), (.cardinality): 0.00 }
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: (n_regionkey(#5.2) = r_regionkey(#6.0))
            ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), r_comment(#6.2), r_name(#6.1), r_regionkey(#6.0), s_acctbal(#4.5), s_address(#4.2), s_comment(#4.6), s_name(#4.1), s_nationkey(#4.3), s_phone(#4.4), s_suppkey(#4.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: (s_nationkey(#4.3) = n_nationkey(#5.0))
            │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), s_acctbal(#4.5), s_address(#4.2), s_comment(#4.6), s_name(#4.1), s_nationkey(#4.3), s_phone(#4.4), s_suppkey(#4.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: (l_suppkey(#3.2) = s_suppkey(#4.0)) AND (c_nationkey(#1.3) = s_nationkey(#4.3))
            │   │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), s_acctbal(#4.5), s_address(#4.2), s_comment(#4.6), s_name(#4.1), s_nationkey(#4.3), s_phone(#4.4), s_suppkey(#4.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (o_orderkey(#2.0) = l_orderkey(#3.0)), (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
            │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (c_custkey(#1.0) = o_custkey(#2.1)), (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
            │   │   │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
            │   │   │   │   └── Select { .predicate: (o_orderdate(#2.4) >= 2023-01-01::date32) AND (o_orderdate(#2.4) < 2024-01-01::date32), (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
            │   │   │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
            │   │   │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }
            │   │   └── Get { .data_source_id: 4, .table_index: 4, .implementation: None, (.output_columns): s_acctbal(#4.5), s_address(#4.2), s_comment(#4.6), s_name(#4.1), s_nationkey(#4.3), s_phone(#4.4), s_suppkey(#4.0), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), (.cardinality): 0.00 }
            └── Select { .predicate: r_name(#6.1) = Asia::utf8_view, (.output_columns): r_comment(#6.2), r_name(#6.1), r_regionkey(#6.0), (.cardinality): 0.00 }
                └── Get { .data_source_id: 2, .table_index: 6, .implementation: None, (.output_columns): r_comment(#6.2), r_name(#6.1), r_regionkey(#6.0), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#8.1, Desc)], (.output_columns): nation(#8.0), revenue(#8.1), (.cardinality): 0.00 }
└── Project { .table_index: 8, .projections: [ n_name(#5.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#7.0) ], (.output_columns): nation(#8.0), revenue(#8.1), (.cardinality): 0.00 }
    └── Aggregate { .aggregate_table_index: 7, .implementation: None, .exprs: sum(l_extendedprice(#3.5) * 1::decimal128(20, 0) - l_discount(#3.6)), .keys: [ n_name(#5.1) ], (.output_columns): n_name(#5.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#7.0), (.cardinality): 0.00 }
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: n_regionkey(#5.2) = r_regionkey(#6.0)
            ├── (.output_columns): c_custkey(#1.0), c_nationkey(#1.3), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_suppkey(#3.2), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), r_name(#6.1), r_regionkey(#6.0), s_nationkey(#4.3), s_suppkey(#4.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: s_nationkey(#4.3) = n_nationkey(#5.0)
            │   ├── (.output_columns): c_custkey(#1.0), c_nationkey(#1.3), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_suppkey(#3.2), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), s_nationkey(#4.3), s_suppkey(#4.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: (l_suppkey(#3.2) = s_suppkey(#4.0)) AND (c_nationkey(#1.3) = s_nationkey(#4.3))
            │   │   ├── (.output_columns): c_custkey(#1.0), c_nationkey(#1.3), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_suppkey(#3.2), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), s_nationkey(#4.3), s_suppkey(#4.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join
            │   │   │   ├── .join_type: Inner
            │   │   │   ├── .implementation: None
            │   │   │   ├── .join_cond: o_orderkey(#2.0) = l_orderkey(#3.0)
            │   │   │   ├── (.output_columns): c_custkey(#1.0), c_nationkey(#1.3), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_suppkey(#3.2), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0)
            │   │   │   ├── (.cardinality): 0.00
            │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: c_custkey(#1.0) = o_custkey(#2.1), (.output_columns): c_custkey(#1.0), c_nationkey(#1.3), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), (.cardinality): 0.00 }
            │   │   │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_custkey(#1.0), c_nationkey(#1.3), (.cardinality): 0.00 }
            │   │   │   │   └── Select { .predicate: (o_orderdate(#2.4) >= 2023-01-01::date32) AND (o_orderdate(#2.4) < 2024-01-01::date32), (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), (.cardinality): 0.00 }
            │   │   │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), (.cardinality): 0.00 }
            │   │   │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_suppkey(#3.2), (.cardinality): 0.00 }
            │   │   └── Get { .data_source_id: 4, .table_index: 4, .implementation: None, (.output_columns): s_nationkey(#4.3), s_suppkey(#4.0), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), (.cardinality): 0.00 }
            └── Select { .predicate: r_name(#6.1) = Asia::utf8_view, (.output_columns): r_name(#6.1), r_regionkey(#6.0), (.cardinality): 0.00 }
                └── Get { .data_source_id: 2, .table_index: 6, .implementation: None, (.output_columns): r_name(#6.1), r_regionkey(#6.0), (.cardinality): 0.00 }
*/

