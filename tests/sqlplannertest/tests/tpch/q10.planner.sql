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
Limit { .skip: 0::bigint, .fetch: 20::bigint, (.output_columns): c_acctbal(#6.3), c_address(#6.5), c_comment(#6.7), c_custkey(#6.0), c_name(#6.1), c_phone(#6.6), n_name(#6.4), revenue(#6.2), (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ revenue(#6.2) DESC ], (.output_columns): c_acctbal(#6.3), c_address(#6.5), c_comment(#6.7), c_custkey(#6.0), c_name(#6.1), c_phone(#6.6), n_name(#6.4), revenue(#6.2), (.cardinality): 0.00 }
    └── Project { .table_index: 6, .projections: [ c_custkey(#1.0), c_name(#1.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0), c_acctbal(#1.5), n_name(#4.1), c_address(#1.2), c_phone(#1.4), c_comment(#1.7) ], (.output_columns): c_acctbal(#6.3), c_address(#6.5), c_comment(#6.7), c_custkey(#6.0), c_name(#6.1), c_phone(#6.6), n_name(#6.4), revenue(#6.2), (.cardinality): 0.00 }
        └── Aggregate { .aggregate_table_index: 5, .implementation: None, .exprs: sum(l_extendedprice(#3.5) * 1::decimal128(20, 0) - l_discount(#3.6)), .keys: [ c_custkey(#1.0), c_name(#1.1), c_acctbal(#1.5), c_phone(#1.4), n_name(#4.1), c_address(#1.2), c_comment(#1.7) ], (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_name(#1.1), c_phone(#1.4), n_name(#4.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0), (.cardinality): 0.00 }
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: (c_nationkey(#1.3) = n_nationkey(#4.0))
                ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), n_comment(#4.3), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: (o_orderkey(#2.0) = l_orderkey(#3.0))
                │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                │   ├── (.cardinality): 0.00
                │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (c_custkey(#1.0) = o_custkey(#2.1)), (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
                │   │   └── Select { .predicate: (o_orderdate(#2.4) >= 1993-07-01::date32) AND (o_orderdate(#2.4) < 1993-10-01::date32), (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │   └── Select { .predicate: l_returnflag(#3.8) = R::utf8_view, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }
                └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): n_comment(#4.3), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 20::bigint, (.output_columns): c_acctbal(#6.3), c_address(#6.5), c_comment(#6.7), c_custkey(#6.0), c_name(#6.1), c_phone(#6.6), n_name(#6.4), revenue(#6.2), (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#6.2, Desc)], (.output_columns): c_acctbal(#6.3), c_address(#6.5), c_comment(#6.7), c_custkey(#6.0), c_name(#6.1), c_phone(#6.6), n_name(#6.4), revenue(#6.2), (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 6
        ├── .projections: [ c_custkey(#1.0), c_name(#1.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0), c_acctbal(#1.5), n_name(#4.1), c_address(#1.2), c_phone(#1.4), c_comment(#1.7) ]
        ├── (.output_columns): c_acctbal(#6.3), c_address(#6.5), c_comment(#6.7), c_custkey(#6.0), c_name(#6.1), c_phone(#6.6), n_name(#6.4), revenue(#6.2)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .aggregate_table_index: 5
            ├── .implementation: None
            ├── .exprs: sum(l_extendedprice(#3.5) * 1::decimal128(20, 0) - l_discount(#3.6))
            ├── .keys: [ c_custkey(#1.0), c_name(#1.1), c_acctbal(#1.5), c_phone(#1.4), n_name(#4.1), c_address(#1.2), c_comment(#1.7) ]
            ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_name(#1.1), c_phone(#1.4), n_name(#4.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: c_nationkey(#1.3) = n_nationkey(#4.0)
                ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_returnflag(#3.8), n_name(#4.1), n_nationkey(#4.0), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: o_orderkey(#2.0) = l_orderkey(#3.0)
                │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_returnflag(#3.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0)
                │   ├── (.cardinality): 0.00
                │   ├── Join
                │   │   ├── .join_type: Inner
                │   │   ├── .implementation: None
                │   │   ├── .join_cond: c_custkey(#1.0) = o_custkey(#2.1)
                │   │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0)
                │   │   ├── (.cardinality): 0.00
                │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
                │   │   └── Select { .predicate: (o_orderdate(#2.4) >= 1993-07-01::date32) AND (o_orderdate(#2.4) < 1993-10-01::date32), (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), (.cardinality): 0.00 }
                │   │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), (.cardinality): 0.00 }
                │   └── Select { .predicate: l_returnflag(#3.8) = R::utf8_view, (.output_columns): l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_returnflag(#3.8), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_returnflag(#3.8), (.cardinality): 0.00 }
                └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): n_name(#4.1), n_nationkey(#4.0), (.cardinality): 0.00 }
*/

