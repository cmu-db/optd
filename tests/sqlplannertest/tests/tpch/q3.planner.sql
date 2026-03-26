-- TPC-H Q3
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority 
FROM
    customer,
    orders,
    lineitem 
WHERE
    c_mktsegment = 'FURNITURE' 
    AND c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND o_orderdate < DATE '1995-03-29' 
    AND l_shipdate > DATE '1995-03-29' 
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority 
ORDER BY
    revenue DESC,
    o_orderdate LIMIT 10;

/*
logical_plan after optd-initial:
Limit { .skip: 0::bigint, .fetch: 10::bigint, (.output_columns): l_orderkey(#6.0), o_orderdate(#6.2), o_shippriority(#6.3), revenue(#6.1), (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ revenue(#6.1) DESC, o_orderdate(#6.2) ASC ], (.output_columns): l_orderkey(#6.0), o_orderdate(#6.2), o_shippriority(#6.3), revenue(#6.1), (.cardinality): 0.00 }
    └── Project { .table_index: 6, .projections: [ l_orderkey(#3.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0), o_orderdate(#2.4), o_shippriority(#2.7) ], (.output_columns): l_orderkey(#6.0), o_orderdate(#6.2), o_shippriority(#6.3), revenue(#6.1), (.cardinality): 0.00 }
        └── Aggregate { .key_table_index: 4, .aggregate_table_index: 5, .implementation: None, .exprs: sum(l_extendedprice(#3.5) * 1::decimal128(20, 0) - l_discount(#3.6)), .keys: [ l_orderkey(#3.0), o_orderdate(#2.4), o_shippriority(#2.7) ], (.output_columns): lineitem.l_orderkey(#4.0), orders.o_orderdate(#4.1), orders.o_shippriority(#4.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0), (.cardinality): 0.00 }
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: (o_orderkey(#2.0) = l_orderkey(#3.0))
                ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                ├── (.cardinality): 0.00
                ├── Join { .join_type: Inner, .implementation: None, .join_cond: (c_custkey(#1.0) = o_custkey(#2.1)), (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │   ├── Select { .predicate: c_mktsegment(#1.6) = FURNITURE::utf8_view, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
                │   │   └── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
                │   └── Select { .predicate: o_orderdate(#2.4) < 1995-03-29::date32, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                └── Select { .predicate: l_shipdate(#3.10) > 1995-03-29::date32, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }
                    └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 10::bigint, (.output_columns): l_orderkey(#6.0), o_orderdate(#6.2), o_shippriority(#6.3), revenue(#6.1), (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#6.1, Desc), (#6.2, Asc)], (.output_columns): l_orderkey(#6.0), o_orderdate(#6.2), o_shippriority(#6.3), revenue(#6.1), (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 6
        ├── .projections: [ l_orderkey(#3.0), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0), o_orderdate(#2.4), o_shippriority(#2.7) ]
        ├── (.output_columns): l_orderkey(#6.0), o_orderdate(#6.2), o_shippriority(#6.3), revenue(#6.1)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 4
            ├── .aggregate_table_index: 5
            ├── .implementation: None
            ├── .exprs: sum(l_extendedprice(#3.5) * 1::decimal128(20, 0) - l_discount(#3.6))
            ├── .keys: [ l_orderkey(#3.0), o_orderdate(#2.4), o_shippriority(#2.7) ]
            ├── (.output_columns): lineitem.l_orderkey(#4.0), orders.o_orderdate(#4.1), orders.o_shippriority(#4.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#5.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: o_orderkey(#2.0) = l_orderkey(#3.0)
                ├── (.output_columns): c_custkey(#1.0), c_mktsegment(#1.6), l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_shipdate(#3.10), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_shippriority(#2.7)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: c_custkey(#1.0) = o_custkey(#2.1)
                │   ├── (.output_columns): c_custkey(#1.0), c_mktsegment(#1.6), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_shippriority(#2.7)
                │   ├── (.cardinality): 0.00
                │   ├── Select { .predicate: c_mktsegment(#1.6) = FURNITURE::utf8_view, (.output_columns): c_custkey(#1.0), c_mktsegment(#1.6), (.cardinality): 0.00 }
                │   │   └── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_custkey(#1.0), c_mktsegment(#1.6), (.cardinality): 0.00 }
                │   └── Select { .predicate: o_orderdate(#2.4) < 1995-03-29::date32, (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_shippriority(#2.7), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_shippriority(#2.7), (.cardinality): 0.00 }
                └── Select { .predicate: l_shipdate(#3.10) > 1995-03-29::date32, (.output_columns): l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_shipdate(#3.10), (.cardinality): 0.00 }
                    └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_discount(#3.6), l_extendedprice(#3.5), l_orderkey(#3.0), l_shipdate(#3.10), (.cardinality): 0.00 }
*/

