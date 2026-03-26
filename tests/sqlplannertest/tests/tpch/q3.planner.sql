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
Limit { .skip: 0::bigint, .fetch: 10::bigint, (.output_columns): __internal_#5.l_orderkey(#5.0), __internal_#5.o_orderdate(#5.2), __internal_#5.o_shippriority(#5.3), __internal_#5.revenue(#5.1), (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ __internal_#5.revenue(#5.1) DESC, __internal_#5.o_orderdate(#5.2) ASC ], (.output_columns): __internal_#5.l_orderkey(#5.0), __internal_#5.o_orderdate(#5.2), __internal_#5.o_shippriority(#5.3), __internal_#5.revenue(#5.1), (.cardinality): 0.00 }
    └── Project { .table_index: 5, .projections: [ lineitem.l_orderkey(#3.0), __internal_#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0), orders.o_orderdate(#2.4), orders.o_shippriority(#2.7) ], (.output_columns): __internal_#5.l_orderkey(#5.0), __internal_#5.o_orderdate(#5.2), __internal_#5.o_shippriority(#5.3), __internal_#5.revenue(#5.1), (.cardinality): 0.00 }
        └── Aggregate { .aggregate_table_index: 4, .implementation: None, .exprs: sum(lineitem.l_extendedprice(#3.5) * CAST (1::bigint AS Decimal128(20, 0)) - lineitem.l_discount(#3.6)), .keys: [ lineitem.l_orderkey(#3.0), orders.o_orderdate(#2.4), orders.o_shippriority(#2.7) ], (.output_columns): __internal_#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0), lineitem.l_orderkey(#3.0), orders.o_orderdate(#2.4), orders.o_shippriority(#2.7), (.cardinality): 0.00 }
            └── Select
                ├── .predicate: (customer.c_mktsegment(#1.6) = CAST (FURNITURE::utf8 AS Utf8View)) AND (customer.c_custkey(#1.0) = orders.o_custkey(#2.1)) AND (lineitem.l_orderkey(#3.0) = orders.o_orderkey(#2.0)) AND (orders.o_orderdate(#2.4) < CAST (1995-03-29::utf8 AS Date32)) AND (lineitem.l_shipdate(#3.10) > CAST (1995-03-29::utf8 AS Date32))
                ├── (.output_columns): customer.c_acctbal(#1.5), customer.c_address(#1.2), customer.c_comment(#1.7), customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), customer.c_name(#1.1), customer.c_nationkey(#1.3), customer.c_phone(#1.4), lineitem.l_comment(#3.15), lineitem.l_commitdate(#3.11), lineitem.l_discount(#3.6), lineitem.l_extendedprice(#3.5), lineitem.l_linenumber(#3.3), lineitem.l_linestatus(#3.9), lineitem.l_orderkey(#3.0), lineitem.l_partkey(#3.1), lineitem.l_quantity(#3.4), lineitem.l_receiptdate(#3.12), lineitem.l_returnflag(#3.8), lineitem.l_shipdate(#3.10), lineitem.l_shipinstruct(#3.13), lineitem.l_shipmode(#3.14), lineitem.l_suppkey(#3.2), lineitem.l_tax(#3.7), orders.o_clerk(#2.6), orders.o_comment(#2.8), orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_orderpriority(#2.5), orders.o_orderstatus(#2.2), orders.o_shippriority(#2.7), orders.o_totalprice(#2.3)
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: 
                    ├── (.output_columns): customer.c_acctbal(#1.5), customer.c_address(#1.2), customer.c_comment(#1.7), customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), customer.c_name(#1.1), customer.c_nationkey(#1.3), customer.c_phone(#1.4), lineitem.l_comment(#3.15), lineitem.l_commitdate(#3.11), lineitem.l_discount(#3.6), lineitem.l_extendedprice(#3.5), lineitem.l_linenumber(#3.3), lineitem.l_linestatus(#3.9), lineitem.l_orderkey(#3.0), lineitem.l_partkey(#3.1), lineitem.l_quantity(#3.4), lineitem.l_receiptdate(#3.12), lineitem.l_returnflag(#3.8), lineitem.l_shipdate(#3.10), lineitem.l_shipinstruct(#3.13), lineitem.l_shipmode(#3.14), lineitem.l_suppkey(#3.2), lineitem.l_tax(#3.7), orders.o_clerk(#2.6), orders.o_comment(#2.8), orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_orderpriority(#2.5), orders.o_orderstatus(#2.2), orders.o_shippriority(#2.7), orders.o_totalprice(#2.3)
                    ├── (.cardinality): 0.00
                    ├── Join { .join_type: Inner, .implementation: None, .join_cond: , (.output_columns): customer.c_acctbal(#1.5), customer.c_address(#1.2), customer.c_comment(#1.7), customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), customer.c_name(#1.1), customer.c_nationkey(#1.3), customer.c_phone(#1.4), orders.o_clerk(#2.6), orders.o_comment(#2.8), orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_orderpriority(#2.5), orders.o_orderstatus(#2.2), orders.o_shippriority(#2.7), orders.o_totalprice(#2.3), (.cardinality): 0.00 }
                    │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): customer.c_acctbal(#1.5), customer.c_address(#1.2), customer.c_comment(#1.7), customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), customer.c_name(#1.1), customer.c_nationkey(#1.3), customer.c_phone(#1.4), (.cardinality): 0.00 }
                    │   └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): orders.o_clerk(#2.6), orders.o_comment(#2.8), orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_orderpriority(#2.5), orders.o_orderstatus(#2.2), orders.o_shippriority(#2.7), orders.o_totalprice(#2.3), (.cardinality): 0.00 }
                    └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): lineitem.l_comment(#3.15), lineitem.l_commitdate(#3.11), lineitem.l_discount(#3.6), lineitem.l_extendedprice(#3.5), lineitem.l_linenumber(#3.3), lineitem.l_linestatus(#3.9), lineitem.l_orderkey(#3.0), lineitem.l_partkey(#3.1), lineitem.l_quantity(#3.4), lineitem.l_receiptdate(#3.12), lineitem.l_returnflag(#3.8), lineitem.l_shipdate(#3.10), lineitem.l_shipinstruct(#3.13), lineitem.l_shipmode(#3.14), lineitem.l_suppkey(#3.2), lineitem.l_tax(#3.7), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 10::bigint, (.output_columns): __internal_#5.l_orderkey(#5.0), __internal_#5.o_orderdate(#5.2), __internal_#5.o_shippriority(#5.3), __internal_#5.revenue(#5.1), (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#5.1, Desc), (#5.2, Asc)], (.output_columns): __internal_#5.l_orderkey(#5.0), __internal_#5.o_orderdate(#5.2), __internal_#5.o_shippriority(#5.3), __internal_#5.revenue(#5.1), (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 5
        ├── .projections: [ lineitem.l_orderkey(#3.0), __internal_#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0), orders.o_orderdate(#2.4), orders.o_shippriority(#2.7) ]
        ├── (.output_columns): __internal_#5.l_orderkey(#5.0), __internal_#5.o_orderdate(#5.2), __internal_#5.o_shippriority(#5.3), __internal_#5.revenue(#5.1)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .aggregate_table_index: 4
            ├── .implementation: None
            ├── .exprs: sum(lineitem.l_extendedprice(#3.5) * CAST (1::bigint AS Decimal128(20, 0)) - lineitem.l_discount(#3.6))
            ├── .keys: [ lineitem.l_orderkey(#3.0), orders.o_orderdate(#2.4), orders.o_shippriority(#2.7) ]
            ├── (.output_columns): __internal_#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#4.0), lineitem.l_orderkey(#3.0), orders.o_orderdate(#2.4), orders.o_shippriority(#2.7)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: lineitem.l_orderkey(#3.0) = orders.o_orderkey(#2.0)
                ├── (.output_columns): customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), lineitem.l_discount(#3.6), lineitem.l_extendedprice(#3.5), lineitem.l_orderkey(#3.0), lineitem.l_shipdate(#3.10), orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_shippriority(#2.7)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: customer.c_custkey(#1.0) = orders.o_custkey(#2.1)
                │   ├── (.output_columns): customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_shippriority(#2.7)
                │   ├── (.cardinality): 0.00
                │   ├── Select { .predicate: customer.c_mktsegment(#1.6) = CAST (FURNITURE::utf8 AS Utf8View), (.output_columns): customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), (.cardinality): 0.00 }
                │   │   └── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): customer.c_custkey(#1.0), customer.c_mktsegment(#1.6), (.cardinality): 0.00 }
                │   └── Select { .predicate: orders.o_orderdate(#2.4) < CAST (1995-03-29::utf8 AS Date32), (.output_columns): orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_shippriority(#2.7), (.cardinality): 0.00 }
                │       └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): orders.o_custkey(#2.1), orders.o_orderdate(#2.4), orders.o_orderkey(#2.0), orders.o_shippriority(#2.7), (.cardinality): 0.00 }
                └── Select { .predicate: lineitem.l_shipdate(#3.10) > CAST (1995-03-29::utf8 AS Date32), (.output_columns): lineitem.l_discount(#3.6), lineitem.l_extendedprice(#3.5), lineitem.l_orderkey(#3.0), lineitem.l_shipdate(#3.10), (.cardinality): 0.00 }
                    └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): lineitem.l_discount(#3.6), lineitem.l_extendedprice(#3.5), lineitem.l_orderkey(#3.0), lineitem.l_shipdate(#3.10), (.cardinality): 0.00 }
*/

