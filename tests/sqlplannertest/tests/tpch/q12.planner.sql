-- TPC-H Q12
SELECT
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT'
             or o_orderpriority = '2-HIGH'
             then 1 else 0 end) as high_priority_orders,
    sum(case when o_orderpriority <> '1-URGENT'
             and o_orderpriority <> '2-HIGH'
             then 1 else 0 end) as low_priority_orders
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode in ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1995-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ l_shipmode(#4.0) ASC ], (.output_columns): high_priority_orders(#4.1), l_shipmode(#4.0), low_priority_orders(#4.2), (.cardinality): 0.00 }
└── Project { .table_index: 4, .projections: [ l_shipmode(#2.14), sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.0), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.1) ], (.output_columns): high_priority_orders(#4.1), l_shipmode(#4.0), low_priority_orders(#4.2), (.cardinality): 0.00 }
    └── Aggregate
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs: [ sum(CASE WHEN (o_orderpriority(#1.5) = 1-URGENT::utf8_view) OR (o_orderpriority(#1.5) = 2-HIGH::utf8_view) THEN 1::bigint ELSE 0::bigint END), sum(CASE WHEN (o_orderpriority(#1.5) != 1-URGENT::utf8_view) AND (o_orderpriority(#1.5) != 2-HIGH::utf8_view) THEN 1::bigint ELSE 0::bigint END) ]
        ├── .keys: [ l_shipmode(#2.14) ]
        ├── (.output_columns): l_shipmode(#2.14), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.1), sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.0)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: (o_orderkey(#1.0) = l_orderkey(#2.0))
            ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), o_clerk(#1.6), o_comment(#1.8), o_custkey(#1.1), o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), o_orderstatus(#1.2), o_shippriority(#1.7), o_totalprice(#1.3)
            ├── (.cardinality): 0.00
            ├── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): o_clerk(#1.6), o_comment(#1.8), o_custkey(#1.1), o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), o_orderstatus(#1.2), o_shippriority(#1.7), o_totalprice(#1.3), (.cardinality): 0.00 }
            └── Select
                ├── .predicate: ((l_shipmode(#2.14) = MAIL::utf8_view) OR (l_shipmode(#2.14) = SHIP::utf8_view)) AND (l_receiptdate(#2.12) > l_commitdate(#2.11)) AND (l_shipdate(#2.10) < l_commitdate(#2.11)) AND (l_receiptdate(#2.12) >= 1994-01-01::date32) AND (l_receiptdate(#2.12) < 1995-01-01::date32)
                ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
                ├── (.cardinality): 0.00
                └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#4.0, Asc)], (.output_columns): high_priority_orders(#4.1), l_shipmode(#4.0), low_priority_orders(#4.2), (.cardinality): 0.00 }
└── Project
    ├── .table_index: 4
    ├── .projections: [ l_shipmode(#2.14), sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.0), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.1) ]
    ├── (.output_columns): high_priority_orders(#4.1), l_shipmode(#4.0), low_priority_orders(#4.2)
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs: [ sum(CASE WHEN (o_orderpriority(#1.5) = 1-URGENT::utf8_view) OR (o_orderpriority(#1.5) = 2-HIGH::utf8_view) THEN 1::bigint ELSE 0::bigint END), sum(CASE WHEN (o_orderpriority(#1.5) != 1-URGENT::utf8_view) AND (o_orderpriority(#1.5) != 2-HIGH::utf8_view) THEN 1::bigint ELSE 0::bigint END) ]
        ├── .keys: [ l_shipmode(#2.14) ]
        ├── (.output_columns): l_shipmode(#2.14), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.1), sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)(#3.0)
        ├── (.cardinality): 0.00
        └── Join { .join_type: Inner, .implementation: None, .join_cond: o_orderkey(#1.0) = l_orderkey(#2.0), (.output_columns): l_commitdate(#2.11), l_orderkey(#2.0), l_receiptdate(#2.12), l_shipdate(#2.10), l_shipmode(#2.14), o_orderkey(#1.0), o_orderpriority(#1.5), (.cardinality): 0.00 }
            ├── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): o_orderkey(#1.0), o_orderpriority(#1.5), (.cardinality): 0.00 }
            └── Select
                ├── .predicate: ((l_shipmode(#2.14) = MAIL::utf8_view) OR (l_shipmode(#2.14) = SHIP::utf8_view)) AND (l_receiptdate(#2.12) > l_commitdate(#2.11)) AND (l_shipdate(#2.10) < l_commitdate(#2.11)) AND (l_receiptdate(#2.12) >= 1994-01-01::date32) AND (l_receiptdate(#2.12) < 1995-01-01::date32)
                ├── (.output_columns): l_commitdate(#2.11), l_orderkey(#2.0), l_receiptdate(#2.12), l_shipdate(#2.10), l_shipmode(#2.14)
                ├── (.cardinality): 0.00
                └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_commitdate(#2.11), l_orderkey(#2.0), l_receiptdate(#2.12), l_shipdate(#2.10), l_shipmode(#2.14), (.cardinality): 0.00 }
*/

