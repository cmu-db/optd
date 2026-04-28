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
OrderBy { ordering_exprs: "__#5.l_shipmode"(#5.0) ASC, (.output_columns): [ "__#5.high_priority_orders"(#5.1), "__#5.l_shipmode"(#5.0), "__#5.low_priority_orders"(#5.2) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 5
    ├── .projections: [ "lineitem.l_shipmode"(#2.14), "__#4.sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.0), "__#4.sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.1) ]
    ├── (.output_columns): [ "__#5.high_priority_orders"(#5.1), "__#5.l_shipmode"(#5.0), "__#5.low_priority_orders"(#5.2) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 3
        ├── .aggregate_table_index: 4
        ├── .implementation: None
        ├── .exprs: [ sum(CASE WHEN ("orders.o_orderpriority"(#1.5) = '1-URGENT'::utf8_view) OR ("orders.o_orderpriority"(#1.5) = '2-HIGH'::utf8_view) THEN 1::bigint ELSE 0::bigint END), sum(CASE WHEN ("orders.o_orderpriority"(#1.5) != '1-URGENT'::utf8_view) AND ("orders.o_orderpriority"(#1.5) != '2-HIGH'::utf8_view) THEN 1::bigint ELSE 0::bigint END) ]
        ├── .keys: "lineitem.l_shipmode"(#2.14)
        ├── (.output_columns): [ "__#3.l_shipmode"(#3.0), "__#4.sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.1), "__#4.sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.0) ]
        ├── (.cardinality): 0.00
        └── Select
            ├── .predicate: (("lineitem.l_shipmode"(#2.14) = 'MAIL'::utf8_view) OR ("lineitem.l_shipmode"(#2.14) = 'SHIP'::utf8_view)) AND ("lineitem.l_receiptdate"(#2.12) > "lineitem.l_commitdate"(#2.11)) AND ("lineitem.l_shipdate"(#2.10) < "lineitem.l_commitdate"(#2.11)) AND ("lineitem.l_receiptdate"(#2.12) >= 1994-01-01::date32) AND ("lineitem.l_receiptdate"(#2.12) < 1995-01-01::date32)
            ├── (.output_columns):
            │   ┌── "lineitem.l_comment"(#2.15)
            │   ├── "lineitem.l_commitdate"(#2.11)
            │   ├── "lineitem.l_discount"(#2.6)
            │   ├── "lineitem.l_extendedprice"(#2.5)
            │   ├── "lineitem.l_linenumber"(#2.3)
            │   ├── "lineitem.l_linestatus"(#2.9)
            │   ├── "lineitem.l_orderkey"(#2.0)
            │   ├── "lineitem.l_partkey"(#2.1)
            │   ├── "lineitem.l_quantity"(#2.4)
            │   ├── "lineitem.l_receiptdate"(#2.12)
            │   ├── "lineitem.l_returnflag"(#2.8)
            │   ├── "lineitem.l_shipdate"(#2.10)
            │   ├── "lineitem.l_shipinstruct"(#2.13)
            │   ├── "lineitem.l_shipmode"(#2.14)
            │   ├── "lineitem.l_suppkey"(#2.2)
            │   ├── "lineitem.l_tax"(#2.7)
            │   ├── "orders.o_clerk"(#1.6)
            │   ├── "orders.o_comment"(#1.8)
            │   ├── "orders.o_custkey"(#1.1)
            │   ├── "orders.o_orderdate"(#1.4)
            │   ├── "orders.o_orderkey"(#1.0)
            │   ├── "orders.o_orderpriority"(#1.5)
            │   ├── "orders.o_orderstatus"(#1.2)
            │   ├── "orders.o_shippriority"(#1.7)
            │   └── "orders.o_totalprice"(#1.3)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: ("orders.o_orderkey"(#1.0) = "lineitem.l_orderkey"(#2.0))
                ├── (.output_columns):
                │   ┌── "lineitem.l_comment"(#2.15)
                │   ├── "lineitem.l_commitdate"(#2.11)
                │   ├── "lineitem.l_discount"(#2.6)
                │   ├── "lineitem.l_extendedprice"(#2.5)
                │   ├── "lineitem.l_linenumber"(#2.3)
                │   ├── "lineitem.l_linestatus"(#2.9)
                │   ├── "lineitem.l_orderkey"(#2.0)
                │   ├── "lineitem.l_partkey"(#2.1)
                │   ├── "lineitem.l_quantity"(#2.4)
                │   ├── "lineitem.l_receiptdate"(#2.12)
                │   ├── "lineitem.l_returnflag"(#2.8)
                │   ├── "lineitem.l_shipdate"(#2.10)
                │   ├── "lineitem.l_shipinstruct"(#2.13)
                │   ├── "lineitem.l_shipmode"(#2.14)
                │   ├── "lineitem.l_suppkey"(#2.2)
                │   ├── "lineitem.l_tax"(#2.7)
                │   ├── "orders.o_clerk"(#1.6)
                │   ├── "orders.o_comment"(#1.8)
                │   ├── "orders.o_custkey"(#1.1)
                │   ├── "orders.o_orderdate"(#1.4)
                │   ├── "orders.o_orderkey"(#1.0)
                │   ├── "orders.o_orderpriority"(#1.5)
                │   ├── "orders.o_orderstatus"(#1.2)
                │   ├── "orders.o_shippriority"(#1.7)
                │   └── "orders.o_totalprice"(#1.3)
                ├── (.cardinality): 0.00
                ├── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): [ "orders.o_clerk"(#1.6), "orders.o_comment"(#1.8), "orders.o_custkey"(#1.1), "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5), "orders.o_orderstatus"(#1.2), "orders.o_shippriority"(#1.7), "orders.o_totalprice"(#1.3) ], (.cardinality): 0.00 }
                └── Get
                    ├── .data_source_id: 8
                    ├── .table_index: 2
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "lineitem.l_comment"(#2.15)
                    │   ├── "lineitem.l_commitdate"(#2.11)
                    │   ├── "lineitem.l_discount"(#2.6)
                    │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   ├── "lineitem.l_linenumber"(#2.3)
                    │   ├── "lineitem.l_linestatus"(#2.9)
                    │   ├── "lineitem.l_orderkey"(#2.0)
                    │   ├── "lineitem.l_partkey"(#2.1)
                    │   ├── "lineitem.l_quantity"(#2.4)
                    │   ├── "lineitem.l_receiptdate"(#2.12)
                    │   ├── "lineitem.l_returnflag"(#2.8)
                    │   ├── "lineitem.l_shipdate"(#2.10)
                    │   ├── "lineitem.l_shipinstruct"(#2.13)
                    │   ├── "lineitem.l_shipmode"(#2.14)
                    │   ├── "lineitem.l_suppkey"(#2.2)
                    │   └── "lineitem.l_tax"(#2.7)
                    └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy { ordering_exprs: "__#5.l_shipmode"(#5.0) ASC, (.output_columns): [ "__#5.high_priority_orders"(#5.1), "__#5.l_shipmode"(#5.0), "__#5.low_priority_orders"(#5.2) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 5
    ├── .projections: [ "lineitem.l_shipmode"(#2.14), "__#4.sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.0), "__#4.sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.1) ]
    ├── (.output_columns): [ "__#5.high_priority_orders"(#5.1), "__#5.l_shipmode"(#5.0), "__#5.low_priority_orders"(#5.2) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 3
        ├── .aggregate_table_index: 4
        ├── .implementation: None
        ├── .exprs: [ sum(CASE WHEN ("orders.o_orderpriority"(#1.5) = '1-URGENT'::utf8_view) OR ("orders.o_orderpriority"(#1.5) = '2-HIGH'::utf8_view) THEN 1::bigint ELSE 0::bigint END), sum(CASE WHEN ("orders.o_orderpriority"(#1.5) != '1-URGENT'::utf8_view) AND ("orders.o_orderpriority"(#1.5) != '2-HIGH'::utf8_view) THEN 1::bigint ELSE 0::bigint END) ]
        ├── .keys: "lineitem.l_shipmode"(#2.14)
        ├── (.output_columns): [ "__#3.l_shipmode"(#3.0), "__#4.sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.1), "__#4.sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.0) ]
        ├── (.cardinality): 0.00
        └── Join { .join_type: Inner, .implementation: None, .join_cond: "orders.o_orderkey"(#1.0) = "lineitem.l_orderkey"(#2.0), (.output_columns): [ "lineitem.l_commitdate"(#2.11), "lineitem.l_orderkey"(#2.0), "lineitem.l_receiptdate"(#2.12), "lineitem.l_shipdate"(#2.10), "lineitem.l_shipmode"(#2.14), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ], (.cardinality): 0.00 }
            ├── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): [ "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ], (.cardinality): 0.00 }
            └── Select
                ├── .predicate: (("lineitem.l_shipmode"(#2.14) = 'MAIL'::utf8_view) OR ("lineitem.l_shipmode"(#2.14) = 'SHIP'::utf8_view)) AND ("lineitem.l_receiptdate"(#2.12) > "lineitem.l_commitdate"(#2.11)) AND ("lineitem.l_shipdate"(#2.10) < "lineitem.l_commitdate"(#2.11)) AND ("lineitem.l_receiptdate"(#2.12) >= 1994-01-01::date32) AND ("lineitem.l_receiptdate"(#2.12) < 1995-01-01::date32)
                ├── (.output_columns): [ "lineitem.l_commitdate"(#2.11), "lineitem.l_orderkey"(#2.0), "lineitem.l_receiptdate"(#2.12), "lineitem.l_shipdate"(#2.10), "lineitem.l_shipmode"(#2.14) ]
                ├── (.cardinality): 0.00
                └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): [ "lineitem.l_commitdate"(#2.11), "lineitem.l_orderkey"(#2.0), "lineitem.l_receiptdate"(#2.12), "lineitem.l_shipdate"(#2.10), "lineitem.l_shipmode"(#2.14) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#5.0, Asc)], (.output_columns): [ "__#5.high_priority_orders"(#5.1), "__#5.l_shipmode"(#5.0), "__#5.low_priority_orders"(#5.2) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 5
    ├── .projections: [ "lineitem.l_shipmode"(#2.14), "__#4.sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.0), "__#4.sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.1) ]
    ├── (.output_columns): [ "__#5.high_priority_orders"(#5.1), "__#5.l_shipmode"(#5.0), "__#5.low_priority_orders"(#5.2) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 3
        ├── .aggregate_table_index: 4
        ├── .implementation: None
        ├── .exprs: [ sum(CASE WHEN ("orders.o_orderpriority"(#1.5) = '1-URGENT'::utf8_view) OR ("orders.o_orderpriority"(#1.5) = '2-HIGH'::utf8_view) THEN 1::bigint ELSE 0::bigint END), sum(CASE WHEN ("orders.o_orderpriority"(#1.5) != '1-URGENT'::utf8_view) AND ("orders.o_orderpriority"(#1.5) != '2-HIGH'::utf8_view) THEN 1::bigint ELSE 0::bigint END) ]
        ├── .keys: "lineitem.l_shipmode"(#2.14)
        ├── (.output_columns): [ "__#3.l_shipmode"(#3.0), "__#4.sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.1), "__#4.sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)"(#4.0) ]
        ├── (.cardinality): 0.00
        └── Join { .join_type: Inner, .implementation: None, .join_cond: "orders.o_orderkey"(#1.0) = "lineitem.l_orderkey"(#2.0), (.output_columns): [ "lineitem.l_commitdate"(#2.11), "lineitem.l_orderkey"(#2.0), "lineitem.l_receiptdate"(#2.12), "lineitem.l_shipdate"(#2.10), "lineitem.l_shipmode"(#2.14), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ], (.cardinality): 0.00 }
            ├── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): [ "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ], (.cardinality): 0.00 }
            └── Select
                ├── .predicate: (("lineitem.l_shipmode"(#2.14) = 'MAIL'::utf8_view) OR ("lineitem.l_shipmode"(#2.14) = 'SHIP'::utf8_view)) AND ("lineitem.l_receiptdate"(#2.12) > "lineitem.l_commitdate"(#2.11)) AND ("lineitem.l_shipdate"(#2.10) < "lineitem.l_commitdate"(#2.11)) AND ("lineitem.l_receiptdate"(#2.12) >= 1994-01-01::date32) AND ("lineitem.l_receiptdate"(#2.12) < 1995-01-01::date32)
                ├── (.output_columns): [ "lineitem.l_commitdate"(#2.11), "lineitem.l_orderkey"(#2.0), "lineitem.l_receiptdate"(#2.12), "lineitem.l_shipdate"(#2.10), "lineitem.l_shipmode"(#2.14) ]
                ├── (.cardinality): 0.00
                └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): [ "lineitem.l_commitdate"(#2.11), "lineitem.l_orderkey"(#2.0), "lineitem.l_receiptdate"(#2.12), "lineitem.l_shipdate"(#2.10), "lineitem.l_shipmode"(#2.14) ], (.cardinality): 0.00 }
*/

