-- TPC-H Q14
SELECT
    100.00 * sum(case when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;

/*
logical_plan after optd-initial:
Project
├── .table_index: 5
├── .projections: 100::float64 * CAST ("__#4.sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)"(#4.0) AS Float64) / CAST ("__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.1) AS Float64)
├── (.output_columns): "__#5.promo_revenue"(#5.0)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 3
    ├── .aggregate_table_index: 4
    ├── .implementation: None
    ├── .exprs:
    │   ┌── sum(CASE WHEN "part.p_type"(#2.4) LIKE CAST ('PROMO%'::utf8 AS Utf8View) THEN "lineitem.l_extendedprice"(#1.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#1.6) ELSE CAST (0::bigint AS Decimal128(38, 4)) END)
    │   └── sum("lineitem.l_extendedprice"(#1.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#1.6))
    ├── .keys: []
    ├── (.output_columns): [ "__#4.sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)"(#4.0), "__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.1) ]
    ├── (.cardinality): 1.00
    └── Select
        ├── .predicate: ("lineitem.l_partkey"(#1.1) = "part.p_partkey"(#2.0)) AND ("lineitem.l_shipdate"(#1.10) >= CAST ('1995-09-01'::utf8 AS Date32)) AND ("lineitem.l_shipdate"(#1.10) < CAST ('1995-09-01'::utf8 AS Date32) + IntervalMonthDayNano { months: 1, days: 0, nanoseconds: 0 }::interval_month_day_nano)
        ├── (.output_columns):
        │   ┌── "lineitem.l_comment"(#1.15)
        │   ├── "lineitem.l_commitdate"(#1.11)
        │   ├── "lineitem.l_discount"(#1.6)
        │   ├── "lineitem.l_extendedprice"(#1.5)
        │   ├── "lineitem.l_linenumber"(#1.3)
        │   ├── "lineitem.l_linestatus"(#1.9)
        │   ├── "lineitem.l_orderkey"(#1.0)
        │   ├── "lineitem.l_partkey"(#1.1)
        │   ├── "lineitem.l_quantity"(#1.4)
        │   ├── "lineitem.l_receiptdate"(#1.12)
        │   ├── "lineitem.l_returnflag"(#1.8)
        │   ├── "lineitem.l_shipdate"(#1.10)
        │   ├── "lineitem.l_shipinstruct"(#1.13)
        │   ├── "lineitem.l_shipmode"(#1.14)
        │   ├── "lineitem.l_suppkey"(#1.2)
        │   ├── "lineitem.l_tax"(#1.7)
        │   ├── "part.p_brand"(#2.3)
        │   ├── "part.p_comment"(#2.8)
        │   ├── "part.p_container"(#2.6)
        │   ├── "part.p_mfgr"(#2.2)
        │   ├── "part.p_name"(#2.1)
        │   ├── "part.p_partkey"(#2.0)
        │   ├── "part.p_retailprice"(#2.7)
        │   ├── "part.p_size"(#2.5)
        │   └── "part.p_type"(#2.4)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: 
            ├── (.output_columns):
            │   ┌── "lineitem.l_comment"(#1.15)
            │   ├── "lineitem.l_commitdate"(#1.11)
            │   ├── "lineitem.l_discount"(#1.6)
            │   ├── "lineitem.l_extendedprice"(#1.5)
            │   ├── "lineitem.l_linenumber"(#1.3)
            │   ├── "lineitem.l_linestatus"(#1.9)
            │   ├── "lineitem.l_orderkey"(#1.0)
            │   ├── "lineitem.l_partkey"(#1.1)
            │   ├── "lineitem.l_quantity"(#1.4)
            │   ├── "lineitem.l_receiptdate"(#1.12)
            │   ├── "lineitem.l_returnflag"(#1.8)
            │   ├── "lineitem.l_shipdate"(#1.10)
            │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   ├── "lineitem.l_shipmode"(#1.14)
            │   ├── "lineitem.l_suppkey"(#1.2)
            │   ├── "lineitem.l_tax"(#1.7)
            │   ├── "part.p_brand"(#2.3)
            │   ├── "part.p_comment"(#2.8)
            │   ├── "part.p_container"(#2.6)
            │   ├── "part.p_mfgr"(#2.2)
            │   ├── "part.p_name"(#2.1)
            │   ├── "part.p_partkey"(#2.0)
            │   ├── "part.p_retailprice"(#2.7)
            │   ├── "part.p_size"(#2.5)
            │   └── "part.p_type"(#2.4)
            ├── (.cardinality): 0.00
            ├── Get
            │   ├── .data_source_id: 8
            │   ├── .table_index: 1
            │   ├── .implementation: None
            │   ├── (.output_columns):
            │   │   ┌── "lineitem.l_comment"(#1.15)
            │   │   ├── "lineitem.l_commitdate"(#1.11)
            │   │   ├── "lineitem.l_discount"(#1.6)
            │   │   ├── "lineitem.l_extendedprice"(#1.5)
            │   │   ├── "lineitem.l_linenumber"(#1.3)
            │   │   ├── "lineitem.l_linestatus"(#1.9)
            │   │   ├── "lineitem.l_orderkey"(#1.0)
            │   │   ├── "lineitem.l_partkey"(#1.1)
            │   │   ├── "lineitem.l_quantity"(#1.4)
            │   │   ├── "lineitem.l_receiptdate"(#1.12)
            │   │   ├── "lineitem.l_returnflag"(#1.8)
            │   │   ├── "lineitem.l_shipdate"(#1.10)
            │   │   ├── "lineitem.l_shipinstruct"(#1.13)
            │   │   ├── "lineitem.l_shipmode"(#1.14)
            │   │   ├── "lineitem.l_suppkey"(#1.2)
            │   │   └── "lineitem.l_tax"(#1.7)
            │   └── (.cardinality): 0.00
            └── Get
                ├── .data_source_id: 3
                ├── .table_index: 2
                ├── .implementation: None
                ├── (.output_columns): [ "part.p_brand"(#2.3), "part.p_comment"(#2.8), "part.p_container"(#2.6), "part.p_mfgr"(#2.2), "part.p_name"(#2.1), "part.p_partkey"(#2.0), "part.p_retailprice"(#2.7), "part.p_size"(#2.5), "part.p_type"(#2.4) ]
                └── (.cardinality): 0.00

physical_plan after optd-finalized:
Project
├── .table_index: 5
├── .projections: 100::float64 * CAST ("__#4.sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)"(#4.0) AS Float64) / CAST ("__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.1) AS Float64)
├── (.output_columns): "__#5.promo_revenue"(#5.0)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 3
    ├── .aggregate_table_index: 4
    ├── .implementation: None
    ├── .exprs:
    │   ┌── sum(CASE WHEN "part.p_type"(#2.4) LIKE 'PROMO%'::utf8_view THEN "lineitem.l_extendedprice"(#1.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#1.6) ELSE 0::decimal128(38, 4) END)
    │   └── sum("lineitem.l_extendedprice"(#1.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#1.6))
    ├── .keys: []
    ├── (.output_columns): [ "__#4.sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)"(#4.0), "__#4.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#4.1) ]
    ├── (.cardinality): 1.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: "lineitem.l_partkey"(#1.1) = "part.p_partkey"(#2.0)
        ├── (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_partkey"(#1.1), "lineitem.l_shipdate"(#1.10), "part.p_partkey"(#2.0), "part.p_type"(#2.4) ]
        ├── (.cardinality): 0.00
        ├── Select
        │   ├── .predicate: ("lineitem.l_shipdate"(#1.10) >= 1995-09-01::date32) AND ("lineitem.l_shipdate"(#1.10) < 1995-10-01::date32)
        │   ├── (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_partkey"(#1.1), "lineitem.l_shipdate"(#1.10) ]
        │   ├── (.cardinality): 0.00
        │   └── Get { .data_source_id: 8, .table_index: 1, .implementation: None, (.output_columns): [ "lineitem.l_discount"(#1.6), "lineitem.l_extendedprice"(#1.5), "lineitem.l_partkey"(#1.1), "lineitem.l_shipdate"(#1.10) ], (.cardinality): 0.00 }
        └── Get { .data_source_id: 3, .table_index: 2, .implementation: None, (.output_columns): [ "part.p_partkey"(#2.0), "part.p_type"(#2.4) ], (.cardinality): 0.00 }

NULL
*/

