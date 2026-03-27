-- (no id or description)
WITH revenue0 (supplier_no, total_revenue) AS 
(
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount)) 
    FROM
        lineitem 
    WHERE
        l_shipdate >= DATE '1993-01-01' 
        AND l_shipdate < DATE '1993-01-01' + INTERVAL '3' MONTH 
    GROUP BY
        l_suppkey 
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue 
FROM
    supplier,
    revenue0 
WHERE
    s_suppkey = supplier_no 
    AND total_revenue = 
    (
        SELECT
            MAX(total_revenue) 
        FROM
            revenue0 
    )
ORDER BY
    s_suppkey;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ `__internal_#19`.`s_suppkey`(#19.0) ASC ], (.output_columns): `__internal_#19`.`s_address`(#19.2), `__internal_#19`.`s_name`(#19.1), `__internal_#19`.`s_phone`(#19.3), `__internal_#19`.`s_suppkey`(#19.0), `__internal_#19`.`total_revenue`(#19.4), (.cardinality): 0.00 }
└── Project { .table_index: 19, .projections: [ `__internal_#18`.`s_suppkey`(#18.0), `__internal_#18`.`s_name`(#18.1), `__internal_#18`.`s_address`(#18.2), `__internal_#18`.`s_phone`(#18.4), `__internal_#18`.`total_revenue`(#18.8) ], (.output_columns): `__internal_#19`.`s_address`(#19.2), `__internal_#19`.`s_name`(#19.1), `__internal_#19`.`s_phone`(#19.3), `__internal_#19`.`s_suppkey`(#19.0), `__internal_#19`.`total_revenue`(#19.4), (.cardinality): 0.00 }
    └── Project
        ├── .table_index: 18
        ├── .projections: [ `supplier`.`s_suppkey`(#1.0), `supplier`.`s_name`(#1.1), `supplier`.`s_address`(#1.2), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_comment`(#1.6), `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1) ]
        ├── (.output_columns): `__internal_#18`.`s_acctbal`(#18.5), `__internal_#18`.`s_address`(#18.2), `__internal_#18`.`s_comment`(#18.6), `__internal_#18`.`s_name`(#18.1), `__internal_#18`.`s_nationkey`(#18.3), `__internal_#18`.`s_phone`(#18.4), `__internal_#18`.`s_suppkey`(#18.0), `__internal_#18`.`supplier_no`(#18.7), `__internal_#18`.`total_revenue`(#18.8)
        ├── (.cardinality): 0.00
        └── Join { .join_type: Inner, .implementation: None, .join_cond: (`revenue0`.`total_revenue`(#7.1) = `__scalar_sq_1`.`max(revenue0.total_revenue)`(#17.0)), (.output_columns): `__scalar_sq_1`.`max(revenue0.total_revenue)`(#17.0), `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
            ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`supplier`.`s_suppkey`(#1.0) = `revenue0`.`supplier_no`(#7.0)), (.output_columns): `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
            │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
            │   └── Remap { .table_index: 7, (.output_columns): `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1), (.cardinality): 0.00 }
            │       └── Project { .table_index: 6, .projections: [ `__internal_#5`.`l_suppkey`(#5.0), `__internal_#5`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#5.1) ], (.output_columns): `__internal_#6`.`supplier_no`(#6.0), `__internal_#6`.`total_revenue`(#6.1), (.cardinality): 0.00 }
            │           └── Project { .table_index: 5, .projections: [ `lineitem`.`l_suppkey`(#2.2), `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0) ], (.output_columns): `__internal_#5`.`l_suppkey`(#5.0), `__internal_#5`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#5.1), (.cardinality): 0.00 }
            │               └── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#2.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#2.6)), .keys: `lineitem`.`l_suppkey`(#2.2), (.output_columns): `__internal_#3`.`l_suppkey`(#3.0), `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0), (.cardinality): 0.00 }
            │                   └── Select
            │                       ├── .predicate: (`lineitem`.`l_shipdate`(#2.10) >= 1993-01-01::date32) AND (`lineitem`.`l_shipdate`(#2.10) < 1993-04-01::date32)
            │                       ├── (.output_columns): `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7)
            │                       ├── (.cardinality): 0.00
            │                       └── Get
            │                           ├── .data_source_id: 8
            │                           ├── .table_index: 2
            │                           ├── .implementation: None
            │                           ├── (.output_columns): `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7)
            │                           └── (.cardinality): 0.00
            └── Remap { .table_index: 17, (.output_columns): `__scalar_sq_1`.`max(revenue0.total_revenue)`(#17.0), (.cardinality): 1.00 }
                └── Project { .table_index: 16, .projections: `__internal_#15`.`max(revenue0.total_revenue)`(#15.0), (.output_columns): `__internal_#16`.`max(revenue0.total_revenue)`(#16.0), (.cardinality): 1.00 }
                    └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: max(`revenue0`.`total_revenue`(#13.1)), .keys: [], (.output_columns): `__internal_#15`.`max(revenue0.total_revenue)`(#15.0), (.cardinality): 1.00 }
                        └── Remap { .table_index: 13, (.output_columns): `revenue0`.`supplier_no`(#13.0), `revenue0`.`total_revenue`(#13.1), (.cardinality): 0.00 }
                            └── Project { .table_index: 12, .projections: [ `__internal_#11`.`l_suppkey`(#11.0), `__internal_#11`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#11.1) ], (.output_columns): `__internal_#12`.`supplier_no`(#12.0), `__internal_#12`.`total_revenue`(#12.1), (.cardinality): 0.00 }
                                └── Project { .table_index: 11, .projections: [ `lineitem`.`l_suppkey`(#8.2), `__internal_#10`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#10.0) ], (.output_columns): `__internal_#11`.`l_suppkey`(#11.0), `__internal_#11`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#11.1), (.cardinality): 0.00 }
                                    └── Aggregate { .key_table_index: 9, .aggregate_table_index: 10, .implementation: None, .exprs: sum(`lineitem`.`l_extendedprice`(#8.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#8.6)), .keys: `lineitem`.`l_suppkey`(#8.2), (.output_columns): `__internal_#10`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#10.0), `__internal_#9`.`l_suppkey`(#9.0), (.cardinality): 0.00 }
                                        └── Select
                                            ├── .predicate: (`lineitem`.`l_shipdate`(#8.10) >= 1993-01-01::date32) AND (`lineitem`.`l_shipdate`(#8.10) < 1993-04-01::date32)
                                            ├── (.output_columns): `lineitem`.`l_comment`(#8.15), `lineitem`.`l_commitdate`(#8.11), `lineitem`.`l_discount`(#8.6), `lineitem`.`l_extendedprice`(#8.5), `lineitem`.`l_linenumber`(#8.3), `lineitem`.`l_linestatus`(#8.9), `lineitem`.`l_orderkey`(#8.0), `lineitem`.`l_partkey`(#8.1), `lineitem`.`l_quantity`(#8.4), `lineitem`.`l_receiptdate`(#8.12), `lineitem`.`l_returnflag`(#8.8), `lineitem`.`l_shipdate`(#8.10), `lineitem`.`l_shipinstruct`(#8.13), `lineitem`.`l_shipmode`(#8.14), `lineitem`.`l_suppkey`(#8.2), `lineitem`.`l_tax`(#8.7)
                                            ├── (.cardinality): 0.00
                                            └── Get
                                                ├── .data_source_id: 8
                                                ├── .table_index: 8
                                                ├── .implementation: None
                                                ├── (.output_columns): `lineitem`.`l_comment`(#8.15), `lineitem`.`l_commitdate`(#8.11), `lineitem`.`l_discount`(#8.6), `lineitem`.`l_extendedprice`(#8.5), `lineitem`.`l_linenumber`(#8.3), `lineitem`.`l_linestatus`(#8.9), `lineitem`.`l_orderkey`(#8.0), `lineitem`.`l_partkey`(#8.1), `lineitem`.`l_quantity`(#8.4), `lineitem`.`l_receiptdate`(#8.12), `lineitem`.`l_returnflag`(#8.8), `lineitem`.`l_shipdate`(#8.10), `lineitem`.`l_shipinstruct`(#8.13), `lineitem`.`l_shipmode`(#8.14), `lineitem`.`l_suppkey`(#8.2), `lineitem`.`l_tax`(#8.7)
                                                └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#19.0, Asc)]
├── (.output_columns): `__internal_#19`.`s_address`(#19.2), `__internal_#19`.`s_name`(#19.1), `__internal_#19`.`s_phone`(#19.3), `__internal_#19`.`s_suppkey`(#19.0), `__internal_#19`.`total_revenue`(#19.4)
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 19
    ├── .projections: [ `supplier`.`s_suppkey`(#1.0), `supplier`.`s_name`(#1.1), `supplier`.`s_address`(#1.2), `supplier`.`s_phone`(#1.4), `revenue0`.`total_revenue`(#7.1) ]
    ├── (.output_columns): `__internal_#19`.`s_address`(#19.2), `__internal_#19`.`s_name`(#19.1), `__internal_#19`.`s_phone`(#19.3), `__internal_#19`.`s_suppkey`(#19.0), `__internal_#19`.`total_revenue`(#19.4)
    ├── (.cardinality): 0.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: `revenue0`.`total_revenue`(#7.1) = `__scalar_sq_1`.`max(revenue0.total_revenue)`(#17.0)
        ├── (.output_columns): `__scalar_sq_1`.`max(revenue0.total_revenue)`(#17.0), `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1), `supplier`.`s_address`(#1.2), `supplier`.`s_name`(#1.1), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0)
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: `supplier`.`s_suppkey`(#1.0) = `revenue0`.`supplier_no`(#7.0)
        │   ├── (.output_columns): `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1), `supplier`.`s_address`(#1.2), `supplier`.`s_name`(#1.1), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0)
        │   ├── (.cardinality): 0.00
        │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): `supplier`.`s_address`(#1.2), `supplier`.`s_name`(#1.1), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
        │   └── Remap { .table_index: 7, (.output_columns): `revenue0`.`supplier_no`(#7.0), `revenue0`.`total_revenue`(#7.1), (.cardinality): 0.00 }
        │       └── Project
        │           ├── .table_index: 6
        │           ├── .projections: [ `lineitem`.`l_suppkey`(#2.2), `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0) ]
        │           ├── (.output_columns): `__internal_#6`.`supplier_no`(#6.0), `__internal_#6`.`total_revenue`(#6.1)
        │           ├── (.cardinality): 0.00
        │           └── Aggregate
        │               ├── .key_table_index: 3
        │               ├── .aggregate_table_index: 4
        │               ├── .implementation: None
        │               ├── .exprs: sum(`lineitem`.`l_extendedprice`(#2.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#2.6))
        │               ├── .keys: [ `lineitem`.`l_suppkey`(#2.2) ]
        │               ├── (.output_columns): `__internal_#3`.`l_suppkey`(#3.0), `__internal_#4`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#4.0)
        │               ├── (.cardinality): 0.00
        │               └── Select
        │                   ├── .predicate: (`lineitem`.`l_shipdate`(#2.10) >= 1993-01-01::date32) AND (`lineitem`.`l_shipdate`(#2.10) < 1993-04-01::date32)
        │                   ├── (.output_columns): `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2)
        │                   ├── (.cardinality): 0.00
        │                   └── Get
        │                       ├── .data_source_id: 8
        │                       ├── .table_index: 2
        │                       ├── .implementation: None
        │                       ├── (.output_columns): `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2)
        │                       └── (.cardinality): 0.00
        └── Remap { .table_index: 17, (.output_columns): `__scalar_sq_1`.`max(revenue0.total_revenue)`(#17.0), (.cardinality): 1.00 }
            └── Project { .table_index: 16, .projections: `__internal_#15`.`max(revenue0.total_revenue)`(#15.0), (.output_columns): `__internal_#16`.`max(revenue0.total_revenue)`(#16.0), (.cardinality): 1.00 }
                └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: max(`revenue0`.`total_revenue`(#13.1)), .keys: [], (.output_columns): `__internal_#15`.`max(revenue0.total_revenue)`(#15.0), (.cardinality): 1.00 }
                    └── Remap { .table_index: 13, (.output_columns): `revenue0`.`supplier_no`(#13.0), `revenue0`.`total_revenue`(#13.1), (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 12
                            ├── .projections: [ `lineitem`.`l_suppkey`(#8.2), `__internal_#10`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#10.0) ]
                            ├── (.output_columns): `__internal_#12`.`supplier_no`(#12.0), `__internal_#12`.`total_revenue`(#12.1)
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 9
                                ├── .aggregate_table_index: 10
                                ├── .implementation: None
                                ├── .exprs: sum(`lineitem`.`l_extendedprice`(#8.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#8.6))
                                ├── .keys: `lineitem`.`l_suppkey`(#8.2)
                                ├── (.output_columns): `__internal_#10`.`sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)`(#10.0), `__internal_#9`.`l_suppkey`(#9.0)
                                ├── (.cardinality): 0.00
                                └── Select
                                    ├── .predicate: (`lineitem`.`l_shipdate`(#8.10) >= 1993-01-01::date32) AND (`lineitem`.`l_shipdate`(#8.10) < 1993-04-01::date32)
                                    ├── (.output_columns): `lineitem`.`l_discount`(#8.6), `lineitem`.`l_extendedprice`(#8.5), `lineitem`.`l_shipdate`(#8.10), `lineitem`.`l_suppkey`(#8.2)
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 8
                                        ├── .implementation: None
                                        ├── (.output_columns): `lineitem`.`l_discount`(#8.6), `lineitem`.`l_extendedprice`(#8.5), `lineitem`.`l_shipdate`(#8.10), `lineitem`.`l_suppkey`(#8.2)
                                        └── (.cardinality): 0.00
*/

