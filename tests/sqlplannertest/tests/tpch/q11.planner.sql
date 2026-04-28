-- TPC-H Q11
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'CHINA'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'CHINA'
        )
order by
    value desc;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: "__#14.value"(#14.1) DESC, (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.value"(#14.1) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 14
    ├── .projections: [ "__#13.ps_partkey"(#13.0), "__#13.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#13.1) ]
    ├── (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.value"(#14.1) ]
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 13
        ├── .projections: [ "partsupp.ps_partkey"(#1.0), "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) ]
        ├── (.output_columns): [ "__#13.ps_partkey"(#13.0), "__#13.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#13.1) ]
        ├── (.cardinality): 0.00
        └── Select
            ├── .predicate: CAST ("__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) AS Decimal128(38, 15)) > "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
            ├── (.output_columns):
            │   ┌── "__#4.ps_partkey"(#4.0)
            │   ├── "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0)
            │   └── "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: Inner
                ├── .implementation: None
                ├── .join_cond: 
                ├── (.output_columns):
                │   ┌── "__#4.ps_partkey"(#4.0)
                │   ├── "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0)
                │   └── "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
                ├── (.cardinality): 0.00
                ├── Aggregate
                │   ├── .key_table_index: 4
                │   ├── .aggregate_table_index: 5
                │   ├── .implementation: None
                │   ├── .exprs: sum("partsupp.ps_supplycost"(#1.3) * CAST ("partsupp.ps_availqty"(#1.2) AS Decimal128(10, 0)))
                │   ├── .keys: "partsupp.ps_partkey"(#1.0)
                │   ├── (.output_columns): [ "__#4.ps_partkey"(#4.0), "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) ]
                │   ├── (.cardinality): 0.00
                │   └── Select
                │       ├── .predicate: "nation.n_name"(#3.1) = 'CHINA'::utf8_view
                │       ├── (.output_columns):
                │       │   ┌── "nation.n_comment"(#3.3)
                │       │   ├── "nation.n_name"(#3.1)
                │       │   ├── "nation.n_nationkey"(#3.0)
                │       │   ├── "nation.n_regionkey"(#3.2)
                │       │   ├── "partsupp.ps_availqty"(#1.2)
                │       │   ├── "partsupp.ps_comment"(#1.4)
                │       │   ├── "partsupp.ps_partkey"(#1.0)
                │       │   ├── "partsupp.ps_suppkey"(#1.1)
                │       │   ├── "partsupp.ps_supplycost"(#1.3)
                │       │   ├── "supplier.s_acctbal"(#2.5)
                │       │   ├── "supplier.s_address"(#2.2)
                │       │   ├── "supplier.s_comment"(#2.6)
                │       │   ├── "supplier.s_name"(#2.1)
                │       │   ├── "supplier.s_nationkey"(#2.3)
                │       │   ├── "supplier.s_phone"(#2.4)
                │       │   └── "supplier.s_suppkey"(#2.0)
                │       ├── (.cardinality): 0.00
                │       └── Join
                │           ├── .join_type: Inner
                │           ├── .implementation: None
                │           ├── .join_cond: ("supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#3.0))
                │           ├── (.output_columns):
                │           │   ┌── "nation.n_comment"(#3.3)
                │           │   ├── "nation.n_name"(#3.1)
                │           │   ├── "nation.n_nationkey"(#3.0)
                │           │   ├── "nation.n_regionkey"(#3.2)
                │           │   ├── "partsupp.ps_availqty"(#1.2)
                │           │   ├── "partsupp.ps_comment"(#1.4)
                │           │   ├── "partsupp.ps_partkey"(#1.0)
                │           │   ├── "partsupp.ps_suppkey"(#1.1)
                │           │   ├── "partsupp.ps_supplycost"(#1.3)
                │           │   ├── "supplier.s_acctbal"(#2.5)
                │           │   ├── "supplier.s_address"(#2.2)
                │           │   ├── "supplier.s_comment"(#2.6)
                │           │   ├── "supplier.s_name"(#2.1)
                │           │   ├── "supplier.s_nationkey"(#2.3)
                │           │   ├── "supplier.s_phone"(#2.4)
                │           │   └── "supplier.s_suppkey"(#2.0)
                │           ├── (.cardinality): 0.00
                │           ├── Join
                │           │   ├── .join_type: Inner
                │           │   ├── .implementation: None
                │           │   ├── .join_cond: ("partsupp.ps_suppkey"(#1.1) = "supplier.s_suppkey"(#2.0))
                │           │   ├── (.output_columns):
                │           │   │   ┌── "partsupp.ps_availqty"(#1.2)
                │           │   │   ├── "partsupp.ps_comment"(#1.4)
                │           │   │   ├── "partsupp.ps_partkey"(#1.0)
                │           │   │   ├── "partsupp.ps_suppkey"(#1.1)
                │           │   │   ├── "partsupp.ps_supplycost"(#1.3)
                │           │   │   ├── "supplier.s_acctbal"(#2.5)
                │           │   │   ├── "supplier.s_address"(#2.2)
                │           │   │   ├── "supplier.s_comment"(#2.6)
                │           │   │   ├── "supplier.s_name"(#2.1)
                │           │   │   ├── "supplier.s_nationkey"(#2.3)
                │           │   │   ├── "supplier.s_phone"(#2.4)
                │           │   │   └── "supplier.s_suppkey"(#2.0)
                │           │   ├── (.cardinality): 0.00
                │           │   ├── Get
                │           │   │   ├── .data_source_id: 5
                │           │   │   ├── .table_index: 1
                │           │   │   ├── .implementation: None
                │           │   │   ├── (.output_columns): [ "partsupp.ps_availqty"(#1.2), "partsupp.ps_comment"(#1.4), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1), "partsupp.ps_supplycost"(#1.3) ]
                │           │   │   └── (.cardinality): 0.00
                │           │   └── Get
                │           │       ├── .data_source_id: 4
                │           │       ├── .table_index: 2
                │           │       ├── .implementation: None
                │           │       ├── (.output_columns):
                │           │       │   ┌── "supplier.s_acctbal"(#2.5)
                │           │       │   ├── "supplier.s_address"(#2.2)
                │           │       │   ├── "supplier.s_comment"(#2.6)
                │           │       │   ├── "supplier.s_name"(#2.1)
                │           │       │   ├── "supplier.s_nationkey"(#2.3)
                │           │       │   ├── "supplier.s_phone"(#2.4)
                │           │       │   └── "supplier.s_suppkey"(#2.0)
                │           │       └── (.cardinality): 0.00
                │           └── Get
                │               ├── .data_source_id: 1
                │               ├── .table_index: 3
                │               ├── .implementation: None
                │               ├── (.output_columns): [ "nation.n_comment"(#3.3), "nation.n_name"(#3.1), "nation.n_nationkey"(#3.0), "nation.n_regionkey"(#3.2) ]
                │               └── (.cardinality): 0.00
                └── Remap { .table_index: 12, (.output_columns): "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0), (.cardinality): 1.00 }
                    └── Project
                        ├── .table_index: 11
                        ├── .projections: CAST (CAST ("__#10.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#10.0) AS Float64) * 0.0001::float64 AS Decimal128(38, 15))
                        ├── (.output_columns): "__#11.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#11.0)
                        ├── (.cardinality): 1.00
                        └── Aggregate
                            ├── .key_table_index: 9
                            ├── .aggregate_table_index: 10
                            ├── .implementation: None
                            ├── .exprs: sum("partsupp.ps_supplycost"(#6.3) * CAST ("partsupp.ps_availqty"(#6.2) AS Decimal128(10, 0)))
                            ├── .keys: []
                            ├── (.output_columns): "__#10.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#10.0)
                            ├── (.cardinality): 1.00
                            └── Select
                                ├── .predicate: "nation.n_name"(#8.1) = 'CHINA'::utf8_view
                                ├── (.output_columns):
                                │   ┌── "nation.n_comment"(#8.3)
                                │   ├── "nation.n_name"(#8.1)
                                │   ├── "nation.n_nationkey"(#8.0)
                                │   ├── "nation.n_regionkey"(#8.2)
                                │   ├── "partsupp.ps_availqty"(#6.2)
                                │   ├── "partsupp.ps_comment"(#6.4)
                                │   ├── "partsupp.ps_partkey"(#6.0)
                                │   ├── "partsupp.ps_suppkey"(#6.1)
                                │   ├── "partsupp.ps_supplycost"(#6.3)
                                │   ├── "supplier.s_acctbal"(#7.5)
                                │   ├── "supplier.s_address"(#7.2)
                                │   ├── "supplier.s_comment"(#7.6)
                                │   ├── "supplier.s_name"(#7.1)
                                │   ├── "supplier.s_nationkey"(#7.3)
                                │   ├── "supplier.s_phone"(#7.4)
                                │   └── "supplier.s_suppkey"(#7.0)
                                ├── (.cardinality): 0.00
                                └── Join
                                    ├── .join_type: Inner
                                    ├── .implementation: None
                                    ├── .join_cond: ("supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0))
                                    ├── (.output_columns):
                                    │   ┌── "nation.n_comment"(#8.3)
                                    │   ├── "nation.n_name"(#8.1)
                                    │   ├── "nation.n_nationkey"(#8.0)
                                    │   ├── "nation.n_regionkey"(#8.2)
                                    │   ├── "partsupp.ps_availqty"(#6.2)
                                    │   ├── "partsupp.ps_comment"(#6.4)
                                    │   ├── "partsupp.ps_partkey"(#6.0)
                                    │   ├── "partsupp.ps_suppkey"(#6.1)
                                    │   ├── "partsupp.ps_supplycost"(#6.3)
                                    │   ├── "supplier.s_acctbal"(#7.5)
                                    │   ├── "supplier.s_address"(#7.2)
                                    │   ├── "supplier.s_comment"(#7.6)
                                    │   ├── "supplier.s_name"(#7.1)
                                    │   ├── "supplier.s_nationkey"(#7.3)
                                    │   ├── "supplier.s_phone"(#7.4)
                                    │   └── "supplier.s_suppkey"(#7.0)
                                    ├── (.cardinality): 0.00
                                    ├── Join
                                    │   ├── .join_type: Inner
                                    │   ├── .implementation: None
                                    │   ├── .join_cond: ("partsupp.ps_suppkey"(#6.1) = "supplier.s_suppkey"(#7.0))
                                    │   ├── (.output_columns):
                                    │   │   ┌── "partsupp.ps_availqty"(#6.2)
                                    │   │   ├── "partsupp.ps_comment"(#6.4)
                                    │   │   ├── "partsupp.ps_partkey"(#6.0)
                                    │   │   ├── "partsupp.ps_suppkey"(#6.1)
                                    │   │   ├── "partsupp.ps_supplycost"(#6.3)
                                    │   │   ├── "supplier.s_acctbal"(#7.5)
                                    │   │   ├── "supplier.s_address"(#7.2)
                                    │   │   ├── "supplier.s_comment"(#7.6)
                                    │   │   ├── "supplier.s_name"(#7.1)
                                    │   │   ├── "supplier.s_nationkey"(#7.3)
                                    │   │   ├── "supplier.s_phone"(#7.4)
                                    │   │   └── "supplier.s_suppkey"(#7.0)
                                    │   ├── (.cardinality): 0.00
                                    │   ├── Get
                                    │   │   ├── .data_source_id: 5
                                    │   │   ├── .table_index: 6
                                    │   │   ├── .implementation: None
                                    │   │   ├── (.output_columns):
                                    │   │   │   ┌── "partsupp.ps_availqty"(#6.2)
                                    │   │   │   ├── "partsupp.ps_comment"(#6.4)
                                    │   │   │   ├── "partsupp.ps_partkey"(#6.0)
                                    │   │   │   ├── "partsupp.ps_suppkey"(#6.1)
                                    │   │   │   └── "partsupp.ps_supplycost"(#6.3)
                                    │   │   └── (.cardinality): 0.00
                                    │   └── Get
                                    │       ├── .data_source_id: 4
                                    │       ├── .table_index: 7
                                    │       ├── .implementation: None
                                    │       ├── (.output_columns):
                                    │       │   ┌── "supplier.s_acctbal"(#7.5)
                                    │       │   ├── "supplier.s_address"(#7.2)
                                    │       │   ├── "supplier.s_comment"(#7.6)
                                    │       │   ├── "supplier.s_name"(#7.1)
                                    │       │   ├── "supplier.s_nationkey"(#7.3)
                                    │       │   ├── "supplier.s_phone"(#7.4)
                                    │       │   └── "supplier.s_suppkey"(#7.0)
                                    │       └── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 1
                                        ├── .table_index: 8
                                        ├── .implementation: None
                                        ├── (.output_columns): [ "nation.n_comment"(#8.3), "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ]
                                        └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy { ordering_exprs: "__#14.value"(#14.1) DESC, (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.value"(#14.1) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 14
    ├── .projections: [ "partsupp.ps_partkey"(#1.0), "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) ]
    ├── (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.value"(#14.1) ]
    ├── (.cardinality): 0.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: CAST ("__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) AS Decimal128(38, 15)) > "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
        ├── (.output_columns):
        │   ┌── "__#4.ps_partkey"(#4.0)
        │   ├── "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0)
        │   └── "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
        ├── (.cardinality): 0.00
        ├── Aggregate
        │   ├── .key_table_index: 4
        │   ├── .aggregate_table_index: 5
        │   ├── .implementation: None
        │   ├── .exprs: sum("partsupp.ps_supplycost"(#1.3) * CAST ("partsupp.ps_availqty"(#1.2) AS Decimal128(10, 0)))
        │   ├── .keys: "partsupp.ps_partkey"(#1.0)
        │   ├── (.output_columns): [ "__#4.ps_partkey"(#4.0), "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) ]
        │   ├── (.cardinality): 0.00
        │   └── Join
        │       ├── .join_type: Inner
        │       ├── .implementation: None
        │       ├── .join_cond: "supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#3.0)
        │       ├── (.output_columns):
        │       │   ┌── "nation.n_name"(#3.1)
        │       │   ├── "nation.n_nationkey"(#3.0)
        │       │   ├── "partsupp.ps_availqty"(#1.2)
        │       │   ├── "partsupp.ps_partkey"(#1.0)
        │       │   ├── "partsupp.ps_suppkey"(#1.1)
        │       │   ├── "partsupp.ps_supplycost"(#1.3)
        │       │   ├── "supplier.s_nationkey"(#2.3)
        │       │   └── "supplier.s_suppkey"(#2.0)
        │       ├── (.cardinality): 0.00
        │       ├── Join
        │       │   ├── .join_type: Inner
        │       │   ├── .implementation: None
        │       │   ├── .join_cond: "partsupp.ps_suppkey"(#1.1) = "supplier.s_suppkey"(#2.0)
        │       │   ├── (.output_columns):
        │       │   │   ┌── "partsupp.ps_availqty"(#1.2)
        │       │   │   ├── "partsupp.ps_partkey"(#1.0)
        │       │   │   ├── "partsupp.ps_suppkey"(#1.1)
        │       │   │   ├── "partsupp.ps_supplycost"(#1.3)
        │       │   │   ├── "supplier.s_nationkey"(#2.3)
        │       │   │   └── "supplier.s_suppkey"(#2.0)
        │       │   ├── (.cardinality): 0.00
        │       │   ├── Get
        │       │   │   ├── .data_source_id: 5
        │       │   │   ├── .table_index: 1
        │       │   │   ├── .implementation: None
        │       │   │   ├── (.output_columns): [ "partsupp.ps_availqty"(#1.2), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1), "partsupp.ps_supplycost"(#1.3) ]
        │       │   │   └── (.cardinality): 0.00
        │       │   └── Get { .data_source_id: 4, .table_index: 2, .implementation: None, (.output_columns): [ "supplier.s_nationkey"(#2.3), "supplier.s_suppkey"(#2.0) ], (.cardinality): 0.00 }
        │       └── Select { .predicate: "nation.n_name"(#3.1) = 'CHINA'::utf8_view, (.output_columns): [ "nation.n_name"(#3.1), "nation.n_nationkey"(#3.0) ], (.cardinality): 0.00 }
        │           └── Get { .data_source_id: 1, .table_index: 3, .implementation: None, (.output_columns): [ "nation.n_name"(#3.1), "nation.n_nationkey"(#3.0) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 12, (.output_columns): "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0), (.cardinality): 1.00 }
            └── Project
                ├── .table_index: 11
                ├── .projections: CAST (CAST ("__#10.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#10.0) AS Float64) * 0.0001::float64 AS Decimal128(38, 15))
                ├── (.output_columns): "__#11.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#11.0)
                ├── (.cardinality): 1.00
                └── Aggregate
                    ├── .key_table_index: 9
                    ├── .aggregate_table_index: 10
                    ├── .implementation: None
                    ├── .exprs: sum("partsupp.ps_supplycost"(#6.3) * CAST ("partsupp.ps_availqty"(#6.2) AS Decimal128(10, 0)))
                    ├── .keys: []
                    ├── (.output_columns): "__#10.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#10.0)
                    ├── (.cardinality): 1.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: "supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0)
                        ├── (.output_columns):
                        │   ┌── "nation.n_name"(#8.1)
                        │   ├── "nation.n_nationkey"(#8.0)
                        │   ├── "partsupp.ps_availqty"(#6.2)
                        │   ├── "partsupp.ps_suppkey"(#6.1)
                        │   ├── "partsupp.ps_supplycost"(#6.3)
                        │   ├── "supplier.s_nationkey"(#7.3)
                        │   └── "supplier.s_suppkey"(#7.0)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: "partsupp.ps_suppkey"(#6.1) = "supplier.s_suppkey"(#7.0)
                        │   ├── (.output_columns): [ "partsupp.ps_availqty"(#6.2), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3), "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ]
                        │   ├── (.cardinality): 0.00
                        │   ├── Get
                        │   │   ├── .data_source_id: 5
                        │   │   ├── .table_index: 6
                        │   │   ├── .implementation: None
                        │   │   ├── (.output_columns): [ "partsupp.ps_availqty"(#6.2), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ]
                        │   │   └── (.cardinality): 0.00
                        │   └── Get { .data_source_id: 4, .table_index: 7, .implementation: None, (.output_columns): [ "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ], (.cardinality): 0.00 }
                        └── Select { .predicate: "nation.n_name"(#8.1) = 'CHINA'::utf8_view, (.output_columns): [ "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): [ "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0) ], (.cardinality): 0.00 }

physical_plan after optd-cascades:
EnforcerSort { tuple_ordering: [(#14.1, Desc)], (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.value"(#14.1) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 14
    ├── .projections: [ "partsupp.ps_partkey"(#1.0), "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) ]
    ├── (.output_columns): [ "__#14.ps_partkey"(#14.0), "__#14.value"(#14.1) ]
    ├── (.cardinality): 0.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: CAST ("__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) AS Decimal128(38, 15)) > "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
        ├── (.output_columns):
        │   ┌── "__#4.ps_partkey"(#4.0)
        │   ├── "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0)
        │   └── "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0)
        ├── (.cardinality): 0.00
        ├── Aggregate
        │   ├── .key_table_index: 4
        │   ├── .aggregate_table_index: 5
        │   ├── .implementation: None
        │   ├── .exprs: sum("partsupp.ps_supplycost"(#1.3) * CAST ("partsupp.ps_availqty"(#1.2) AS Decimal128(10, 0)))
        │   ├── .keys: "partsupp.ps_partkey"(#1.0)
        │   ├── (.output_columns): [ "__#4.ps_partkey"(#4.0), "__#5.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#5.0) ]
        │   ├── (.cardinality): 0.00
        │   └── Join
        │       ├── .join_type: Inner
        │       ├── .implementation: None
        │       ├── .join_cond: "supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#3.0)
        │       ├── (.output_columns):
        │       │   ┌── "nation.n_name"(#3.1)
        │       │   ├── "nation.n_nationkey"(#3.0)
        │       │   ├── "partsupp.ps_availqty"(#1.2)
        │       │   ├── "partsupp.ps_partkey"(#1.0)
        │       │   ├── "partsupp.ps_suppkey"(#1.1)
        │       │   ├── "partsupp.ps_supplycost"(#1.3)
        │       │   ├── "supplier.s_nationkey"(#2.3)
        │       │   └── "supplier.s_suppkey"(#2.0)
        │       ├── (.cardinality): 0.00
        │       ├── Join
        │       │   ├── .join_type: Inner
        │       │   ├── .implementation: None
        │       │   ├── .join_cond: "partsupp.ps_suppkey"(#1.1) = "supplier.s_suppkey"(#2.0)
        │       │   ├── (.output_columns):
        │       │   │   ┌── "partsupp.ps_availqty"(#1.2)
        │       │   │   ├── "partsupp.ps_partkey"(#1.0)
        │       │   │   ├── "partsupp.ps_suppkey"(#1.1)
        │       │   │   ├── "partsupp.ps_supplycost"(#1.3)
        │       │   │   ├── "supplier.s_nationkey"(#2.3)
        │       │   │   └── "supplier.s_suppkey"(#2.0)
        │       │   ├── (.cardinality): 0.00
        │       │   ├── Get
        │       │   │   ├── .data_source_id: 5
        │       │   │   ├── .table_index: 1
        │       │   │   ├── .implementation: None
        │       │   │   ├── (.output_columns): [ "partsupp.ps_availqty"(#1.2), "partsupp.ps_partkey"(#1.0), "partsupp.ps_suppkey"(#1.1), "partsupp.ps_supplycost"(#1.3) ]
        │       │   │   └── (.cardinality): 0.00
        │       │   └── Get { .data_source_id: 4, .table_index: 2, .implementation: None, (.output_columns): [ "supplier.s_nationkey"(#2.3), "supplier.s_suppkey"(#2.0) ], (.cardinality): 0.00 }
        │       └── Select { .predicate: "nation.n_name"(#3.1) = 'CHINA'::utf8_view, (.output_columns): [ "nation.n_name"(#3.1), "nation.n_nationkey"(#3.0) ], (.cardinality): 0.00 }
        │           └── Get { .data_source_id: 1, .table_index: 3, .implementation: None, (.output_columns): [ "nation.n_name"(#3.1), "nation.n_nationkey"(#3.0) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 12, (.output_columns): "__scalar_sq_1.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#12.0), (.cardinality): 1.00 }
            └── Project
                ├── .table_index: 11
                ├── .projections: CAST (CAST ("__#10.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#10.0) AS Float64) * 0.0001::float64 AS Decimal128(38, 15))
                ├── (.output_columns): "__#11.sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)"(#11.0)
                ├── (.cardinality): 1.00
                └── Aggregate
                    ├── .key_table_index: 9
                    ├── .aggregate_table_index: 10
                    ├── .implementation: None
                    ├── .exprs: sum("partsupp.ps_supplycost"(#6.3) * CAST ("partsupp.ps_availqty"(#6.2) AS Decimal128(10, 0)))
                    ├── .keys: []
                    ├── (.output_columns): "__#10.sum(partsupp.ps_supplycost * partsupp.ps_availqty)"(#10.0)
                    ├── (.cardinality): 1.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: "supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0)
                        ├── (.output_columns):
                        │   ┌── "nation.n_name"(#8.1)
                        │   ├── "nation.n_nationkey"(#8.0)
                        │   ├── "partsupp.ps_availqty"(#6.2)
                        │   ├── "partsupp.ps_suppkey"(#6.1)
                        │   ├── "partsupp.ps_supplycost"(#6.3)
                        │   ├── "supplier.s_nationkey"(#7.3)
                        │   └── "supplier.s_suppkey"(#7.0)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: "partsupp.ps_suppkey"(#6.1) = "supplier.s_suppkey"(#7.0)
                        │   ├── (.output_columns): [ "partsupp.ps_availqty"(#6.2), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3), "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ]
                        │   ├── (.cardinality): 0.00
                        │   ├── Get
                        │   │   ├── .data_source_id: 5
                        │   │   ├── .table_index: 6
                        │   │   ├── .implementation: None
                        │   │   ├── (.output_columns): [ "partsupp.ps_availqty"(#6.2), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ]
                        │   │   └── (.cardinality): 0.00
                        │   └── Get { .data_source_id: 4, .table_index: 7, .implementation: None, (.output_columns): [ "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ], (.cardinality): 0.00 }
                        └── Select { .predicate: "nation.n_name"(#8.1) = 'CHINA'::utf8_view, (.output_columns): [ "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): [ "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0) ], (.cardinality): 0.00 }
*/

