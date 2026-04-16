-- TPC-H Q22
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2) in
                ('13', '31', '23', '29', '30', '18', '17')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring(c_phone from 1 for 2) in
                        ('13', '31', '23', '29', '30', '18', '17')
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: "__#15.cntrycode"(#15.0) ASC, (.output_columns): [ "__#15.cntrycode"(#15.0), "__#15.numcust"(#15.1), "__#15.totacctbal"(#15.2) ], (.cardinality): 0.00 }
└── Project { .table_index: 15, .projections: [ "custsale.cntrycode"(#12.0), "__#14.count(Int64(1))"(#14.0), "__#14.sum(custsale.c_acctbal)"(#14.1) ], (.output_columns): [ "__#15.cntrycode"(#15.0), "__#15.numcust"(#15.1), "__#15.totacctbal"(#15.2) ], (.cardinality): 0.00 }
    └── Aggregate
        ├── .key_table_index: 13
        ├── .aggregate_table_index: 14
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint), sum("custsale.c_acctbal"(#12.1)) ]
        ├── .keys: "custsale.cntrycode"(#12.0)
        ├── (.output_columns): [ "__#13.cntrycode"(#13.0), "__#14.count(Int64(1))"(#14.0), "__#14.sum(custsale.c_acctbal)"(#14.1) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 12, (.output_columns): [ "custsale.c_acctbal"(#12.1), "custsale.cntrycode"(#12.0) ], (.cardinality): 0.00 }
            └── Project { .table_index: 11, .projections: [ substr("__#10.c_phone"(#10.4), 1::bigint, 2::bigint), "__#10.c_acctbal"(#10.5) ], (.output_columns): [ "__#11.c_acctbal"(#11.1), "__#11.cntrycode"(#11.0) ], (.cardinality): 0.00 }
                └── Project
                    ├── .table_index: 10
                    ├── .projections: [ "customer.c_custkey"(#1.0), "customer.c_name"(#1.1), "customer.c_address"(#1.2), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4), "customer.c_acctbal"(#1.5), "customer.c_mktsegment"(#1.6), "customer.c_comment"(#1.7) ]
                    ├── (.output_columns): [ "__#10.c_acctbal"(#10.5), "__#10.c_address"(#10.2), "__#10.c_comment"(#10.7), "__#10.c_custkey"(#10.0), "__#10.c_mktsegment"(#10.6), "__#10.c_name"(#10.1), "__#10.c_nationkey"(#10.3), "__#10.c_phone"(#10.4) ]
                    ├── (.cardinality): 0.00
                    └── Select
                        ├── .predicate: (substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint) IN ['13'::utf8_view, '31'::utf8_view, '23'::utf8_view, '29'::utf8_view, '30'::utf8_view, '18'::utf8_view, '17'::utf8_view]) AND (CAST ("customer.c_acctbal"(#1.5) AS Decimal128(19, 6)) > "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0))
                        ├── (.output_columns): [ "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0), "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ]
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: 
                            ├── (.output_columns): [ "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0), "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ]
                            ├── (.cardinality): 0.00
                            ├── Join
                            │   ├── .join_type: LeftAnti
                            │   ├── .implementation: None
                            │   ├── .join_cond: ("customer.c_custkey"(#1.0) = "__correlated_sq_1.o_custkey"(#4.1))
                            │   ├── (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ]
                            │   ├── (.cardinality): 0.00
                            │   ├── Get
                            │   │   ├── .data_source_id: 6
                            │   │   ├── .table_index: 1
                            │   │   ├── .implementation: None
                            │   │   ├── (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ]
                            │   │   └── (.cardinality): 0.00
                            │   └── Remap
                            │       ├── .table_index: 4
                            │       ├── (.output_columns):
                            │       │   ┌── "__correlated_sq_1.o_clerk"(#4.6)
                            │       │   ├── "__correlated_sq_1.o_comment"(#4.8)
                            │       │   ├── "__correlated_sq_1.o_custkey"(#4.1)
                            │       │   ├── "__correlated_sq_1.o_orderdate"(#4.4)
                            │       │   ├── "__correlated_sq_1.o_orderkey"(#4.0)
                            │       │   ├── "__correlated_sq_1.o_orderpriority"(#4.5)
                            │       │   ├── "__correlated_sq_1.o_orderstatus"(#4.2)
                            │       │   ├── "__correlated_sq_1.o_shippriority"(#4.7)
                            │       │   └── "__correlated_sq_1.o_totalprice"(#4.3)
                            │       ├── (.cardinality): 0.00
                            │       └── Project
                            │           ├── .table_index: 3
                            │           ├── .projections: [ "orders.o_orderkey"(#2.0), "orders.o_custkey"(#2.1), "orders.o_orderstatus"(#2.2), "orders.o_totalprice"(#2.3), "orders.o_orderdate"(#2.4), "orders.o_orderpriority"(#2.5), "orders.o_clerk"(#2.6), "orders.o_shippriority"(#2.7), "orders.o_comment"(#2.8) ]
                            │           ├── (.output_columns): [ "__#3.o_clerk"(#3.6), "__#3.o_comment"(#3.8), "__#3.o_custkey"(#3.1), "__#3.o_orderdate"(#3.4), "__#3.o_orderkey"(#3.0), "__#3.o_orderpriority"(#3.5), "__#3.o_orderstatus"(#3.2), "__#3.o_shippriority"(#3.7), "__#3.o_totalprice"(#3.3) ]
                            │           ├── (.cardinality): 0.00
                            │           └── Get
                            │               ├── .data_source_id: 7
                            │               ├── .table_index: 2
                            │               ├── .implementation: None
                            │               ├── (.output_columns): [ "orders.o_clerk"(#2.6), "orders.o_comment"(#2.8), "orders.o_custkey"(#2.1), "orders.o_orderdate"(#2.4), "orders.o_orderkey"(#2.0), "orders.o_orderpriority"(#2.5), "orders.o_orderstatus"(#2.2), "orders.o_shippriority"(#2.7), "orders.o_totalprice"(#2.3) ]
                            │               └── (.cardinality): 0.00
                            └── Remap { .table_index: 9, (.output_columns): "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0), (.cardinality): 1.00 }
                                └── Project { .table_index: 8, .projections: "__#7.avg(customer.c_acctbal)"(#7.0), (.output_columns): "__#8.avg(customer.c_acctbal)"(#8.0), (.cardinality): 1.00 }
                                    └── Aggregate { .key_table_index: 6, .aggregate_table_index: 7, .implementation: None, .exprs: avg("customer.c_acctbal"(#5.5)), .keys: [], (.output_columns): "__#7.avg(customer.c_acctbal)"(#7.0), (.cardinality): 1.00 }
                                        └── Select
                                            ├── .predicate: ("customer.c_acctbal"(#5.5) > 0::decimal128(15, 2)) AND (substr("customer.c_phone"(#5.4), 1::bigint, 2::bigint) IN ['13'::utf8_view, '31'::utf8_view, '23'::utf8_view, '29'::utf8_view, '30'::utf8_view, '18'::utf8_view, '17'::utf8_view])
                                            ├── (.output_columns): [ "customer.c_acctbal"(#5.5), "customer.c_address"(#5.2), "customer.c_comment"(#5.7), "customer.c_custkey"(#5.0), "customer.c_mktsegment"(#5.6), "customer.c_name"(#5.1), "customer.c_nationkey"(#5.3), "customer.c_phone"(#5.4) ]
                                            ├── (.cardinality): 0.00
                                            └── Get
                                                ├── .data_source_id: 6
                                                ├── .table_index: 5
                                                ├── .implementation: None
                                                ├── (.output_columns): [ "customer.c_acctbal"(#5.5), "customer.c_address"(#5.2), "customer.c_comment"(#5.7), "customer.c_custkey"(#5.0), "customer.c_mktsegment"(#5.6), "customer.c_name"(#5.1), "customer.c_nationkey"(#5.3), "customer.c_phone"(#5.4) ]
                                                └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#15.0, Asc)], (.output_columns): [ "__#15.cntrycode"(#15.0), "__#15.numcust"(#15.1), "__#15.totacctbal"(#15.2) ], (.cardinality): 0.00 }
└── Project { .table_index: 15, .projections: [ "custsale.cntrycode"(#12.0), "__#14.count(Int64(1))"(#14.0), "__#14.sum(custsale.c_acctbal)"(#14.1) ], (.output_columns): [ "__#15.cntrycode"(#15.0), "__#15.numcust"(#15.1), "__#15.totacctbal"(#15.2) ], (.cardinality): 0.00 }
    └── Aggregate
        ├── .key_table_index: 13
        ├── .aggregate_table_index: 14
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint), sum("custsale.c_acctbal"(#12.1)) ]
        ├── .keys: "custsale.cntrycode"(#12.0)
        ├── (.output_columns): [ "__#13.cntrycode"(#13.0), "__#14.count(Int64(1))"(#14.0), "__#14.sum(custsale.c_acctbal)"(#14.1) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 12, (.output_columns): [ "custsale.c_acctbal"(#12.1), "custsale.cntrycode"(#12.0) ], (.cardinality): 0.00 }
            └── Project { .table_index: 11, .projections: [ substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint), "customer.c_acctbal"(#1.5) ], (.output_columns): [ "__#11.c_acctbal"(#11.1), "__#11.cntrycode"(#11.0) ], (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: CAST ("customer.c_acctbal"(#1.5) AS Decimal128(19, 6)) > "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0)
                    ├── (.output_columns): [ "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0), "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ]
                    ├── (.cardinality): 0.00
                    ├── Select
                    │   ├── .predicate: substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint) IN ['13'::utf8_view, '31'::utf8_view, '23'::utf8_view, '29'::utf8_view, '30'::utf8_view, '18'::utf8_view, '17'::utf8_view]
                    │   ├── (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ]
                    │   ├── (.cardinality): 0.00
                    │   └── Join { .join_type: LeftAnti, .implementation: None, .join_cond: "customer.c_custkey"(#1.0) = "__correlated_sq_1.o_custkey"(#4.1), (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ], (.cardinality): 0.00 }
                    │       ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ], (.cardinality): 0.00 }
                    │       └── Remap
                    │           ├── .table_index: 4
                    │           ├── (.output_columns):
                    │           │   ┌── "__correlated_sq_1.o_clerk"(#4.6)
                    │           │   ├── "__correlated_sq_1.o_comment"(#4.8)
                    │           │   ├── "__correlated_sq_1.o_custkey"(#4.1)
                    │           │   ├── "__correlated_sq_1.o_orderdate"(#4.4)
                    │           │   ├── "__correlated_sq_1.o_orderkey"(#4.0)
                    │           │   ├── "__correlated_sq_1.o_orderpriority"(#4.5)
                    │           │   ├── "__correlated_sq_1.o_orderstatus"(#4.2)
                    │           │   ├── "__correlated_sq_1.o_shippriority"(#4.7)
                    │           │   └── "__correlated_sq_1.o_totalprice"(#4.3)
                    │           ├── (.cardinality): 0.00
                    │           └── Project
                    │               ├── .table_index: 3
                    │               ├── .projections:
                    │               │   ┌── "orders.o_orderkey"(#2.0)
                    │               │   ├── "orders.o_custkey"(#2.1)
                    │               │   ├── "orders.o_orderstatus"(#2.2)
                    │               │   ├── "orders.o_totalprice"(#2.3)
                    │               │   ├── "orders.o_orderdate"(#2.4)
                    │               │   ├── "orders.o_orderpriority"(#2.5)
                    │               │   ├── "orders.o_clerk"(#2.6)
                    │               │   ├── "orders.o_shippriority"(#2.7)
                    │               │   └── "orders.o_comment"(#2.8)
                    │               ├── (.output_columns):
                    │               │   ┌── "__#3.o_clerk"(#3.6)
                    │               │   ├── "__#3.o_comment"(#3.8)
                    │               │   ├── "__#3.o_custkey"(#3.1)
                    │               │   ├── "__#3.o_orderdate"(#3.4)
                    │               │   ├── "__#3.o_orderkey"(#3.0)
                    │               │   ├── "__#3.o_orderpriority"(#3.5)
                    │               │   ├── "__#3.o_orderstatus"(#3.2)
                    │               │   ├── "__#3.o_shippriority"(#3.7)
                    │               │   └── "__#3.o_totalprice"(#3.3)
                    │               ├── (.cardinality): 0.00
                    │               └── Get
                    │                   ├── .data_source_id: 7
                    │                   ├── .table_index: 2
                    │                   ├── .implementation: None
                    │                   ├── (.output_columns):
                    │                   │   ┌── "orders.o_clerk"(#2.6)
                    │                   │   ├── "orders.o_comment"(#2.8)
                    │                   │   ├── "orders.o_custkey"(#2.1)
                    │                   │   ├── "orders.o_orderdate"(#2.4)
                    │                   │   ├── "orders.o_orderkey"(#2.0)
                    │                   │   ├── "orders.o_orderpriority"(#2.5)
                    │                   │   ├── "orders.o_orderstatus"(#2.2)
                    │                   │   ├── "orders.o_shippriority"(#2.7)
                    │                   │   └── "orders.o_totalprice"(#2.3)
                    │                   └── (.cardinality): 0.00
                    └── Remap { .table_index: 9, (.output_columns): "__scalar_sq_2.avg(customer.c_acctbal)"(#9.0), (.cardinality): 1.00 }
                        └── Project { .table_index: 8, .projections: "__#7.avg(customer.c_acctbal)"(#7.0), (.output_columns): "__#8.avg(customer.c_acctbal)"(#8.0), (.cardinality): 1.00 }
                            └── Aggregate { .key_table_index: 6, .aggregate_table_index: 7, .implementation: None, .exprs: avg("customer.c_acctbal"(#5.5)), .keys: [], (.output_columns): "__#7.avg(customer.c_acctbal)"(#7.0), (.cardinality): 1.00 }
                                └── Select
                                    ├── .predicate: ("customer.c_acctbal"(#5.5) > 0::decimal128(15, 2)) AND (substr("customer.c_phone"(#5.4), 1::bigint, 2::bigint) IN ['13'::utf8_view, '31'::utf8_view, '23'::utf8_view, '29'::utf8_view, '30'::utf8_view, '18'::utf8_view, '17'::utf8_view])
                                    ├── (.output_columns): [ "customer.c_acctbal"(#5.5), "customer.c_phone"(#5.4) ]
                                    ├── (.cardinality): 0.00
                                    └── Get { .data_source_id: 6, .table_index: 5, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#5.5), "customer.c_phone"(#5.4) ], (.cardinality): 0.00 }
*/

