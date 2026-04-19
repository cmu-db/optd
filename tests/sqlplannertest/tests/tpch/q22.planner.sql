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
OrderBy { ordering_exprs: "__#12.cntrycode"(#12.0) ASC, (.output_columns): [ "__#12.cntrycode"(#12.0), "__#12.numcust"(#12.1), "__#12.totacctbal"(#12.2) ], (.cardinality): 0.00 }
└── Project { .table_index: 12, .projections: [ "custsale.cntrycode"(#9.0), "__#11.count(Int64(1))"(#11.0), "__#11.sum(custsale.c_acctbal)"(#11.1) ], (.output_columns): [ "__#12.cntrycode"(#12.0), "__#12.numcust"(#12.1), "__#12.totacctbal"(#12.2) ], (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 10, .aggregate_table_index: 11, .implementation: None, .exprs: [ count(1::bigint), sum("custsale.c_acctbal"(#9.1)) ], .keys: "custsale.cntrycode"(#9.0), (.output_columns): [ "__#10.cntrycode"(#10.0), "__#11.count(Int64(1))"(#11.0), "__#11.sum(custsale.c_acctbal)"(#11.1) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 9, (.output_columns): [ "custsale.c_acctbal"(#9.1), "custsale.cntrycode"(#9.0) ], (.cardinality): 0.00 }
            └── Project { .table_index: 8, .projections: [ substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint), "customer.c_acctbal"(#1.5) ], (.output_columns): [ "__#8.c_acctbal"(#8.1), "__#8.cntrycode"(#8.0) ], (.cardinality): 0.00 }
                └── DependentJoin { .join_type: LeftAnti, .join_cond: true::boolean, (.output_columns): [ "__#5.avg(customer.c_acctbal)"(#5.0), "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ], (.cardinality): 0.00 }
                    ├── DependentJoin
                    │   ├── .join_type: Inner
                    │   ├── .join_cond: CAST ("customer.c_acctbal"(#1.5) AS Decimal128(19, 6)) > "__#5.avg(customer.c_acctbal)"(#5.0)
                    │   ├── (.output_columns): [ "__#5.avg(customer.c_acctbal)"(#5.0), "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ]
                    │   ├── (.cardinality): 0.00
                    │   ├── Select
                    │   │   ├── .predicate: substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint) IN [CAST ('13'::utf8 AS Utf8View), CAST ('31'::utf8 AS Utf8View), CAST ('23'::utf8 AS Utf8View), CAST ('29'::utf8 AS Utf8View), CAST ('30'::utf8 AS Utf8View), CAST ('18'::utf8 AS Utf8View), CAST ('17'::utf8 AS Utf8View)]
                    │   │   ├── (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ]
                    │   │   ├── (.cardinality): 0.00
                    │   │   └── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_address"(#1.2), "customer.c_comment"(#1.7), "customer.c_custkey"(#1.0), "customer.c_mktsegment"(#1.6), "customer.c_name"(#1.1), "customer.c_nationkey"(#1.3), "customer.c_phone"(#1.4) ], (.cardinality): 0.00 }
                    │   └── Project { .table_index: 5, .projections: "__#4.avg(customer.c_acctbal)"(#4.0), (.output_columns): "__#5.avg(customer.c_acctbal)"(#5.0), (.cardinality): 1.00 }
                    │       └── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: avg("customer.c_acctbal"(#2.5)), .keys: [], (.output_columns): "__#4.avg(customer.c_acctbal)"(#4.0), (.cardinality): 1.00 }
                    │           └── Select
                    │               ├── .predicate: (CAST ("customer.c_acctbal"(#2.5) AS Decimal128(30, 15)) > CAST (0::float64 AS Decimal128(30, 15))) AND (substr("customer.c_phone"(#2.4), 1::bigint, 2::bigint) IN [CAST ('13'::utf8 AS Utf8View), CAST ('31'::utf8 AS Utf8View), CAST ('23'::utf8 AS Utf8View), CAST ('29'::utf8 AS Utf8View), CAST ('30'::utf8 AS Utf8View), CAST ('18'::utf8 AS Utf8View), CAST ('17'::utf8 AS Utf8View)])
                    │               ├── (.output_columns): [ "customer.c_acctbal"(#2.5), "customer.c_address"(#2.2), "customer.c_comment"(#2.7), "customer.c_custkey"(#2.0), "customer.c_mktsegment"(#2.6), "customer.c_name"(#2.1), "customer.c_nationkey"(#2.3), "customer.c_phone"(#2.4) ]
                    │               ├── (.cardinality): 0.00
                    │               └── Get { .data_source_id: 6, .table_index: 2, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#2.5), "customer.c_address"(#2.2), "customer.c_comment"(#2.7), "customer.c_custkey"(#2.0), "customer.c_mktsegment"(#2.6), "customer.c_name"(#2.1), "customer.c_nationkey"(#2.3), "customer.c_phone"(#2.4) ], (.cardinality): 0.00 }
                    └── Project
                        ├── .table_index: 7
                        ├── .projections: [ "orders.o_orderkey"(#6.0), "orders.o_custkey"(#6.1), "orders.o_orderstatus"(#6.2), "orders.o_totalprice"(#6.3), "orders.o_orderdate"(#6.4), "orders.o_orderpriority"(#6.5), "orders.o_clerk"(#6.6), "orders.o_shippriority"(#6.7), "orders.o_comment"(#6.8) ]
                        ├── (.output_columns): [ "__#7.o_clerk"(#7.6), "__#7.o_comment"(#7.8), "__#7.o_custkey"(#7.1), "__#7.o_orderdate"(#7.4), "__#7.o_orderkey"(#7.0), "__#7.o_orderpriority"(#7.5), "__#7.o_orderstatus"(#7.2), "__#7.o_shippriority"(#7.7), "__#7.o_totalprice"(#7.3) ]
                        ├── (.cardinality): 0.00
                        └── Select { .predicate: "orders.o_custkey"(#6.1) = "customer.c_custkey"(#1.0), (.output_columns): [ "orders.o_clerk"(#6.6), "orders.o_comment"(#6.8), "orders.o_custkey"(#6.1), "orders.o_orderdate"(#6.4), "orders.o_orderkey"(#6.0), "orders.o_orderpriority"(#6.5), "orders.o_orderstatus"(#6.2), "orders.o_shippriority"(#6.7), "orders.o_totalprice"(#6.3) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 7, .table_index: 6, .implementation: None, (.output_columns): [ "orders.o_clerk"(#6.6), "orders.o_comment"(#6.8), "orders.o_custkey"(#6.1), "orders.o_orderdate"(#6.4), "orders.o_orderkey"(#6.0), "orders.o_orderpriority"(#6.5), "orders.o_orderstatus"(#6.2), "orders.o_shippriority"(#6.7), "orders.o_totalprice"(#6.3) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#12.0, Asc)], (.output_columns): [ "__#12.cntrycode"(#12.0), "__#12.numcust"(#12.1), "__#12.totacctbal"(#12.2) ], (.cardinality): 0.00 }
└── Project { .table_index: 12, .projections: [ "custsale.cntrycode"(#9.0), "__#11.count(Int64(1))"(#11.0), "__#11.sum(custsale.c_acctbal)"(#11.1) ], (.output_columns): [ "__#12.cntrycode"(#12.0), "__#12.numcust"(#12.1), "__#12.totacctbal"(#12.2) ], (.cardinality): 0.00 }
    └── Aggregate
        ├── .key_table_index: 10
        ├── .aggregate_table_index: 11
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint), sum("custsale.c_acctbal"(#9.1)) ]
        ├── .keys: "custsale.cntrycode"(#9.0)
        ├── (.output_columns): [ "__#10.cntrycode"(#10.0), "__#11.count(Int64(1))"(#11.0), "__#11.sum(custsale.c_acctbal)"(#11.1) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 9, (.output_columns): [ "custsale.c_acctbal"(#9.1), "custsale.cntrycode"(#9.0) ], (.cardinality): 0.00 }
            └── Project { .table_index: 8, .projections: [ substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint), "customer.c_acctbal"(#1.5) ], (.output_columns): [ "__#8.c_acctbal"(#8.1), "__#8.cntrycode"(#8.0) ], (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: LeftAnti
                    ├── .implementation: None
                    ├── .join_cond: "customer.c_custkey"(#1.0) IS NOT DISTINCT FROM "__#15.o_custkey"(#15.1)
                    ├── (.output_columns): [ "__#5.avg(customer.c_acctbal)"(#5.0), "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ]
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: CAST ("customer.c_acctbal"(#1.5) AS Decimal128(19, 6)) > "__#5.avg(customer.c_acctbal)"(#5.0)
                    │   ├── (.output_columns): [ "__#5.avg(customer.c_acctbal)"(#5.0), "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ]
                    │   ├── (.cardinality): 0.00
                    │   ├── Select
                    │   │   ├── .predicate: substr("customer.c_phone"(#1.4), 1::bigint, 2::bigint) IN ['13'::utf8_view, '31'::utf8_view, '23'::utf8_view, '29'::utf8_view, '30'::utf8_view, '18'::utf8_view, '17'::utf8_view]
                    │   │   ├── (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ]
                    │   │   ├── (.cardinality): 0.00
                    │   │   └── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#1.5), "customer.c_custkey"(#1.0), "customer.c_phone"(#1.4) ], (.cardinality): 0.00 }
                    │   └── Project { .table_index: 5, .projections: "__#4.avg(customer.c_acctbal)"(#4.0), (.output_columns): "__#5.avg(customer.c_acctbal)"(#5.0), (.cardinality): 1.00 }
                    │       └── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: avg("customer.c_acctbal"(#2.5)), .keys: [], (.output_columns): "__#4.avg(customer.c_acctbal)"(#4.0), (.cardinality): 1.00 }
                    │           └── Select
                    │               ├── .predicate: ("customer.c_acctbal"(#2.5) > 0::decimal128(15, 2)) AND (substr("customer.c_phone"(#2.4), 1::bigint, 2::bigint) IN ['13'::utf8_view, '31'::utf8_view, '23'::utf8_view, '29'::utf8_view, '30'::utf8_view, '18'::utf8_view, '17'::utf8_view])
                    │               ├── (.output_columns): [ "customer.c_acctbal"(#2.5), "customer.c_phone"(#2.4) ]
                    │               ├── (.cardinality): 0.00
                    │               └── Get { .data_source_id: 6, .table_index: 2, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#2.5), "customer.c_phone"(#2.4) ], (.cardinality): 0.00 }
                    └── Project
                        ├── .table_index: 15
                        ├── .projections:
                        │   ┌── "orders.o_orderkey"(#6.0)
                        │   ├── "orders.o_custkey"(#6.1)
                        │   ├── "orders.o_orderstatus"(#6.2)
                        │   ├── "orders.o_totalprice"(#6.3)
                        │   ├── "orders.o_orderdate"(#6.4)
                        │   ├── "orders.o_orderpriority"(#6.5)
                        │   ├── "orders.o_clerk"(#6.6)
                        │   ├── "orders.o_shippriority"(#6.7)
                        │   └── "orders.o_comment"(#6.8)
                        ├── (.output_columns):
                        │   ┌── "__#15.o_clerk"(#15.6)
                        │   ├── "__#15.o_comment"(#15.8)
                        │   ├── "__#15.o_custkey"(#15.1)
                        │   ├── "__#15.o_orderdate"(#15.4)
                        │   ├── "__#15.o_orderkey"(#15.0)
                        │   ├── "__#15.o_orderpriority"(#15.5)
                        │   ├── "__#15.o_orderstatus"(#15.2)
                        │   ├── "__#15.o_shippriority"(#15.7)
                        │   └── "__#15.o_totalprice"(#15.3)
                        ├── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: "orders.o_custkey"(#6.1) = "orders.o_custkey"(#6.1)
                            ├── (.output_columns):
                            │   ┌── "orders.o_clerk"(#6.6)
                            │   ├── "orders.o_comment"(#6.8)
                            │   ├── "orders.o_custkey"(#6.1)
                            │   ├── "orders.o_orderdate"(#6.4)
                            │   ├── "orders.o_orderkey"(#6.0)
                            │   ├── "orders.o_orderpriority"(#6.5)
                            │   ├── "orders.o_orderstatus"(#6.2)
                            │   ├── "orders.o_shippriority"(#6.7)
                            │   └── "orders.o_totalprice"(#6.3)
                            ├── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 7
                                ├── .table_index: 6
                                ├── .implementation: None
                                ├── (.output_columns):
                                │   ┌── "orders.o_clerk"(#6.6)
                                │   ├── "orders.o_comment"(#6.8)
                                │   ├── "orders.o_custkey"(#6.1)
                                │   ├── "orders.o_orderdate"(#6.4)
                                │   ├── "orders.o_orderkey"(#6.0)
                                │   ├── "orders.o_orderpriority"(#6.5)
                                │   ├── "orders.o_orderstatus"(#6.2)
                                │   ├── "orders.o_shippriority"(#6.7)
                                │   └── "orders.o_totalprice"(#6.3)
                                └── (.cardinality): 0.00
*/

