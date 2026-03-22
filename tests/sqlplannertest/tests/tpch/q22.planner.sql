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
OrderBy { ordering_exprs: [ cntrycode(#13.0) ASC ], (.output_columns): cntrycode(#13.0), numcust(#13.1), totacctbal(#13.2), (.cardinality): 0.20 }
└── Project { .table_index: 13, .projections: [ cntrycode(#11.0), count(Int64(1))(#12.0), sum(custsale.c_acctbal)(#12.1) ], (.output_columns): cntrycode(#13.0), numcust(#13.1), totacctbal(#13.2), (.cardinality): 0.20 }
    └── Aggregate
        ├── .aggregate_table_index: 12
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint), sum(c_acctbal(#11.1)) ]
        ├── .keys: [ cntrycode(#11.0) ]
        ├── (.output_columns): cntrycode(#11.0), count(Int64(1))(#12.0), sum(custsale.c_acctbal)(#12.1)
        ├── (.cardinality): 0.20
        └── Remap { .table_index: 11, (.output_columns): c_acctbal(#11.1), cntrycode(#11.0), (.cardinality): 0.00 }
            └── Project { .table_index: 10, .projections: [ substr(c_phone(#9.4), 1::bigint, 2::bigint), c_acctbal(#9.5) ], (.output_columns): c_acctbal(#10.1), cntrycode(#10.0), (.cardinality): 0.00 }
                └── Project
                    ├── .table_index: 9
                    ├── .projections: [ c_custkey(#1.0), c_name(#1.1), c_address(#1.2), c_nationkey(#1.3), c_phone(#1.4), c_acctbal(#1.5), c_mktsegment(#1.6), c_comment(#1.7) ]
                    ├── (.output_columns): c_acctbal(#9.5), c_address(#9.2), c_comment(#9.7), c_custkey(#9.0), c_mktsegment(#9.6), c_name(#9.1), c_nationkey(#9.3), c_phone(#9.4)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: (CAST (c_acctbal(#1.5) AS Decimal128(19, 6)) > avg(customer.c_acctbal)(#8.0))
                        ├── (.output_columns): avg(customer.c_acctbal)(#8.0), c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: LeftAnti
                        │   ├── .implementation: None
                        │   ├── .join_cond: (c_custkey(#1.0) = o_custkey(#4.1))
                        │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        │   ├── (.cardinality): 0.00
                        │   ├── Select
                        │   │   ├── .predicate: substr(c_phone(#1.4), 1::bigint, 2::bigint) IN [13::utf8_view, 31::utf8_view, 23::utf8_view, 29::utf8_view, 30::utf8_view, 18::utf8_view, 17::utf8_view]
                        │   │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        │   │   ├── (.cardinality): 0.00
                        │   │   └── Get
                        │   │       ├── .data_source_id: 6
                        │   │       ├── .table_index: 1
                        │   │       ├── .implementation: None
                        │   │       ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        │   │       └── (.cardinality): 0.00
                        │   └── Remap
                        │       ├── .table_index: 4
                        │       ├── (.output_columns): o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3)
                        │       ├── (.cardinality): 0.00
                        │       └── Project
                        │           ├── .table_index: 3
                        │           ├── .projections: [ o_orderkey(#2.0), o_custkey(#2.1), o_orderstatus(#2.2), o_totalprice(#2.3), o_orderdate(#2.4), o_orderpriority(#2.5), o_clerk(#2.6), o_shippriority(#2.7), o_comment(#2.8) ]
                        │           ├── (.output_columns): o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3)
                        │           ├── (.cardinality): 0.00
                        │           └── Get
                        │               ├── .data_source_id: 7
                        │               ├── .table_index: 2
                        │               ├── .implementation: None
                        │               ├── (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                        │               └── (.cardinality): 0.00
                        └── Remap { .table_index: 8, (.output_columns): avg(customer.c_acctbal)(#8.0), (.cardinality): 1.00 }
                            └── Project { .table_index: 7, .projections: avg(customer.c_acctbal)(#6.0), (.output_columns): avg(customer.c_acctbal)(#7.0), (.cardinality): 1.00 }
                                └── Aggregate { .aggregate_table_index: 6, .implementation: None, .exprs: avg(c_acctbal(#5.5)), .keys: [], (.output_columns): avg(customer.c_acctbal)(#6.0), (.cardinality): 1.00 }
                                    └── Select
                                        ├── .predicate: (c_acctbal(#5.5) > 0::decimal128(15, 2)) AND (substr(c_phone(#5.4), 1::bigint, 2::bigint) IN [13::utf8_view, 31::utf8_view, 23::utf8_view, 29::utf8_view, 30::utf8_view, 18::utf8_view, 17::utf8_view])
                                        ├── (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4)
                                        ├── (.cardinality): 0.00
                                        └── Get
                                            ├── .data_source_id: 6
                                            ├── .table_index: 5
                                            ├── .implementation: None
                                            ├── (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4)
                                            └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#13.0, Asc)], (.output_columns): cntrycode(#13.0), numcust(#13.1), totacctbal(#13.2), (.cardinality): 0.20 }
└── Project { .table_index: 13, .projections: [ cntrycode(#11.0), count(Int64(1))(#12.0), sum(custsale.c_acctbal)(#12.1) ], (.output_columns): cntrycode(#13.0), numcust(#13.1), totacctbal(#13.2), (.cardinality): 0.20 }
    └── Aggregate
        ├── .aggregate_table_index: 12
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint), sum(c_acctbal(#11.1)) ]
        ├── .keys: [ cntrycode(#11.0) ]
        ├── (.output_columns): cntrycode(#11.0), count(Int64(1))(#12.0), sum(custsale.c_acctbal)(#12.1)
        ├── (.cardinality): 0.20
        └── Remap { .table_index: 11, (.output_columns): c_acctbal(#11.1), cntrycode(#11.0), (.cardinality): 0.00 }
            └── Project { .table_index: 10, .projections: [ substr(c_phone(#9.4), 1::bigint, 2::bigint), c_acctbal(#9.5) ], (.output_columns): c_acctbal(#10.1), cntrycode(#10.0), (.cardinality): 0.00 }
                └── Project
                    ├── .table_index: 9
                    ├── .projections: [ c_custkey(#1.0), c_name(#1.1), c_address(#1.2), c_nationkey(#1.3), c_phone(#1.4), c_acctbal(#1.5), c_mktsegment(#1.6), c_comment(#1.7) ]
                    ├── (.output_columns): c_acctbal(#9.5), c_address(#9.2), c_comment(#9.7), c_custkey(#9.0), c_mktsegment(#9.6), c_name(#9.1), c_nationkey(#9.3), c_phone(#9.4)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: (CAST (c_acctbal(#1.5) AS Decimal128(19, 6)) > avg(customer.c_acctbal)(#8.0))
                        ├── (.output_columns): avg(customer.c_acctbal)(#8.0), c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: LeftAnti
                        │   ├── .implementation: None
                        │   ├── .join_cond: (c_custkey(#1.0) = o_custkey(#4.1))
                        │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        │   ├── (.cardinality): 0.00
                        │   ├── Select
                        │   │   ├── .predicate: substr(c_phone(#1.4), 1::bigint, 2::bigint) IN [13::utf8_view, 31::utf8_view, 23::utf8_view, 29::utf8_view, 30::utf8_view, 18::utf8_view, 17::utf8_view]
                        │   │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        │   │   ├── (.cardinality): 0.00
                        │   │   └── Get
                        │   │       ├── .data_source_id: 6
                        │   │       ├── .table_index: 1
                        │   │       ├── .implementation: None
                        │   │       ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4)
                        │   │       └── (.cardinality): 0.00
                        │   └── Remap
                        │       ├── .table_index: 4
                        │       ├── (.output_columns): o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3)
                        │       ├── (.cardinality): 0.00
                        │       └── Project
                        │           ├── .table_index: 3
                        │           ├── .projections: [ o_orderkey(#2.0), o_custkey(#2.1), o_orderstatus(#2.2), o_totalprice(#2.3), o_orderdate(#2.4), o_orderpriority(#2.5), o_clerk(#2.6), o_shippriority(#2.7), o_comment(#2.8) ]
                        │           ├── (.output_columns): o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3)
                        │           ├── (.cardinality): 0.00
                        │           └── Get
                        │               ├── .data_source_id: 7
                        │               ├── .table_index: 2
                        │               ├── .implementation: None
                        │               ├── (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                        │               └── (.cardinality): 0.00
                        └── Remap { .table_index: 8, (.output_columns): avg(customer.c_acctbal)(#8.0), (.cardinality): 1.00 }
                            └── Project { .table_index: 7, .projections: avg(customer.c_acctbal)(#6.0), (.output_columns): avg(customer.c_acctbal)(#7.0), (.cardinality): 1.00 }
                                └── Aggregate { .aggregate_table_index: 6, .implementation: None, .exprs: avg(c_acctbal(#5.5)), .keys: [], (.output_columns): avg(customer.c_acctbal)(#6.0), (.cardinality): 1.00 }
                                    └── Select
                                        ├── .predicate: (c_acctbal(#5.5) > 0::decimal128(15, 2)) AND (substr(c_phone(#5.4), 1::bigint, 2::bigint) IN [13::utf8_view, 31::utf8_view, 23::utf8_view, 29::utf8_view, 30::utf8_view, 18::utf8_view, 17::utf8_view])
                                        ├── (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4)
                                        ├── (.cardinality): 0.00
                                        └── Get
                                            ├── .data_source_id: 6
                                            ├── .table_index: 5
                                            ├── .implementation: None
                                            ├── (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4)
                                            └── (.cardinality): 0.00
*/

