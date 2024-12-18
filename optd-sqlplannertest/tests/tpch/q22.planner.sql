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
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1, #2 ] }
    └── LogicalAgg
        ├── exprs:
        │   ┌── Agg(Count)
        │   │   └── [ 1(i64) ]
        │   └── Agg(Sum)
        │       └── [ #1 ]
        ├── groups: [ #0 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── Scalar(Substr)
            │   │   └── [ #4, 1(i64), 2(i64) ]
            │   └── #5
            └── LogicalFilter
                ├── cond:And
                │   ├── InList
                │   │   ├── expr:Scalar(Substr)
                │   │   │   └── [ #4, 1(i64), 2(i64) ]
                │   │   ├── list: [ "13", "31", "23", "29", "30", "18", "17" ]
                │   │   ├── negated: false

                │   ├── Gt
                │   │   ├── Cast { cast_to: Decimal128(19, 6), child: #5 }
                │   │   └── #8
                │   └── Not
                │       └── [ #9 ]
                └── RawDependentJoin { sq_type: Exists, cond: true, extern_cols: [ Extern(#0) ] }
                    ├── RawDependentJoin { sq_type: Scalar, cond: true, extern_cols: [] }
                    │   ├── LogicalScan { table: customer }
                    │   └── LogicalProjection { exprs: [ #0 ] }
                    │       └── LogicalAgg
                    │           ├── exprs:Agg(Avg)
                    │           │   └── [ #5 ]
                    │           ├── groups: []
                    │           └── LogicalFilter
                    │               ├── cond:And
                    │               │   ├── Gt
                    │               │   │   ├── Cast { cast_to: Decimal128(30, 15), child: #5 }
                    │               │   │   └── Cast { cast_to: Decimal128(30, 15), child: 0(float) }
                    │               │   └── InList
                    │               │       ├── expr:Scalar(Substr)
                    │               │       │   └── [ #4, 1(i64), 2(i64) ]
                    │               │       ├── list: [ "13", "31", "23", "29", "30", "18", "17" ]
                    │               │       ├── negated: false

                    │               └── LogicalScan { table: customer }
                    └── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
                        └── LogicalFilter
                            ├── cond:Eq
                            │   ├── #1
                            │   └── Extern(#0)
                            └── LogicalScan { table: orders }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalAgg
    ├── aggrs:
    │   ┌── Agg(Count)
    │   │   └── [ 1(i64) ]
    │   └── Agg(Sum)
    │       └── [ #1 ]
    ├── groups: [ #0 ]
    └── PhysicalProjection
        ├── exprs:
        │   ┌── Scalar(Substr)
        │   │   └── [ #4, 1(i64), 2(i64) ]
        │   └── #5
        └── PhysicalFilter
            ├── cond:And
            │   ├── InList
            │   │   ├── expr:Scalar(Substr)
            │   │   │   └── [ #4, 1(i64), 2(i64) ]
            │   │   ├── list: [ "13", "31", "23", "29", "30", "18", "17" ]
            │   │   ├── negated: false

            │   ├── Gt
            │   │   ├── Cast { cast_to: Decimal128(19, 6), child: #5 }
            │   │   └── #8
            │   └── Not
            │       └── [ #9 ]
            └── PhysicalNestedLoopJoin
                ├── join_type: LeftMark
                ├── cond:And
                │   └── Eq
                │       ├── #0
                │       └── #9
                ├── PhysicalProjection { exprs: [ #1, #2, #3, #4, #5, #6, #7, #8, #0 ] }
                │   └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                │       ├── PhysicalAgg
                │       │   ├── aggrs:Agg(Avg)
                │       │   │   └── [ #5 ]
                │       │   ├── groups: []
                │       │   └── PhysicalFilter
                │       │       ├── cond:And
                │       │       │   ├── Gt
                │       │       │   │   ├── Cast { cast_to: Decimal128(30, 15), child: #5 }
                │       │       │   │   └── Cast { cast_to: Decimal128(30, 15), child: 0(float) }
                │       │       │   └── InList
                │       │       │       ├── expr:Scalar(Substr)
                │       │       │       │   └── [ #4, 1(i64), 2(i64) ]
                │       │       │       ├── list: [ "13", "31", "23", "29", "30", "18", "17" ]
                │       │       │       ├── negated: false

                │       │       └── PhysicalScan { table: customer }
                │       └── PhysicalScan { table: customer }
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                    ├── PhysicalAgg { aggrs: [], groups: [ #0 ] }
                    │   └── PhysicalProjection { exprs: [ #1, #2, #3, #4, #5, #6, #7, #8, #0 ] }
                    │       └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                    │           ├── PhysicalAgg
                    │           │   ├── aggrs:Agg(Avg)
                    │           │   │   └── [ #5 ]
                    │           │   ├── groups: []
                    │           │   └── PhysicalFilter
                    │           │       ├── cond:And
                    │           │       │   ├── Gt
                    │           │       │   │   ├── Cast { cast_to: Decimal128(30, 15), child: #5 }
                    │           │       │   │   └── Cast { cast_to: Decimal128(30, 15), child: 0(float) }
                    │           │       │   └── InList
                    │           │       │       ├── expr:Scalar(Substr)
                    │           │       │       │   └── [ #4, 1(i64), 2(i64) ]
                    │           │       │       ├── list: [ "13", "31", "23", "29", "30", "18", "17" ]
                    │           │       │       ├── negated: false

                    │           │       └── PhysicalScan { table: customer }
                    │           └── PhysicalScan { table: customer }
                    └── PhysicalScan { table: orders }
*/

