-- (no id or description)
create table t1(a int, b int);
create table t2(c int, d int);
create table t3(e int, f int);
create table t4(g int, h int);

/*

*/

-- test 3-way join
select * from t1, t2, t3 where a = c AND d = e;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5 ] }
└── LogicalFilter
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── Eq
    │       ├── #3
    │       └── #4
    └── LogicalJoin { join_type: Inner, cond: true }
        ├── LogicalJoin { join_type: Inner, cond: true }
        │   ├── LogicalScan { table: t1 }
        │   └── LogicalScan { table: t2 }
        └── LogicalScan { table: t3 }
PhysicalHashJoin { join_type: Inner, left_keys: [ #3 ], right_keys: [ #0 ] }
├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
│   ├── PhysicalScan { table: t1 }
│   └── PhysicalScan { table: t2 }
└── PhysicalScan { table: t3 }
*/

-- test 3-way join
select * from t1, t2, t3 where a = c AND b = e;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5 ] }
└── LogicalFilter
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── Eq
    │       ├── #1
    │       └── #4
    └── LogicalJoin { join_type: Inner, cond: true }
        ├── LogicalJoin { join_type: Inner, cond: true }
        │   ├── LogicalScan { table: t1 }
        │   └── LogicalScan { table: t2 }
        └── LogicalScan { table: t3 }
PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
│   ├── PhysicalScan { table: t1 }
│   └── PhysicalScan { table: t2 }
└── PhysicalScan { table: t3 }
*/

-- test 4-way join
select * from t1, t2, t3, t4 where a = c AND b = e AND f = g;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7 ] }
└── LogicalFilter
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   ├── Eq
    │   │   ├── #1
    │   │   └── #4
    │   └── Eq
    │       ├── #5
    │       └── #6
    └── LogicalJoin { join_type: Inner, cond: true }
        ├── LogicalJoin { join_type: Inner, cond: true }
        │   ├── LogicalJoin { join_type: Inner, cond: true }
        │   │   ├── LogicalScan { table: t1 }
        │   │   └── LogicalScan { table: t2 }
        │   └── LogicalScan { table: t3 }
        └── LogicalScan { table: t4 }
PhysicalHashJoin { join_type: Inner, left_keys: [ #5 ], right_keys: [ #0 ] }
├── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
│   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
│   │   ├── PhysicalScan { table: t1 }
│   │   └── PhysicalScan { table: t2 }
│   └── PhysicalScan { table: t3 }
└── PhysicalScan { table: t4 }
*/

