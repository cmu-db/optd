-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- Test EliminateFilterRule (false filter to empty relation)
select * from t1 where false;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter { cond: false }
    └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #0, #1 ] }
└── PhysicalEmptyRelation { produce_one_row: false }
*/

-- Test EliminateFilterRule (replace true filter with child)
select * from t1 where true;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter { cond: true }
    └── LogicalScan { table: t1 }
PhysicalScan { table: t1 }
0 0
1 1
2 2
*/

-- Test SimplifyFilterRule and EliminateFilterRule (false filter to empty relation)
select * from t1, t2 where t1v1 = t2v1 and false;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalFilter
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── false
    └── LogicalJoin { join_type: Cross, cond: true }
        ├── LogicalScan { table: t1 }
        └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalEmptyRelation { produce_one_row: false }
*/

-- Test SimplifyFilterRule (skip true filter for and)
select * from t1, t2 where t1v1 = t2v1 and t1v1 = t2v3 and true;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalFilter
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #3
    │   └── true
    └── LogicalJoin { join_type: Cross, cond: true }
        ├── LogicalScan { table: t1 }
        └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #0 ], right_keys: [ #0, #1 ] }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
*/

-- Test SimplifyFilterRule (skip true filter for and)
select * from t1, t2 where t1v1 = t2v1 or t1v1 = t2v3 and true;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalFilter
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── And
    │       ├── Eq
    │       │   ├── #0
    │       │   └── #3
    │       └── true
    └── LogicalJoin { join_type: Cross, cond: true }
        ├── LogicalScan { table: t1 }
        └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalNestedLoopJoin
    ├── join_type: Inner
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── Eq
    │       ├── #0
    │       └── #3
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0 0 200
1 1 1 201
2 2 2 202
*/

-- Test SimplifyFilterRule, EliminateFilter (repace true filter for or)
select * from t1, t2 where t1v1 = t2v1 or t1v1 = t2v3 or true;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalFilter
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #3
    │   └── true
    └── LogicalJoin { join_type: Cross, cond: true }
        ├── LogicalScan { table: t1 }
        └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0 0 200
0 0 1 201
0 0 2 202
1 1 0 200
1 1 1 201
1 1 2 202
2 2 0 200
2 2 1 201
2 2 2 202
*/

-- Test SimplifyFilterRule (remove duplicates)
select * from t1, t2 where t1v1 = t2v1 or t1v1 = t2v1 and t1v1 = t2v1;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalFilter
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── And
    │       ├── Eq
    │       │   ├── #0
    │       │   └── #2
    │       └── Eq
    │           ├── #0
    │           └── #2
    └── LogicalJoin { join_type: Cross, cond: true }
        ├── LogicalScan { table: t1 }
        └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0 0 200
1 1 1 201
2 2 2 202
*/

-- Test SimplifyJoinCondRule and EliminateJoinRule (false filter to empty relation)
select * from t1 inner join t2 on t1v1 = t2v1 and false;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalJoin
    ├── join_type: Inner
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── false
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalEmptyRelation { produce_one_row: false }
*/

-- Test SimplifyJoinCondRule (skip true filter for and)
select * from t1 inner join t2 on t1v1 = t2v1 and t1v1 = t2v3 and true;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalJoin
    ├── join_type: Inner
    ├── cond:And
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #3
    │   └── true
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #0 ], right_keys: [ #0, #1 ] }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
*/

-- Test SimplifyJoinCondRule (skip true filter for and)
select * from t1 inner join t2 on t1v1 = t2v1 or t1v1 = t2v3 and true;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalJoin
    ├── join_type: Inner
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── And
    │       ├── Eq
    │       │   ├── #0
    │       │   └── #3
    │       └── true
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalNestedLoopJoin
    ├── join_type: Inner
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── Eq
    │       ├── #0
    │       └── #3
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0 0 200
1 1 1 201
2 2 2 202
*/

-- Test SimplifyJoinCondRule, EliminateFilter (repace true filter for or)
select * from t1 inner join t2 on t1v1 = t2v1 or t1v1 = t2v3 or true;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalJoin
    ├── join_type: Inner
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #3
    │   └── true
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0 0 200
0 0 1 201
0 0 2 202
1 1 0 200
1 1 1 201
1 1 2 202
2 2 0 200
2 2 1 201
2 2 2 202
*/

-- Test SimplifyJoinCondRule (remove duplicates)
select * from t1 inner join t2 on t1v1 = t2v1 or t1v1 = t2v1 and t1v1 = t2v1;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalJoin
    ├── join_type: Inner
    ├── cond:Or
    │   ├── Eq
    │   │   ├── #0
    │   │   └── #2
    │   └── And
    │       ├── Eq
    │       │   ├── #0
    │       │   └── #2
    │       └── Eq
    │           ├── #0
    │           └── #2
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0 0 200
1 1 1 201
2 2 2 202
*/

