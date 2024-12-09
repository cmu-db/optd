-- (no id or description)
create table t1(v1 int, v2 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
create table t2(v0 int, v1 int, v2 int, v3 int);
insert into t2 values (0, 0, 0, 0), (1, 1, 1, 1), (2, 2, 2, 2);

/*
3
3
*/

-- Test MergeProjectRule with only the rule enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(i64), fetch: 5(i64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalLimit { skip: 0(i64), fetch: 5(i64) }
    └── PhysicalProjection { exprs: [ #0, #1 ] }
        └── PhysicalScan { table: t1 }
*/

-- Test EliminateProjectRule with only the rule enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(i64), fetch: 5(i64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #1 ] }
└── PhysicalProjection { exprs: [ #1, #0 ] }
    └── PhysicalLimit { skip: 0(i64), fetch: 5(i64) }
        └── PhysicalScan { table: t1 }
*/

-- Test with all rules enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(i64), fetch: 5(i64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalLimit { skip: 0(i64), fetch: 5(i64) }
    └── PhysicalScan { table: t1 }
0
1
2
*/

-- Test with all rules enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(i64), fetch: 5(i64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalLimit { skip: 0(i64), fetch: 5(i64) }
    └── PhysicalScan { table: t1 }
0
1
2
*/

-- Test with all rules enabled
select v0, v2, v1, v3 from (select v0 as v0, v2 as v1, v1 as v2, v3 from t2);

/*
LogicalProjection { exprs: [ #0, #2, #1, #3 ] }
└── LogicalProjection { exprs: [ #0, #2, #1, #3 ] }
    └── LogicalScan { table: t2 }
PhysicalScan { table: t2 }
0 0 0 0
1 1 1 1
2 2 2 2
*/

