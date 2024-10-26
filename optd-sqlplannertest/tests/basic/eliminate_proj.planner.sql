-- (no id or description)
create table t1(v1 int, v2 int);
insert into t1 values (0, 0), (1, 1), (2, 2);

/*
3
*/

-- Test MergeProjectRule with only the rule enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(u64), fetch: 5(u64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalLimit { skip: 0(u64), fetch: 5(u64) }
    └── PhysicalProjection { exprs: [ #0, #1 ] }
        └── PhysicalScan { table: t1 }
*/

-- Test EliminateProjectRule with only the rule enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(u64), fetch: 5(u64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #1 ] }
└── PhysicalProjection { exprs: [ #1, #0 ] }
    └── PhysicalLimit { skip: 0(u64), fetch: 5(u64) }
        └── PhysicalScan { table: t1 }
*/

-- Test with all rules enabled
select v1 from (select v2, v1 from (select v1, v2 from t1 limit 5));

/*
LogicalProjection { exprs: [ #1 ] }
└── LogicalProjection { exprs: [ #1, #0 ] }
    └── LogicalLimit { skip: 0(u64), fetch: 5(u64) }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalScan { table: t1 }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalLimit { skip: 0(u64), fetch: 5(u64) }
    └── PhysicalScan { table: t1 }
0
1
2
*/

