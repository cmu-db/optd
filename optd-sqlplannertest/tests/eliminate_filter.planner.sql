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
PhysicalProjection { exprs: [ #0, #1 ] }
└── PhysicalScan { table: t1 }
0 0
1 1
2 2
*/

