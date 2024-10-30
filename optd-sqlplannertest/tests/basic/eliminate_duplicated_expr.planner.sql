-- (no id or description)
create table t1(v1 int, v2 int);
insert into t1 values (0, 0), (1, 1), (5, 2), (2, 4), (0, 2);

/*
5
*/

-- Test without sorts/aggs.
select * from t1;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalScan { table: t1 }
PhysicalScan { table: t1 }
0 0
1 1
5 2
2 4
0 2
*/

-- Test whether the optimizer handles duplicate sort expressions correctly.
select * from t1 order by v1, v2, v1 desc, v2 desc, v1 asc;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   ├── SortOrder { order: Asc }
│   │   └── #1
│   ├── SortOrder { order: Desc }
│   │   └── #0
│   ├── SortOrder { order: Desc }
│   │   └── #1
│   └── SortOrder { order: Asc }
│       └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Asc }
│       └── #1
└── PhysicalScan { table: t1 }
0 0
0 2
1 1
2 4
5 2
*/

-- Test whether the optimizer handles duplicate agg expressions correctly.
select * from t1 group by v1, v2, v1;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalAgg { exprs: [], groups: [ #0, #1, #0 ] }
    └── LogicalScan { table: t1 }
PhysicalAgg { aggrs: [], groups: [ #0, #1 ] }
└── PhysicalScan { table: t1 }
0 0
1 1
5 2
2 4
0 2
*/

-- Test whether the optimizer handles duplicate sort and agg expressions correctly.
select * from t1 group by v1, v2, v1, v2, v2 order by v1, v2, v1 desc, v2 desc, v1 asc;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   ├── SortOrder { order: Asc }
│   │   └── #1
│   ├── SortOrder { order: Desc }
│   │   └── #0
│   ├── SortOrder { order: Desc }
│   │   └── #1
│   └── SortOrder { order: Asc }
│       └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalAgg { exprs: [], groups: [ #0, #1, #0, #1, #1 ] }
        └── LogicalScan { table: t1 }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Asc }
│       └── #1
└── PhysicalAgg { aggrs: [], groups: [ #0, #1 ] }
    └── PhysicalScan { table: t1 }
0 0
0 2
1 1
2 4
5 2
*/

