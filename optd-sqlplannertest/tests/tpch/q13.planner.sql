-- TPC-H Q13
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Desc }
│   │   └── #1
│   └── SortOrder { order: Desc }
│       └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalAgg
        ├── exprs:Agg(Count)
        │   └── [ 1(i64) ]
        ├── groups: [ #1 ]
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalProjection { exprs: [ #0, #1 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Count)
                    │   └── [ #8 ]
                    ├── groups: [ #0 ]
                    └── LogicalJoin
                        ├── join_type: LeftOuter
                        ├── cond:And
                        │   ├── Eq
                        │   │   ├── #0
                        │   │   └── #9
                        │   └── Like { expr: #16, pattern: "%special%requests%", negated: true, case_insensitive: false }
                        ├── LogicalScan { table: customer }
                        └── LogicalScan { table: orders }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Desc }
│   │   └── #1
│   └── SortOrder { order: Desc }
│       └── #0
└── PhysicalAgg
    ├── aggrs:Agg(Count)
    │   └── [ 1(i64) ]
    ├── groups: [ #1 ]
    └── PhysicalAgg
        ├── aggrs:Agg(Count)
        │   └── [ #8 ]
        ├── groups: [ #0 ]
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftOuter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #9
            │   └── Like { expr: #16, pattern: "%special%requests%", negated: true, case_insensitive: false }
            ├── PhysicalScan { table: customer }
            └── PhysicalScan { table: orders }
*/

