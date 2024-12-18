-- TPC-H Q8 without top-most limit node
select
    o_year,
    sum(case
        when nation = 'IRAQ' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection
    ├── exprs:
    │   ┌── #0
    │   └── Div
    │       ├── #1
    │       └── #2
    └── LogicalAgg
        ├── exprs:
        │   ┌── Agg(Sum)
        │   │   └── Case
        │   │       └── 
        │   │           ┌── Eq
        │   │           │   ├── #2
        │   │           │   └── "IRAQ"
        │   │           ├── #1
        │   │           └── Cast { cast_to: Decimal128(38, 4), child: 0(i64) }
        │   └── Agg(Sum)
        │       └── [ #1 ]
        ├── groups: [ #0 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── Scalar(DatePart)
            │   │   └── [ "YEAR", #36 ]
            │   ├── Mul
            │   │   ├── #21
            │   │   └── Sub
            │   │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │   │       └── #22
            │   └── #54
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #9
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #16
                │   │   └── #32
                │   ├── Eq
                │   │   ├── #33
                │   │   └── #41
                │   ├── Eq
                │   │   ├── #44
                │   │   └── #49
                │   ├── Eq
                │   │   ├── #51
                │   │   └── #57
                │   ├── Eq
                │   │   ├── #58
                │   │   └── "AMERICA"
                │   ├── Eq
                │   │   ├── #12
                │   │   └── #53
                │   ├── Between { child: #36, lower: Cast { cast_to: Date32, child: "1995-01-01" }, upper: Cast { cast_to: Date32, child: "1996-12-31" } }
                │   └── Eq
                │       ├── #4
                │       └── "ECONOMY ANODIZED STEEL"
                └── LogicalJoin { join_type: Inner, cond: true }
                    ├── LogicalJoin { join_type: Inner, cond: true }
                    │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   │   │   ├── LogicalScan { table: part }
                    │   │   │   │   │   │   └── LogicalScan { table: supplier }
                    │   │   │   │   │   └── LogicalScan { table: lineitem }
                    │   │   │   │   └── LogicalScan { table: orders }
                    │   │   │   └── LogicalScan { table: customer }
                    │   │   └── LogicalScan { table: nation }
                    │   └── LogicalScan { table: nation }
                    └── LogicalScan { table: region }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalProjection
    ├── exprs:
    │   ┌── #0
    │   └── Div
    │       ├── #1
    │       └── #2
    └── PhysicalAgg
        ├── aggrs:
        │   ┌── Agg(Sum)
        │   │   └── Case
        │   │       └── 
        │   │           ┌── Eq
        │   │           │   ├── #2
        │   │           │   └── "IRAQ"
        │   │           ├── #1
        │   │           └── Cast { cast_to: Decimal128(38, 4), child: 0(i64) }
        │   └── Agg(Sum)
        │       └── [ #1 ]
        ├── groups: [ #0 ]
        └── PhysicalProjection
            ├── exprs:
            │   ┌── Scalar(DatePart)
            │   │   └── [ "YEAR", #36 ]
            │   ├── Mul
            │   │   ├── #21
            │   │   └── Sub
            │   │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │   │       └── #22
            │   └── #54
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #51 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #44 ], right_keys: [ #0 ] }
                │   │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #33 ], right_keys: [ #0 ] }
                │   │   │   ├── PhysicalProjection { exprs: [ #25, #26, #27, #28, #29, #30, #31, #32, #33, #34, #35, #36, #37, #38, #39, #40, #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24 ] }
                │   │   │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
                │   │   │   │       ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                │   │   │   │       │   ├── PhysicalProjection { exprs: [ #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24, #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
                │   │   │   │       │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                │   │   │   │       │   │       ├── PhysicalFilter { cond: Between { child: #4, lower: Cast { cast_to: Date32, child: "1995-01-01" }, upper: Cast { cast_to: Date32, child: "1996-12-31" } } }
                │   │   │   │       │   │       │   └── PhysicalScan { table: orders }
                │   │   │   │       │   │       └── PhysicalScan { table: lineitem }
                │   │   │   │       │   └── PhysicalFilter
                │   │   │   │       │       ├── cond:Eq
                │   │   │   │       │       │   ├── #4
                │   │   │   │       │       │   └── "ECONOMY ANODIZED STEEL"
                │   │   │   │       │       └── PhysicalScan { table: part }
                │   │   │   │       └── PhysicalScan { table: supplier }
                │   │   │   └── PhysicalScan { table: customer }
                │   │   └── PhysicalScan { table: nation }
                │   └── PhysicalScan { table: nation }
                └── PhysicalFilter
                    ├── cond:Eq
                    │   ├── #1
                    │   └── "AMERICA"
                    └── PhysicalScan { table: region }
*/

