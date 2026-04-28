-- Setup tables with skewed sizes for join-ordering coverage.
create table big_fact(id int, k1 int, payload int);
create table small_dim1(k1 int, k2 int, label varchar);
create table small_dim2(k2 int, class varchar);
insert into big_fact values
  (1, 1, 10),
  (2, 1, 11),
  (3, 1, 12),
  (4, 1, 13),
  (5, 1, 14),
  (6, 1, 15),
  (7, 1, 16),
  (8, 1, 17),
  (9, 1, 18),
  (10, 1, 19),
  (11, 2, 20),
  (12, 2, 21),
  (13, 2, 22),
  (14, 2, 23),
  (15, 2, 24),
  (16, 2, 25),
  (17, 2, 26),
  (18, 2, 27),
  (19, 2, 28),
  (20, 2, 29);
insert into small_dim1 values
  (1, 10, 'left'),
  (2, 20, 'right');
insert into small_dim2 values
  (10, 'x'),
  (20, 'y');

/*
20
2
2
*/

-- The planner should join the two small dimensions before the large fact table without aliases.
select big_fact.id, small_dim1.label, small_dim2.class
from big_fact
join small_dim1 on big_fact.k1 = small_dim1.k1
join small_dim2 on small_dim1.k2 = small_dim2.k2
order by big_fact.id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#4.id"(#4.0) ASC
├── (.output_columns):
│   ┌── "__#4.class"(#4.2)
│   ├── "__#4.id"(#4.0)
│   └── "__#4.label"(#4.1)
├── (.cardinality): 12.80
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "big_fact.id"(#1.0)
    │   ├── "small_dim1.label"(#2.2)
    │   └── "small_dim2.class"(#3.1)
    ├── (.output_columns):
    │   ┌── "__#4.class"(#4.2)
    │   ├── "__#4.id"(#4.0)
    │   └── "__#4.label"(#4.1)
    ├── (.cardinality): 12.80
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: ("small_dim1.k2"(#2.1) = "small_dim2.k2"(#3.0))
        ├── (.output_columns):
        │   ┌── "big_fact.id"(#1.0)
        │   ├── "big_fact.k1"(#1.1)
        │   ├── "big_fact.payload"(#1.2)
        │   ├── "small_dim1.k1"(#2.0)
        │   ├── "small_dim1.k2"(#2.1)
        │   ├── "small_dim1.label"(#2.2)
        │   ├── "small_dim2.class"(#3.1)
        │   └── "small_dim2.k2"(#3.0)
        ├── (.cardinality): 12.80
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: ("big_fact.k1"(#1.1) = "small_dim1.k1"(#2.0))
        │   ├── (.output_columns):
        │   │   ┌── "big_fact.id"(#1.0)
        │   │   ├── "big_fact.k1"(#1.1)
        │   │   ├── "big_fact.payload"(#1.2)
        │   │   ├── "small_dim1.k1"(#2.0)
        │   │   ├── "small_dim1.k2"(#2.1)
        │   │   └── "small_dim1.label"(#2.2)
        │   ├── (.cardinality): 16.00
        │   ├── Get
        │   │   ├── .data_source_id: 1
        │   │   ├── .table_index: 1
        │   │   ├── .implementation: None
        │   │   ├── (.output_columns):
        │   │   │   ┌── "big_fact.id"(#1.0)
        │   │   │   ├── "big_fact.k1"(#1.1)
        │   │   │   └── "big_fact.payload"(#1.2)
        │   │   └── (.cardinality): 20.00
        │   └── Get
        │       ├── .data_source_id: 2
        │       ├── .table_index: 2
        │       ├── .implementation: None
        │       ├── (.output_columns):
        │       │   ┌── "small_dim1.k1"(#2.0)
        │       │   ├── "small_dim1.k2"(#2.1)
        │       │   └── "small_dim1.label"(#2.2)
        │       └── (.cardinality): 2.00
        └── Get
            ├── .data_source_id: 3
            ├── .table_index: 3
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "small_dim2.class"(#3.1)
            │   └── "small_dim2.k2"(#3.0)
            └── (.cardinality): 2.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#4.0, Asc)]
├── (.output_columns): [ "__#4.class"(#4.2), "__#4.id"(#4.0), "__#4.label"(#4.1) ]
├── (.cardinality): 12.80
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "big_fact.id"(#1.0)
    │   ├── "small_dim1.label"(#2.2)
    │   └── "small_dim2.class"(#3.1)
    ├── (.output_columns): [ "__#4.class"(#4.2), "__#4.id"(#4.0), "__#4.label"(#4.1) ]
    ├── (.cardinality): 12.80
    └── Join
        ├── .join_type: Inner
        ├── .implementation: Some(Hash { build_side: Inner, keys: [(#1.1, #2.0)] })
        ├── .join_cond: "big_fact.k1"(#1.1) = "small_dim1.k1"(#2.0)
        ├── (.output_columns):
        │   ┌── "big_fact.id"(#1.0)
        │   ├── "big_fact.k1"(#1.1)
        │   ├── "small_dim1.k1"(#2.0)
        │   ├── "small_dim1.k2"(#2.1)
        │   ├── "small_dim1.label"(#2.2)
        │   ├── "small_dim2.class"(#3.1)
        │   └── "small_dim2.k2"(#3.0)
        ├── (.cardinality): 12.80
        ├── Get
        │   ├── .data_source_id: 1
        │   ├── .table_index: 1
        │   ├── .implementation: None
        │   ├── (.output_columns): [ "big_fact.id"(#1.0), "big_fact.k1"(#1.1) ]
        │   └── (.cardinality): 20.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: Some(Hash { build_side: Outer, keys: [(#2.1, #3.0)] })
            ├── .join_cond: "small_dim1.k2"(#2.1) = "small_dim2.k2"(#3.0)
            ├── (.output_columns):
            │   ┌── "small_dim1.k1"(#2.0)
            │   ├── "small_dim1.k2"(#2.1)
            │   ├── "small_dim1.label"(#2.2)
            │   ├── "small_dim2.class"(#3.1)
            │   └── "small_dim2.k2"(#3.0)
            ├── (.cardinality): 1.60
            ├── Get
            │   ├── .data_source_id: 2
            │   ├── .table_index: 2
            │   ├── .implementation: None
            │   ├── (.output_columns):
            │   │   ┌── "small_dim1.k1"(#2.0)
            │   │   ├── "small_dim1.k2"(#2.1)
            │   │   └── "small_dim1.label"(#2.2)
            │   └── (.cardinality): 2.00
            └── Get
                ├── .data_source_id: 3
                ├── .table_index: 3
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "small_dim2.class"(#3.1)
                │   └── "small_dim2.k2"(#3.0)
                └── (.cardinality): 2.00

1 left x
2 left x
3 left x
4 left x
5 left x
6 left x
7 left x
8 left x
9 left x
10 left x
11 right y
12 right y
13 right y
14 right y
15 right y
16 right y
17 right y
18 right y
19 right y
20 right y
*/

