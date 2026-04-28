-- Setup join feature tables.
create table numbers(id int, grp int, val int, bonus int, note varchar);
create table dim(id int, category varchar, enabled int);
create table grp_offsets(grp int, shift int, label varchar);
create table tags(id int, tag varchar);
insert into numbers values
  (1, 1, 10, 0, 'Alpha'),
  (2, 1, 20, 5, 'alphabet'),
  (3, 2, 15, 5, 'Beta'),
  (4, 2, 30, 10, 'gamma'),
  (5, 3, 8, 0, 'delta');
insert into dim values
  (1, 'odd', 1),
  (2, 'even', 1),
  (3, 'odd', 0),
  (4, 'even', 1),
  (6, 'missing', 1);
insert into grp_offsets values
  (1, 100, 'g1'),
  (2, 200, 'g2'),
  (4, 400, 'g4');
insert into tags values
  (1, 'x'),
  (2, 'y'),
  (4, 'z');

/*
5
5
3
3
*/

-- Inner joins with aliases and post-join filters.
select n.id, d.category
from numbers n
join dim d on n.id = d.id
where d.enabled = 1
order by n.id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#5.id"(#5.0) ASC
├── (.output_columns):
│   ┌── "__#5.category"(#5.1)
│   └── "__#5.id"(#5.0)
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 5
    ├── .projections:
    │   ┌── "n.id"(#2.0)
    │   └── "d.category"(#4.1)
    ├── (.output_columns):
    │   ┌── "__#5.category"(#5.1)
    │   └── "__#5.id"(#5.0)
    ├── (.cardinality): 1.00
    └── Select
        ├── .predicate: "d.enabled"(#4.2) = 1::integer
        ├── (.output_columns):
        │   ┌── "d.category"(#4.1)
        │   ├── "d.enabled"(#4.2)
        │   ├── "d.id"(#4.0)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   └── "n.val"(#2.2)
        ├── (.cardinality): 1.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("n.id"(#2.0) = "d.id"(#4.0))
            ├── (.output_columns):
            │   ┌── "d.category"(#4.1)
            │   ├── "d.enabled"(#4.2)
            │   ├── "d.id"(#4.0)
            │   ├── "n.bonus"(#2.3)
            │   ├── "n.grp"(#2.1)
            │   ├── "n.id"(#2.0)
            │   ├── "n.note"(#2.4)
            │   └── "n.val"(#2.2)
            ├── (.cardinality): 10.00
            ├── Remap
            │   ├── .table_index: 2
            │   ├── (.output_columns):
            │   │   ┌── "n.bonus"(#2.3)
            │   │   ├── "n.grp"(#2.1)
            │   │   ├── "n.id"(#2.0)
            │   │   ├── "n.note"(#2.4)
            │   │   └── "n.val"(#2.2)
            │   ├── (.cardinality): 5.00
            │   └── Get
            │       ├── .data_source_id: 1
            │       ├── .table_index: 1
            │       ├── .implementation: None
            │       ├── (.output_columns):
            │       │   ┌── "numbers.bonus"(#1.3)
            │       │   ├── "numbers.grp"(#1.1)
            │       │   ├── "numbers.id"(#1.0)
            │       │   ├── "numbers.note"(#1.4)
            │       │   └── "numbers.val"(#1.2)
            │       └── (.cardinality): 5.00
            └── Remap
                ├── .table_index: 4
                ├── (.output_columns):
                │   ┌── "d.category"(#4.1)
                │   ├── "d.enabled"(#4.2)
                │   └── "d.id"(#4.0)
                ├── (.cardinality): 5.00
                └── Get
                    ├── .data_source_id: 2
                    ├── .table_index: 3
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "dim.category"(#3.1)
                    │   ├── "dim.enabled"(#3.2)
                    │   └── "dim.id"(#3.0)
                    └── (.cardinality): 5.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs: "__#5.id"(#5.0) ASC
├── (.output_columns):
│   ┌── "__#5.category"(#5.1)
│   └── "__#5.id"(#5.0)
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "n.id"(#2.0), "d.category"(#4.1) ]
    ├── (.output_columns):
    │   ┌── "__#5.category"(#5.1)
    │   └── "__#5.id"(#5.0)
    ├── (.cardinality): 1.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: "n.id"(#2.0) = "d.id"(#4.0)
        ├── (.output_columns):
        │   ┌── "d.category"(#4.1)
        │   ├── "d.enabled"(#4.2)
        │   ├── "d.id"(#4.0)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   └── "n.val"(#2.2)
        ├── (.cardinality): 1.00
        ├── Remap
        │   ├── .table_index: 2
        │   ├── (.output_columns):
        │   │   ┌── "n.bonus"(#2.3)
        │   │   ├── "n.grp"(#2.1)
        │   │   ├── "n.id"(#2.0)
        │   │   ├── "n.note"(#2.4)
        │   │   └── "n.val"(#2.2)
        │   ├── (.cardinality): 5.00
        │   └── Get
        │       ├── .data_source_id: 1
        │       ├── .table_index: 1
        │       ├── .implementation: None
        │       ├── (.output_columns):
        │       │   ┌── "numbers.bonus"(#1.3)
        │       │   ├── "numbers.grp"(#1.1)
        │       │   ├── "numbers.id"(#1.0)
        │       │   ├── "numbers.note"(#1.4)
        │       │   └── "numbers.val"(#1.2)
        │       └── (.cardinality): 5.00
        └── Select
            ├── .predicate: "d.enabled"(#4.2) = 1::integer
            ├── (.output_columns):
            │   ┌── "d.category"(#4.1)
            │   ├── "d.enabled"(#4.2)
            │   └── "d.id"(#4.0)
            ├── (.cardinality): 0.50
            └── Remap
                ├── .table_index: 4
                ├── (.output_columns):
                │   ┌── "d.category"(#4.1)
                │   ├── "d.enabled"(#4.2)
                │   └── "d.id"(#4.0)
                ├── (.cardinality): 5.00
                └── Get
                    ├── .data_source_id: 2
                    ├── .table_index: 3
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "dim.category"(#3.1)
                    │   ├── "dim.enabled"(#3.2)
                    │   └── "dim.id"(#3.0)
                    └── (.cardinality): 5.00

physical_plan after optd-cascades:
EnforcerSort
├── tuple_ordering: [(#5.0, Asc)]
├── (.output_columns): [ "__#5.category"(#5.1), "__#5.id"(#5.0) ]
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "n.id"(#2.0), "d.category"(#4.1) ]
    ├── (.output_columns): [ "__#5.category"(#5.1), "__#5.id"(#5.0) ]
    ├── (.cardinality): 1.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: Some(Hash { build_side: Outer, keys: [(#4.0, #2.0)] })
        ├── .join_cond: "n.id"(#2.0) = "d.id"(#4.0)
        ├── (.output_columns):
        │   ┌── "d.category"(#4.1)
        │   ├── "d.enabled"(#4.2)
        │   ├── "d.id"(#4.0)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   └── "n.val"(#2.2)
        ├── (.cardinality): 1.00
        ├── Select
        │   ├── .predicate: "d.enabled"(#4.2) = 1::integer
        │   ├── (.output_columns):
        │   │   ┌── "d.category"(#4.1)
        │   │   ├── "d.enabled"(#4.2)
        │   │   └── "d.id"(#4.0)
        │   ├── (.cardinality): 0.50
        │   └── Remap
        │       ├── .table_index: 4
        │       ├── (.output_columns):
        │       │   ┌── "d.category"(#4.1)
        │       │   ├── "d.enabled"(#4.2)
        │       │   └── "d.id"(#4.0)
        │       ├── (.cardinality): 5.00
        │       └── Get
        │           ├── .data_source_id: 2
        │           ├── .table_index: 3
        │           ├── .implementation: None
        │           ├── (.output_columns):
        │           │   ┌── "dim.category"(#3.1)
        │           │   ├── "dim.enabled"(#3.2)
        │           │   └── "dim.id"(#3.0)
        │           └── (.cardinality): 5.00
        └── Remap
            ├── .table_index: 2
            ├── (.output_columns):
            │   ┌── "n.bonus"(#2.3)
            │   ├── "n.grp"(#2.1)
            │   ├── "n.id"(#2.0)
            │   ├── "n.note"(#2.4)
            │   └── "n.val"(#2.2)
            ├── (.cardinality): 5.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.bonus"(#1.3)
                │   ├── "numbers.grp"(#1.1)
                │   ├── "numbers.id"(#1.0)
                │   ├── "numbers.note"(#1.4)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

1 odd
2 even
4 even
*/

-- Left joins with equi and non-equi join predicates.
select n.id, g.label
from numbers n
left join grp_offsets g on n.grp = g.grp and n.val < g.shift
order by n.id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#5.id"(#5.0) ASC
├── (.output_columns): [ "__#5.id"(#5.0), "__#5.label"(#5.1) ]
├── (.cardinality): 6.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "n.id"(#2.0), "g.label"(#4.2) ]
    ├── (.output_columns): [ "__#5.id"(#5.0), "__#5.label"(#5.1) ]
    ├── (.cardinality): 6.00
    └── Join
        ├── .join_type: LeftOuter
        ├── .implementation: None
        ├── .join_cond: ("n.grp"(#2.1) = "g.grp"(#4.0)) AND ("n.val"(#2.2) < "g.shift"(#4.1))
        ├── (.output_columns):
        │   ┌── "g.grp"(#4.0)
        │   ├── "g.label"(#4.2)
        │   ├── "g.shift"(#4.1)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   └── "n.val"(#2.2)
        ├── (.cardinality): 6.00
        ├── Remap
        │   ├── .table_index: 2
        │   ├── (.output_columns):
        │   │   ┌── "n.bonus"(#2.3)
        │   │   ├── "n.grp"(#2.1)
        │   │   ├── "n.id"(#2.0)
        │   │   ├── "n.note"(#2.4)
        │   │   └── "n.val"(#2.2)
        │   ├── (.cardinality): 5.00
        │   └── Get
        │       ├── .data_source_id: 1
        │       ├── .table_index: 1
        │       ├── .implementation: None
        │       ├── (.output_columns):
        │       │   ┌── "numbers.bonus"(#1.3)
        │       │   ├── "numbers.grp"(#1.1)
        │       │   ├── "numbers.id"(#1.0)
        │       │   ├── "numbers.note"(#1.4)
        │       │   └── "numbers.val"(#1.2)
        │       └── (.cardinality): 5.00
        └── Remap
            ├── .table_index: 4
            ├── (.output_columns): [ "g.grp"(#4.0), "g.label"(#4.2), "g.shift"(#4.1) ]
            ├── (.cardinality): 3.00
            └── Get
                ├── .data_source_id: 3
                ├── .table_index: 3
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "grp_offsets.grp"(#3.0)
                │   ├── "grp_offsets.label"(#3.2)
                │   └── "grp_offsets.shift"(#3.1)
                └── (.cardinality): 3.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
SAME TEXT AS ABOVE

physical_plan after optd-cascades:
EnforcerSort
├── tuple_ordering: [(#5.0, Asc)]
├── (.output_columns): [ "__#5.id"(#5.0), "__#5.label"(#5.1) ]
├── (.cardinality): 6.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "n.id"(#2.0), "g.label"(#4.2) ]
    ├── (.output_columns): [ "__#5.id"(#5.0), "__#5.label"(#5.1) ]
    ├── (.cardinality): 6.00
    └── Join
        ├── .join_type: LeftOuter
        ├── .implementation: Some(Hash { build_side: Outer, keys: [(#2.1, #4.0)] })
        ├── .join_cond: ("n.grp"(#2.1) = "g.grp"(#4.0)) AND ("n.val"(#2.2) < "g.shift"(#4.1))
        ├── (.output_columns):
        │   ┌── "g.grp"(#4.0)
        │   ├── "g.label"(#4.2)
        │   ├── "g.shift"(#4.1)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   └── "n.val"(#2.2)
        ├── (.cardinality): 6.00
        ├── Remap
        │   ├── .table_index: 2
        │   ├── (.output_columns):
        │   │   ┌── "n.bonus"(#2.3)
        │   │   ├── "n.grp"(#2.1)
        │   │   ├── "n.id"(#2.0)
        │   │   ├── "n.note"(#2.4)
        │   │   └── "n.val"(#2.2)
        │   ├── (.cardinality): 5.00
        │   └── Get
        │       ├── .data_source_id: 1
        │       ├── .table_index: 1
        │       ├── .implementation: None
        │       ├── (.output_columns):
        │       │   ┌── "numbers.bonus"(#1.3)
        │       │   ├── "numbers.grp"(#1.1)
        │       │   ├── "numbers.id"(#1.0)
        │       │   ├── "numbers.note"(#1.4)
        │       │   └── "numbers.val"(#1.2)
        │       └── (.cardinality): 5.00
        └── Remap
            ├── .table_index: 4
            ├── (.output_columns): [ "g.grp"(#4.0), "g.label"(#4.2), "g.shift"(#4.1) ]
            ├── (.cardinality): 3.00
            └── Get
                ├── .data_source_id: 3
                ├── .table_index: 3
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "grp_offsets.grp"(#3.0)
                │   ├── "grp_offsets.label"(#3.2)
                │   └── "grp_offsets.shift"(#3.1)
                └── (.cardinality): 3.00

1 g1
2 g1
3 g2
4 g2
5 NULL
*/

-- Multi-join plans over three relations.
select n.id, d.category, t.tag
from numbers n
join dim d on n.id = d.id
join tags t on n.id = t.id
where n.val >= 10
order by n.id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#7.id"(#7.0) ASC
├── (.output_columns):
│   ┌── "__#7.category"(#7.1)
│   ├── "__#7.id"(#7.0)
│   └── "__#7.tag"(#7.2)
├── (.cardinality): 1.20
└── Project
    ├── .table_index: 7
    ├── .projections:
    │   ┌── "n.id"(#2.0)
    │   ├── "d.category"(#4.1)
    │   └── "t.tag"(#6.1)
    ├── (.output_columns):
    │   ┌── "__#7.category"(#7.1)
    │   ├── "__#7.id"(#7.0)
    │   └── "__#7.tag"(#7.2)
    ├── (.cardinality): 1.20
    └── Select
        ├── .predicate: "n.val"(#2.2) >= 10::integer
        ├── (.output_columns):
        │   ┌── "d.category"(#4.1)
        │   ├── "d.enabled"(#4.2)
        │   ├── "d.id"(#4.0)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   ├── "n.val"(#2.2)
        │   ├── "t.id"(#6.0)
        │   └── "t.tag"(#6.1)
        ├── (.cardinality): 1.20
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("n.id"(#2.0) = "t.id"(#6.0))
            ├── (.output_columns):
            │   ┌── "d.category"(#4.1)
            │   ├── "d.enabled"(#4.2)
            │   ├── "d.id"(#4.0)
            │   ├── "n.bonus"(#2.3)
            │   ├── "n.grp"(#2.1)
            │   ├── "n.id"(#2.0)
            │   ├── "n.note"(#2.4)
            │   ├── "n.val"(#2.2)
            │   ├── "t.id"(#6.0)
            │   └── "t.tag"(#6.1)
            ├── (.cardinality): 12.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: ("n.id"(#2.0) = "d.id"(#4.0))
            │   ├── (.output_columns):
            │   │   ┌── "d.category"(#4.1)
            │   │   ├── "d.enabled"(#4.2)
            │   │   ├── "d.id"(#4.0)
            │   │   ├── "n.bonus"(#2.3)
            │   │   ├── "n.grp"(#2.1)
            │   │   ├── "n.id"(#2.0)
            │   │   ├── "n.note"(#2.4)
            │   │   └── "n.val"(#2.2)
            │   ├── (.cardinality): 10.00
            │   ├── Remap
            │   │   ├── .table_index: 2
            │   │   ├── (.output_columns):
            │   │   │   ┌── "n.bonus"(#2.3)
            │   │   │   ├── "n.grp"(#2.1)
            │   │   │   ├── "n.id"(#2.0)
            │   │   │   ├── "n.note"(#2.4)
            │   │   │   └── "n.val"(#2.2)
            │   │   ├── (.cardinality): 5.00
            │   │   └── Get
            │   │       ├── .data_source_id: 1
            │   │       ├── .table_index: 1
            │   │       ├── .implementation: None
            │   │       ├── (.output_columns):
            │   │       │   ┌── "numbers.bonus"(#1.3)
            │   │       │   ├── "numbers.grp"(#1.1)
            │   │       │   ├── "numbers.id"(#1.0)
            │   │       │   ├── "numbers.note"(#1.4)
            │   │       │   └── "numbers.val"(#1.2)
            │   │       └── (.cardinality): 5.00
            │   └── Remap
            │       ├── .table_index: 4
            │       ├── (.output_columns):
            │       │   ┌── "d.category"(#4.1)
            │       │   ├── "d.enabled"(#4.2)
            │       │   └── "d.id"(#4.0)
            │       ├── (.cardinality): 5.00
            │       └── Get
            │           ├── .data_source_id: 2
            │           ├── .table_index: 3
            │           ├── .implementation: None
            │           ├── (.output_columns):
            │           │   ┌── "dim.category"(#3.1)
            │           │   ├── "dim.enabled"(#3.2)
            │           │   └── "dim.id"(#3.0)
            │           └── (.cardinality): 5.00
            └── Remap
                ├── .table_index: 6
                ├── (.output_columns):
                │   ┌── "t.id"(#6.0)
                │   └── "t.tag"(#6.1)
                ├── (.cardinality): 3.00
                └── Get
                    ├── .data_source_id: 4
                    ├── .table_index: 5
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "tags.id"(#5.0)
                    │   └── "tags.tag"(#5.1)
                    └── (.cardinality): 3.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs: "__#7.id"(#7.0) ASC
├── (.output_columns):
│   ┌── "__#7.category"(#7.1)
│   ├── "__#7.id"(#7.0)
│   └── "__#7.tag"(#7.2)
├── (.cardinality): 1.20
└── Project
    ├── .table_index: 7
    ├── .projections:
    │   ┌── "n.id"(#2.0)
    │   ├── "d.category"(#4.1)
    │   └── "t.tag"(#6.1)
    ├── (.output_columns):
    │   ┌── "__#7.category"(#7.1)
    │   ├── "__#7.id"(#7.0)
    │   └── "__#7.tag"(#7.2)
    ├── (.cardinality): 1.20
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: "n.id"(#2.0) = "t.id"(#6.0)
        ├── (.output_columns):
        │   ┌── "d.category"(#4.1)
        │   ├── "d.enabled"(#4.2)
        │   ├── "d.id"(#4.0)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   ├── "n.val"(#2.2)
        │   ├── "t.id"(#6.0)
        │   └── "t.tag"(#6.1)
        ├── (.cardinality): 1.20
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: "n.id"(#2.0) = "d.id"(#4.0)
        │   ├── (.output_columns):
        │   │   ┌── "d.category"(#4.1)
        │   │   ├── "d.enabled"(#4.2)
        │   │   ├── "d.id"(#4.0)
        │   │   ├── "n.bonus"(#2.3)
        │   │   ├── "n.grp"(#2.1)
        │   │   ├── "n.id"(#2.0)
        │   │   ├── "n.note"(#2.4)
        │   │   └── "n.val"(#2.2)
        │   ├── (.cardinality): 1.00
        │   ├── Select
        │   │   ├── .predicate: "n.val"(#2.2) >= 10::integer
        │   │   ├── (.output_columns):
        │   │   │   ┌── "n.bonus"(#2.3)
        │   │   │   ├── "n.grp"(#2.1)
        │   │   │   ├── "n.id"(#2.0)
        │   │   │   ├── "n.note"(#2.4)
        │   │   │   └── "n.val"(#2.2)
        │   │   ├── (.cardinality): 0.50
        │   │   └── Remap
        │   │       ├── .table_index: 2
        │   │       ├── (.output_columns):
        │   │       │   ┌── "n.bonus"(#2.3)
        │   │       │   ├── "n.grp"(#2.1)
        │   │       │   ├── "n.id"(#2.0)
        │   │       │   ├── "n.note"(#2.4)
        │   │       │   └── "n.val"(#2.2)
        │   │       ├── (.cardinality): 5.00
        │   │       └── Get
        │   │           ├── .data_source_id: 1
        │   │           ├── .table_index: 1
        │   │           ├── .implementation: None
        │   │           ├── (.output_columns):
        │   │           │   ┌── "numbers.bonus"(#1.3)
        │   │           │   ├── "numbers.grp"(#1.1)
        │   │           │   ├── "numbers.id"(#1.0)
        │   │           │   ├── "numbers.note"(#1.4)
        │   │           │   └── "numbers.val"(#1.2)
        │   │           └── (.cardinality): 5.00
        │   └── Remap
        │       ├── .table_index: 4
        │       ├── (.output_columns):
        │       │   ┌── "d.category"(#4.1)
        │       │   ├── "d.enabled"(#4.2)
        │       │   └── "d.id"(#4.0)
        │       ├── (.cardinality): 5.00
        │       └── Get
        │           ├── .data_source_id: 2
        │           ├── .table_index: 3
        │           ├── .implementation: None
        │           ├── (.output_columns):
        │           │   ┌── "dim.category"(#3.1)
        │           │   ├── "dim.enabled"(#3.2)
        │           │   └── "dim.id"(#3.0)
        │           └── (.cardinality): 5.00
        └── Remap
            ├── .table_index: 6
            ├── (.output_columns):
            │   ┌── "t.id"(#6.0)
            │   └── "t.tag"(#6.1)
            ├── (.cardinality): 3.00
            └── Get
                ├── .data_source_id: 4
                ├── .table_index: 5
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "tags.id"(#5.0)
                │   └── "tags.tag"(#5.1)
                └── (.cardinality): 3.00

physical_plan after optd-cascades:
EnforcerSort
├── tuple_ordering: [(#7.0, Asc)]
├── (.output_columns): [ "__#7.category"(#7.1), "__#7.id"(#7.0), "__#7.tag"(#7.2) ]
├── (.cardinality): 1.20
└── Project
    ├── .table_index: 7
    ├── .projections: [ "n.id"(#2.0), "d.category"(#4.1), "t.tag"(#6.1) ]
    ├── (.output_columns): [ "__#7.category"(#7.1), "__#7.id"(#7.0), "__#7.tag"(#7.2) ]
    ├── (.cardinality): 1.20
    └── Join
        ├── .join_type: Inner
        ├── .implementation: Some(Hash { build_side: Outer, keys: [(#2.0, #4.0)] })
        ├── .join_cond: "n.id"(#2.0) = "d.id"(#4.0)
        ├── (.output_columns):
        │   ┌── "d.category"(#4.1)
        │   ├── "d.enabled"(#4.2)
        │   ├── "d.id"(#4.0)
        │   ├── "n.bonus"(#2.3)
        │   ├── "n.grp"(#2.1)
        │   ├── "n.id"(#2.0)
        │   ├── "n.note"(#2.4)
        │   ├── "n.val"(#2.2)
        │   ├── "t.id"(#6.0)
        │   └── "t.tag"(#6.1)
        ├── (.cardinality): 1.20
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: Some(Hash { build_side: Outer, keys: [(#2.0, #6.0)] })
        │   ├── .join_cond: "n.id"(#2.0) = "t.id"(#6.0)
        │   ├── (.output_columns):
        │   │   ┌── "n.bonus"(#2.3)
        │   │   ├── "n.grp"(#2.1)
        │   │   ├── "n.id"(#2.0)
        │   │   ├── "n.note"(#2.4)
        │   │   ├── "n.val"(#2.2)
        │   │   ├── "t.id"(#6.0)
        │   │   └── "t.tag"(#6.1)
        │   ├── (.cardinality): 0.60
        │   ├── Select
        │   │   ├── .predicate: "n.val"(#2.2) >= 10::integer
        │   │   ├── (.output_columns):
        │   │   │   ┌── "n.bonus"(#2.3)
        │   │   │   ├── "n.grp"(#2.1)
        │   │   │   ├── "n.id"(#2.0)
        │   │   │   ├── "n.note"(#2.4)
        │   │   │   └── "n.val"(#2.2)
        │   │   ├── (.cardinality): 0.50
        │   │   └── Remap
        │   │       ├── .table_index: 2
        │   │       ├── (.output_columns):
        │   │       │   ┌── "n.bonus"(#2.3)
        │   │       │   ├── "n.grp"(#2.1)
        │   │       │   ├── "n.id"(#2.0)
        │   │       │   ├── "n.note"(#2.4)
        │   │       │   └── "n.val"(#2.2)
        │   │       ├── (.cardinality): 5.00
        │   │       └── Get
        │   │           ├── .data_source_id: 1
        │   │           ├── .table_index: 1
        │   │           ├── .implementation: None
        │   │           ├── (.output_columns):
        │   │           │   ┌── "numbers.bonus"(#1.3)
        │   │           │   ├── "numbers.grp"(#1.1)
        │   │           │   ├── "numbers.id"(#1.0)
        │   │           │   ├── "numbers.note"(#1.4)
        │   │           │   └── "numbers.val"(#1.2)
        │   │           └── (.cardinality): 5.00
        │   └── Remap
        │       ├── .table_index: 6
        │       ├── (.output_columns): [ "t.id"(#6.0), "t.tag"(#6.1) ]
        │       ├── (.cardinality): 3.00
        │       └── Get
        │           ├── .data_source_id: 4
        │           ├── .table_index: 5
        │           ├── .implementation: None
        │           ├── (.output_columns): [ "tags.id"(#5.0), "tags.tag"(#5.1) ]
        │           └── (.cardinality): 3.00
        └── Remap
            ├── .table_index: 4
            ├── (.output_columns):
            │   ┌── "d.category"(#4.1)
            │   ├── "d.enabled"(#4.2)
            │   └── "d.id"(#4.0)
            ├── (.cardinality): 5.00
            └── Get
                ├── .data_source_id: 2
                ├── .table_index: 3
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "dim.category"(#3.1)
                │   ├── "dim.enabled"(#3.2)
                │   └── "dim.id"(#3.0)
                └── (.cardinality): 5.00

1 odd x
2 even y
4 even z
*/

-- Self joins with mixed equi and inequality conditions.
select a.id as left_id, b.id as right_id
from numbers a
join numbers b on a.grp = b.grp and a.id < b.id
order by left_id, right_id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs:
│   ┌── "__#5.left_id"(#5.0) ASC
│   └── "__#5.right_id"(#5.1) ASC
├── (.output_columns):
│   ┌── "__#5.left_id"(#5.0)
│   └── "__#5.right_id"(#5.1)
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "a.id"(#2.0), "b.id"(#4.0) ]
    ├── (.output_columns):
    │   ┌── "__#5.left_id"(#5.0)
    │   └── "__#5.right_id"(#5.1)
    ├── (.cardinality): 1.00
    └── Select
        ├── .predicate: "b.id"(#4.0) > "a.id"(#2.0)
        ├── (.output_columns):
        │   ┌── "a.bonus"(#2.3)
        │   ├── "a.grp"(#2.1)
        │   ├── "a.id"(#2.0)
        │   ├── "a.note"(#2.4)
        │   ├── "a.val"(#2.2)
        │   ├── "b.bonus"(#4.3)
        │   ├── "b.grp"(#4.1)
        │   ├── "b.id"(#4.0)
        │   ├── "b.note"(#4.4)
        │   └── "b.val"(#4.2)
        ├── (.cardinality): 1.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("a.grp"(#2.1) = "b.grp"(#4.1))
            ├── (.output_columns):
            │   ┌── "a.bonus"(#2.3)
            │   ├── "a.grp"(#2.1)
            │   ├── "a.id"(#2.0)
            │   ├── "a.note"(#2.4)
            │   ├── "a.val"(#2.2)
            │   ├── "b.bonus"(#4.3)
            │   ├── "b.grp"(#4.1)
            │   ├── "b.id"(#4.0)
            │   ├── "b.note"(#4.4)
            │   └── "b.val"(#4.2)
            ├── (.cardinality): 10.00
            ├── Remap
            │   ├── .table_index: 2
            │   ├── (.output_columns):
            │   │   ┌── "a.bonus"(#2.3)
            │   │   ├── "a.grp"(#2.1)
            │   │   ├── "a.id"(#2.0)
            │   │   ├── "a.note"(#2.4)
            │   │   └── "a.val"(#2.2)
            │   ├── (.cardinality): 5.00
            │   └── Get
            │       ├── .data_source_id: 1
            │       ├── .table_index: 1
            │       ├── .implementation: None
            │       ├── (.output_columns):
            │       │   ┌── "numbers.bonus"(#1.3)
            │       │   ├── "numbers.grp"(#1.1)
            │       │   ├── "numbers.id"(#1.0)
            │       │   ├── "numbers.note"(#1.4)
            │       │   └── "numbers.val"(#1.2)
            │       └── (.cardinality): 5.00
            └── Remap
                ├── .table_index: 4
                ├── (.output_columns):
                │   ┌── "b.bonus"(#4.3)
                │   ├── "b.grp"(#4.1)
                │   ├── "b.id"(#4.0)
                │   ├── "b.note"(#4.4)
                │   └── "b.val"(#4.2)
                ├── (.cardinality): 5.00
                └── Get
                    ├── .data_source_id: 1
                    ├── .table_index: 3
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "numbers.bonus"(#3.3)
                    │   ├── "numbers.grp"(#3.1)
                    │   ├── "numbers.id"(#3.0)
                    │   ├── "numbers.note"(#3.4)
                    │   └── "numbers.val"(#3.2)
                    └── (.cardinality): 5.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs: [ "__#5.left_id"(#5.0) ASC, "__#5.right_id"(#5.1) ASC ]
├── (.output_columns): [ "__#5.left_id"(#5.0), "__#5.right_id"(#5.1) ]
├── (.cardinality): 10.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "a.id"(#2.0), "b.id"(#4.0) ]
    ├── (.output_columns): [ "__#5.left_id"(#5.0), "__#5.right_id"(#5.1) ]
    ├── (.cardinality): 10.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: ("a.grp"(#2.1) = "b.grp"(#4.1)) AND ("b.id"(#4.0) > "a.id"(#2.0))
        ├── (.output_columns):
        │   ┌── "a.bonus"(#2.3)
        │   ├── "a.grp"(#2.1)
        │   ├── "a.id"(#2.0)
        │   ├── "a.note"(#2.4)
        │   ├── "a.val"(#2.2)
        │   ├── "b.bonus"(#4.3)
        │   ├── "b.grp"(#4.1)
        │   ├── "b.id"(#4.0)
        │   ├── "b.note"(#4.4)
        │   └── "b.val"(#4.2)
        ├── (.cardinality): 10.00
        ├── Remap
        │   ├── .table_index: 2
        │   ├── (.output_columns):
        │   │   ┌── "a.bonus"(#2.3)
        │   │   ├── "a.grp"(#2.1)
        │   │   ├── "a.id"(#2.0)
        │   │   ├── "a.note"(#2.4)
        │   │   └── "a.val"(#2.2)
        │   ├── (.cardinality): 5.00
        │   └── Get
        │       ├── .data_source_id: 1
        │       ├── .table_index: 1
        │       ├── .implementation: None
        │       ├── (.output_columns):
        │       │   ┌── "numbers.bonus"(#1.3)
        │       │   ├── "numbers.grp"(#1.1)
        │       │   ├── "numbers.id"(#1.0)
        │       │   ├── "numbers.note"(#1.4)
        │       │   └── "numbers.val"(#1.2)
        │       └── (.cardinality): 5.00
        └── Remap
            ├── .table_index: 4
            ├── (.output_columns):
            │   ┌── "b.bonus"(#4.3)
            │   ├── "b.grp"(#4.1)
            │   ├── "b.id"(#4.0)
            │   ├── "b.note"(#4.4)
            │   └── "b.val"(#4.2)
            ├── (.cardinality): 5.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 3
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.bonus"(#3.3)
                │   ├── "numbers.grp"(#3.1)
                │   ├── "numbers.id"(#3.0)
                │   ├── "numbers.note"(#3.4)
                │   └── "numbers.val"(#3.2)
                └── (.cardinality): 5.00

physical_plan after optd-cascades:
EnforcerSort
├── tuple_ordering: [(#5.0, Asc), (#5.1, Asc)]
├── (.output_columns): [ "__#5.left_id"(#5.0), "__#5.right_id"(#5.1) ]
├── (.cardinality): 10.00
└── Project
    ├── .table_index: 5
    ├── .projections: [ "a.id"(#2.0), "b.id"(#4.0) ]
    ├── (.output_columns): [ "__#5.left_id"(#5.0), "__#5.right_id"(#5.1) ]
    ├── (.cardinality): 10.00
    └── Join
        ├── .join_type: Inner
        ├── .implementation: Some(Hash { build_side: Outer, keys: [(#2.1, #4.1)] })
        ├── .join_cond: ("a.grp"(#2.1) = "b.grp"(#4.1)) AND ("b.id"(#4.0) > "a.id"(#2.0))
        ├── (.output_columns):
        │   ┌── "a.bonus"(#2.3)
        │   ├── "a.grp"(#2.1)
        │   ├── "a.id"(#2.0)
        │   ├── "a.note"(#2.4)
        │   ├── "a.val"(#2.2)
        │   ├── "b.bonus"(#4.3)
        │   ├── "b.grp"(#4.1)
        │   ├── "b.id"(#4.0)
        │   ├── "b.note"(#4.4)
        │   └── "b.val"(#4.2)
        ├── (.cardinality): 10.00
        ├── Remap
        │   ├── .table_index: 2
        │   ├── (.output_columns):
        │   │   ┌── "a.bonus"(#2.3)
        │   │   ├── "a.grp"(#2.1)
        │   │   ├── "a.id"(#2.0)
        │   │   ├── "a.note"(#2.4)
        │   │   └── "a.val"(#2.2)
        │   ├── (.cardinality): 5.00
        │   └── Get
        │       ├── .data_source_id: 1
        │       ├── .table_index: 1
        │       ├── .implementation: None
        │       ├── (.output_columns):
        │       │   ┌── "numbers.bonus"(#1.3)
        │       │   ├── "numbers.grp"(#1.1)
        │       │   ├── "numbers.id"(#1.0)
        │       │   ├── "numbers.note"(#1.4)
        │       │   └── "numbers.val"(#1.2)
        │       └── (.cardinality): 5.00
        └── Remap
            ├── .table_index: 4
            ├── (.output_columns):
            │   ┌── "b.bonus"(#4.3)
            │   ├── "b.grp"(#4.1)
            │   ├── "b.id"(#4.0)
            │   ├── "b.note"(#4.4)
            │   └── "b.val"(#4.2)
            ├── (.cardinality): 5.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 3
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.bonus"(#3.3)
                │   ├── "numbers.grp"(#3.1)
                │   ├── "numbers.id"(#3.0)
                │   ├── "numbers.note"(#3.4)
                │   └── "numbers.val"(#3.2)
                └── (.cardinality): 5.00

1 2
3 4
*/

