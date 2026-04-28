-- Setup aggregate feature tables.
create table numbers(id int, grp int, val int, bonus int, note varchar);
create table dim(id int, category varchar, enabled int);
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

/*
5
5
*/

-- Global aggregates without grouping keys.
select count(id), sum(val), min(val), max(val)
from numbers;

/*
logical_plan after optd-initial:
Project
├── .table_index: 4
├── .projections:
│   ┌── "__#3.count(numbers.id)"(#3.0)
│   ├── "__#3.sum(numbers.val)"(#3.1)
│   ├── "__#3.min(numbers.val)"(#3.2)
│   └── "__#3.max(numbers.val)"(#3.3)
├── (.output_columns):
│   ┌── "__#4.count(numbers.id)"(#4.0)
│   ├── "__#4.max(numbers.val)"(#4.3)
│   ├── "__#4.min(numbers.val)"(#4.2)
│   └── "__#4.sum(numbers.val)"(#4.1)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 2
    ├── .aggregate_table_index: 3
    ├── .implementation: None
    ├── .exprs:
    │   ┌── count("numbers.id"(#1.0))
    │   ├── sum(CAST ("numbers.val"(#1.2) AS Int64))
    │   ├── min("numbers.val"(#1.2))
    │   └── max("numbers.val"(#1.2))
    ├── .keys: []
    ├── (.output_columns):
    │   ┌── "__#3.count(numbers.id)"(#3.0)
    │   ├── "__#3.max(numbers.val)"(#3.3)
    │   ├── "__#3.min(numbers.val)"(#3.2)
    │   └── "__#3.sum(numbers.val)"(#3.1)
    ├── (.cardinality): 1.00
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

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
Project
├── .table_index: 4
├── .projections:
│   ┌── "__#3.count(numbers.id)"(#3.0)
│   ├── "__#3.sum(numbers.val)"(#3.1)
│   ├── "__#3.min(numbers.val)"(#3.2)
│   └── "__#3.max(numbers.val)"(#3.3)
├── (.output_columns):
│   ┌── "__#4.count(numbers.id)"(#4.0)
│   ├── "__#4.max(numbers.val)"(#4.3)
│   ├── "__#4.min(numbers.val)"(#4.2)
│   └── "__#4.sum(numbers.val)"(#4.1)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 2
    ├── .aggregate_table_index: 3
    ├── .implementation: None
    ├── .exprs:
    │   ┌── count("numbers.id"(#1.0))
    │   ├── sum(CAST ("numbers.val"(#1.2) AS Int64))
    │   ├── min("numbers.val"(#1.2))
    │   └── max("numbers.val"(#1.2))
    ├── .keys: []
    ├── (.output_columns):
    │   ┌── "__#3.count(numbers.id)"(#3.0)
    │   ├── "__#3.max(numbers.val)"(#3.3)
    │   ├── "__#3.min(numbers.val)"(#3.2)
    │   └── "__#3.sum(numbers.val)"(#3.1)
    ├── (.cardinality): 1.00
    └── Get
        ├── .data_source_id: 1
        ├── .table_index: 1
        ├── .implementation: None
        ├── (.output_columns):
        │   ┌── "numbers.id"(#1.0)
        │   └── "numbers.val"(#1.2)
        └── (.cardinality): 5.00

physical_plan after optd-finalized:
Project
├── .table_index: 4
├── .projections:
│   ┌── "__#3.count(numbers.id)"(#3.0)
│   ├── "__#3.sum(numbers.val)"(#3.1)
│   ├── "__#3.min(numbers.val)"(#3.2)
│   └── "__#3.max(numbers.val)"(#3.3)
├── (.output_columns):
│   ┌── "__#4.count(numbers.id)"(#4.0)
│   ├── "__#4.max(numbers.val)"(#4.3)
│   ├── "__#4.min(numbers.val)"(#4.2)
│   └── "__#4.sum(numbers.val)"(#4.1)
├── (.cardinality): 1.00
└── Aggregate
    ├── .key_table_index: 2
    ├── .aggregate_table_index: 3
    ├── .implementation: None
    ├── .exprs:
    │   ┌── count("numbers.id"(#1.0))
    │   ├── sum(CAST ("numbers.val"(#1.2) AS Int64))
    │   ├── min("numbers.val"(#1.2))
    │   └── max("numbers.val"(#1.2))
    ├── .keys: []
    ├── (.output_columns):
    │   ┌── "__#3.count(numbers.id)"(#3.0)
    │   ├── "__#3.max(numbers.val)"(#3.3)
    │   ├── "__#3.min(numbers.val)"(#3.2)
    │   └── "__#3.sum(numbers.val)"(#3.1)
    ├── (.cardinality): 1.00
    └── Get
        ├── .data_source_id: 1
        ├── .table_index: 1
        ├── .implementation: None
        ├── (.output_columns):
        │   ┌── "numbers.id"(#1.0)
        │   └── "numbers.val"(#1.2)
        └── (.cardinality): 5.00

5 83 8 30
*/

-- Grouped aggregates with expression arguments.
select grp, count(id), sum(val + bonus)
from numbers
group by grp
order by grp;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#4.grp"(#4.0) ASC
├── (.output_columns):
│   ┌── "__#4.count(numbers.id)"(#4.1)
│   ├── "__#4.grp"(#4.0)
│   └── "__#4.sum(numbers.val + numbers.bonus)"(#4.2)
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   ├── "__#3.count(numbers.id)"(#3.0)
    │   └── "__#3.sum(numbers.val + numbers.bonus)"(#3.1)
    ├── (.output_columns):
    │   ┌── "__#4.count(numbers.id)"(#4.1)
    │   ├── "__#4.grp"(#4.0)
    │   └── "__#4.sum(numbers.val + numbers.bonus)"(#4.2)
    ├── (.cardinality): 1.00
    └── Aggregate
        ├── .key_table_index: 2
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs:
        │   ┌── count("numbers.id"(#1.0))
        │   └── sum(CAST ("numbers.val"(#1.2) + "numbers.bonus"(#1.3) AS Int64))
        ├── .keys: "numbers.grp"(#1.1)
        ├── (.output_columns):
        │   ┌── "__#2.grp"(#2.0)
        │   ├── "__#3.count(numbers.id)"(#3.0)
        │   └── "__#3.sum(numbers.val + numbers.bonus)"(#3.1)
        ├── (.cardinality): 1.00
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

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs: "__#4.grp"(#4.0) ASC
├── (.output_columns):
│   ┌── "__#4.count(numbers.id)"(#4.1)
│   ├── "__#4.grp"(#4.0)
│   └── "__#4.sum(numbers.val + numbers.bonus)"(#4.2)
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   ├── "__#3.count(numbers.id)"(#3.0)
    │   └── "__#3.sum(numbers.val + numbers.bonus)"(#3.1)
    ├── (.output_columns):
    │   ┌── "__#4.count(numbers.id)"(#4.1)
    │   ├── "__#4.grp"(#4.0)
    │   └── "__#4.sum(numbers.val + numbers.bonus)"(#4.2)
    ├── (.cardinality): 1.00
    └── Aggregate
        ├── .key_table_index: 2
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs:
        │   ┌── count("numbers.id"(#1.0))
        │   └── sum(CAST ("numbers.val"(#1.2) + "numbers.bonus"(#1.3) AS Int64))
        ├── .keys: "numbers.grp"(#1.1)
        ├── (.output_columns):
        │   ┌── "__#2.grp"(#2.0)
        │   ├── "__#3.count(numbers.id)"(#3.0)
        │   └── "__#3.sum(numbers.val + numbers.bonus)"(#3.1)
        ├── (.cardinality): 1.00
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.grp"(#1.1)
            │   ├── "numbers.id"(#1.0)
            │   └── "numbers.val"(#1.2)
            └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#4.0, Asc)]
├── (.output_columns):
│   ┌── "__#4.count(numbers.id)"(#4.1)
│   ├── "__#4.grp"(#4.0)
│   └── "__#4.sum(numbers.val + numbers.bonus)"(#4.2)
├── (.cardinality): 1.00
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   ├── "__#3.count(numbers.id)"(#3.0)
    │   └── "__#3.sum(numbers.val + numbers.bonus)"(#3.1)
    ├── (.output_columns):
    │   ┌── "__#4.count(numbers.id)"(#4.1)
    │   ├── "__#4.grp"(#4.0)
    │   └── "__#4.sum(numbers.val + numbers.bonus)"(#4.2)
    ├── (.cardinality): 1.00
    └── Aggregate
        ├── .key_table_index: 2
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs:
        │   ┌── count("numbers.id"(#1.0))
        │   └── sum(CAST ("numbers.val"(#1.2) + "numbers.bonus"(#1.3) AS Int64))
        ├── .keys: "numbers.grp"(#1.1)
        ├── (.output_columns):
        │   ┌── "__#2.grp"(#2.0)
        │   ├── "__#3.count(numbers.id)"(#3.0)
        │   └── "__#3.sum(numbers.val + numbers.bonus)"(#3.1)
        ├── (.cardinality): 1.00
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.grp"(#1.1)
            │   ├── "numbers.id"(#1.0)
            │   └── "numbers.val"(#1.2)
            └── (.cardinality): 5.00

1 2 35
2 2 60
3 1 8
*/

-- Aggregates with multiple grouping keys.
select grp, bonus, count(id)
from numbers
group by grp, bonus
order by grp, bonus;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs:
│   ┌── "__#4.grp"(#4.0) ASC
│   └── "__#4.bonus"(#4.1) ASC
├── (.output_columns):
│   ┌── "__#4.bonus"(#4.1)
│   ├── "__#4.count(numbers.id)"(#4.2)
│   └── "__#4.grp"(#4.0)
├── (.cardinality): 0.20
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   ├── "numbers.bonus"(#1.3)
    │   └── "__#3.count(numbers.id)"(#3.0)
    ├── (.output_columns):
    │   ┌── "__#4.bonus"(#4.1)
    │   ├── "__#4.count(numbers.id)"(#4.2)
    │   └── "__#4.grp"(#4.0)
    ├── (.cardinality): 0.20
    └── Aggregate
        ├── .key_table_index: 2
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs: count("numbers.id"(#1.0))
        ├── .keys:
        │   ┌── "numbers.grp"(#1.1)
        │   └── "numbers.bonus"(#1.3)
        ├── (.output_columns):
        │   ┌── "__#2.bonus"(#2.1)
        │   ├── "__#2.grp"(#2.0)
        │   └── "__#3.count(numbers.id)"(#3.0)
        ├── (.cardinality): 0.20
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

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs:
│   ┌── "__#4.grp"(#4.0) ASC
│   └── "__#4.bonus"(#4.1) ASC
├── (.output_columns):
│   ┌── "__#4.bonus"(#4.1)
│   ├── "__#4.count(numbers.id)"(#4.2)
│   └── "__#4.grp"(#4.0)
├── (.cardinality): 0.20
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   ├── "numbers.bonus"(#1.3)
    │   └── "__#3.count(numbers.id)"(#3.0)
    ├── (.output_columns):
    │   ┌── "__#4.bonus"(#4.1)
    │   ├── "__#4.count(numbers.id)"(#4.2)
    │   └── "__#4.grp"(#4.0)
    ├── (.cardinality): 0.20
    └── Aggregate
        ├── .key_table_index: 2
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs: count("numbers.id"(#1.0))
        ├── .keys:
        │   ┌── "numbers.grp"(#1.1)
        │   └── "numbers.bonus"(#1.3)
        ├── (.output_columns):
        │   ┌── "__#2.bonus"(#2.1)
        │   ├── "__#2.grp"(#2.0)
        │   └── "__#3.count(numbers.id)"(#3.0)
        ├── (.cardinality): 0.20
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.grp"(#1.1)
            │   └── "numbers.id"(#1.0)
            └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#4.0, Asc), (#4.1, Asc)]
├── (.output_columns):
│   ┌── "__#4.bonus"(#4.1)
│   ├── "__#4.count(numbers.id)"(#4.2)
│   └── "__#4.grp"(#4.0)
├── (.cardinality): 0.20
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   ├── "numbers.bonus"(#1.3)
    │   └── "__#3.count(numbers.id)"(#3.0)
    ├── (.output_columns):
    │   ┌── "__#4.bonus"(#4.1)
    │   ├── "__#4.count(numbers.id)"(#4.2)
    │   └── "__#4.grp"(#4.0)
    ├── (.cardinality): 0.20
    └── Aggregate
        ├── .key_table_index: 2
        ├── .aggregate_table_index: 3
        ├── .implementation: None
        ├── .exprs: count("numbers.id"(#1.0))
        ├── .keys:
        │   ┌── "numbers.grp"(#1.1)
        │   └── "numbers.bonus"(#1.3)
        ├── (.output_columns):
        │   ┌── "__#2.bonus"(#2.1)
        │   ├── "__#2.grp"(#2.0)
        │   └── "__#3.count(numbers.id)"(#3.0)
        ├── (.cardinality): 0.20
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.grp"(#1.1)
            │   └── "numbers.id"(#1.0)
            └── (.cardinality): 5.00

1 0 1
1 5 1
2 5 1
2 10 1
3 0 1
*/

-- Aggregates over join outputs.
select d.category, count(n.id), sum(n.val)
from numbers n
join dim d on n.id = d.id
group by d.category
order by d.category;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#7.category"(#7.0) ASC
├── (.output_columns):
│   ┌── "__#7.category"(#7.0)
│   ├── "__#7.count(n.id)"(#7.1)
│   └── "__#7.sum(n.val)"(#7.2)
├── (.cardinality): 2.00
└── Project
    ├── .table_index: 7
    ├── .projections:
    │   ┌── "d.category"(#4.1)
    │   ├── "__#6.count(n.id)"(#6.0)
    │   └── "__#6.sum(n.val)"(#6.1)
    ├── (.output_columns):
    │   ┌── "__#7.category"(#7.0)
    │   ├── "__#7.count(n.id)"(#7.1)
    │   └── "__#7.sum(n.val)"(#7.2)
    ├── (.cardinality): 2.00
    └── Aggregate
        ├── .key_table_index: 5
        ├── .aggregate_table_index: 6
        ├── .implementation: None
        ├── .exprs:
        │   ┌── count("n.id"(#2.0))
        │   └── sum(CAST ("n.val"(#2.2) AS Int64))
        ├── .keys: "d.category"(#4.1)
        ├── (.output_columns):
        │   ┌── "__#5.category"(#5.0)
        │   ├── "__#6.count(n.id)"(#6.0)
        │   └── "__#6.sum(n.val)"(#6.1)
        ├── (.cardinality): 2.00
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
├── ordering_exprs: "__#7.category"(#7.0) ASC
├── (.output_columns):
│   ┌── "__#7.category"(#7.0)
│   ├── "__#7.count(n.id)"(#7.1)
│   └── "__#7.sum(n.val)"(#7.2)
├── (.cardinality): 2.00
└── Project
    ├── .table_index: 7
    ├── .projections:
    │   ┌── "d.category"(#4.1)
    │   ├── "__#6.count(n.id)"(#6.0)
    │   └── "__#6.sum(n.val)"(#6.1)
    ├── (.output_columns):
    │   ┌── "__#7.category"(#7.0)
    │   ├── "__#7.count(n.id)"(#7.1)
    │   └── "__#7.sum(n.val)"(#7.2)
    ├── (.cardinality): 2.00
    └── Aggregate
        ├── .key_table_index: 5
        ├── .aggregate_table_index: 6
        ├── .implementation: None
        ├── .exprs:
        │   ┌── count("n.id"(#2.0))
        │   └── sum(CAST ("n.val"(#2.2) AS Int64))
        ├── .keys: "d.category"(#4.1)
        ├── (.output_columns):
        │   ┌── "__#5.category"(#5.0)
        │   ├── "__#6.count(n.id)"(#6.0)
        │   └── "__#6.sum(n.val)"(#6.1)
        ├── (.cardinality): 2.00
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

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#7.0, Asc)]
├── (.output_columns):
│   ┌── "__#7.category"(#7.0)
│   ├── "__#7.count(n.id)"(#7.1)
│   └── "__#7.sum(n.val)"(#7.2)
├── (.cardinality): 2.00
└── Project
    ├── .table_index: 7
    ├── .projections:
    │   ┌── "d.category"(#4.1)
    │   ├── "__#6.count(n.id)"(#6.0)
    │   └── "__#6.sum(n.val)"(#6.1)
    ├── (.output_columns):
    │   ┌── "__#7.category"(#7.0)
    │   ├── "__#7.count(n.id)"(#7.1)
    │   └── "__#7.sum(n.val)"(#7.2)
    ├── (.cardinality): 2.00
    └── Aggregate
        ├── .key_table_index: 5
        ├── .aggregate_table_index: 6
        ├── .implementation: None
        ├── .exprs: [ count("n.id"(#2.0)), sum(CAST ("n.val"(#2.2) AS Int64)) ]
        ├── .keys: "d.category"(#4.1)
        ├── (.output_columns):
        │   ┌── "__#5.category"(#5.0)
        │   ├── "__#6.count(n.id)"(#6.0)
        │   └── "__#6.sum(n.val)"(#6.1)
        ├── (.cardinality): 2.00
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

even 2 50
odd 2 25
*/

-- HAVING filters on aggregate outputs.
select grp, sum(val) as total
from numbers
group by grp
having sum(val) > 20
order by grp;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#4.grp"(#4.0) ASC
├── (.output_columns): [ "__#4.grp"(#4.0), "__#4.total"(#4.1) ]
├── (.cardinality): 0.10
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   └── "__#3.sum(numbers.val)"(#3.0)
    ├── (.output_columns):
    │   ┌── "__#4.grp"(#4.0)
    │   └── "__#4.total"(#4.1)
    ├── (.cardinality): 0.10
    └── Select
        ├── .predicate: "__#3.sum(numbers.val)"(#3.0) > 20::bigint
        ├── (.output_columns):
        │   ┌── "__#2.grp"(#2.0)
        │   └── "__#3.sum(numbers.val)"(#3.0)
        ├── (.cardinality): 0.10
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: sum(CAST ("numbers.val"(#1.2) AS Int64))
            ├── .keys: "numbers.grp"(#1.1)
            ├── (.output_columns):
            │   ┌── "__#2.grp"(#2.0)
            │   └── "__#3.sum(numbers.val)"(#3.0)
            ├── (.cardinality): 1.00
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

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
OrderBy
├── ordering_exprs: "__#4.grp"(#4.0) ASC
├── (.output_columns): [ "__#4.grp"(#4.0), "__#4.total"(#4.1) ]
├── (.cardinality): 0.10
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   └── "__#3.sum(numbers.val)"(#3.0)
    ├── (.output_columns):
    │   ┌── "__#4.grp"(#4.0)
    │   └── "__#4.total"(#4.1)
    ├── (.cardinality): 0.10
    └── Select
        ├── .predicate: "__#3.sum(numbers.val)"(#3.0) > 20::bigint
        ├── (.output_columns):
        │   ┌── "__#2.grp"(#2.0)
        │   └── "__#3.sum(numbers.val)"(#3.0)
        ├── (.cardinality): 0.10
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: sum(CAST ("numbers.val"(#1.2) AS Int64))
            ├── .keys: "numbers.grp"(#1.1)
            ├── (.output_columns):
            │   ┌── "__#2.grp"(#2.0)
            │   └── "__#3.sum(numbers.val)"(#3.0)
            ├── (.cardinality): 1.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.grp"(#1.1)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#4.0, Asc)]
├── (.output_columns): [ "__#4.grp"(#4.0), "__#4.total"(#4.1) ]
├── (.cardinality): 0.10
└── Project
    ├── .table_index: 4
    ├── .projections:
    │   ┌── "numbers.grp"(#1.1)
    │   └── "__#3.sum(numbers.val)"(#3.0)
    ├── (.output_columns):
    │   ┌── "__#4.grp"(#4.0)
    │   └── "__#4.total"(#4.1)
    ├── (.cardinality): 0.10
    └── Select
        ├── .predicate: "__#3.sum(numbers.val)"(#3.0) > 20::bigint
        ├── (.output_columns):
        │   ┌── "__#2.grp"(#2.0)
        │   └── "__#3.sum(numbers.val)"(#3.0)
        ├── (.cardinality): 0.10
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: sum(CAST ("numbers.val"(#1.2) AS Int64))
            ├── .keys: "numbers.grp"(#1.1)
            ├── (.output_columns):
            │   ┌── "__#2.grp"(#2.0)
            │   └── "__#3.sum(numbers.val)"(#3.0)
            ├── (.cardinality): 1.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.grp"(#1.1)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

1 30
2 45
*/

-- Ordered top-N aggregate results.
select grp, sum(val) as total
from numbers
group by grp
order by total desc
limit 2;

/*
logical_plan after optd-initial:
Limit
├── .skip: 0::bigint
├── .fetch: 2::bigint
├── (.output_columns): [ "__#4.grp"(#4.0), "__#4.total"(#4.1) ]
├── (.cardinality): 1.00
└── OrderBy
    ├── ordering_exprs: "__#4.total"(#4.1) DESC
    ├── (.output_columns):
    │   ┌── "__#4.grp"(#4.0)
    │   └── "__#4.total"(#4.1)
    ├── (.cardinality): 1.00
    └── Project
        ├── .table_index: 4
        ├── .projections:
        │   ┌── "numbers.grp"(#1.1)
        │   └── "__#3.sum(numbers.val)"(#3.0)
        ├── (.output_columns):
        │   ┌── "__#4.grp"(#4.0)
        │   └── "__#4.total"(#4.1)
        ├── (.cardinality): 1.00
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: sum(CAST ("numbers.val"(#1.2) AS Int64))
            ├── .keys: "numbers.grp"(#1.1)
            ├── (.output_columns):
            │   ┌── "__#2.grp"(#2.0)
            │   └── "__#3.sum(numbers.val)"(#3.0)
            ├── (.cardinality): 1.00
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

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
Limit
├── .skip: 0::bigint
├── .fetch: 2::bigint
├── (.output_columns): [ "__#4.grp"(#4.0), "__#4.total"(#4.1) ]
├── (.cardinality): 1.00
└── OrderBy
    ├── ordering_exprs: "__#4.total"(#4.1) DESC
    ├── (.output_columns):
    │   ┌── "__#4.grp"(#4.0)
    │   └── "__#4.total"(#4.1)
    ├── (.cardinality): 1.00
    └── Project
        ├── .table_index: 4
        ├── .projections:
        │   ┌── "numbers.grp"(#1.1)
        │   └── "__#3.sum(numbers.val)"(#3.0)
        ├── (.output_columns):
        │   ┌── "__#4.grp"(#4.0)
        │   └── "__#4.total"(#4.1)
        ├── (.cardinality): 1.00
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: sum(CAST ("numbers.val"(#1.2) AS Int64))
            ├── .keys: "numbers.grp"(#1.1)
            ├── (.output_columns):
            │   ┌── "__#2.grp"(#2.0)
            │   └── "__#3.sum(numbers.val)"(#3.0)
            ├── (.cardinality): 1.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.grp"(#1.1)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

physical_plan after optd-finalized:
Limit
├── .skip: 0::bigint
├── .fetch: 2::bigint
├── (.output_columns): [ "__#4.grp"(#4.0), "__#4.total"(#4.1) ]
├── (.cardinality): 1.00
└── EnforcerSort
    ├── tuple_ordering: [(#4.1, Desc)]
    ├── (.output_columns):
    │   ┌── "__#4.grp"(#4.0)
    │   └── "__#4.total"(#4.1)
    ├── (.cardinality): 1.00
    └── Project
        ├── .table_index: 4
        ├── .projections:
        │   ┌── "numbers.grp"(#1.1)
        │   └── "__#3.sum(numbers.val)"(#3.0)
        ├── (.output_columns):
        │   ┌── "__#4.grp"(#4.0)
        │   └── "__#4.total"(#4.1)
        ├── (.cardinality): 1.00
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: sum(CAST ("numbers.val"(#1.2) AS Int64))
            ├── .keys: "numbers.grp"(#1.1)
            ├── (.output_columns):
            │   ┌── "__#2.grp"(#2.0)
            │   └── "__#3.sum(numbers.val)"(#3.0)
            ├── (.cardinality): 1.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.grp"(#1.1)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

2 45
1 30
*/

