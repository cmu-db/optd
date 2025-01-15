use crate::GroupId;

/// A type representing different kinds of physical expressions / operators.
pub enum PhysicalExpression {
    TableScan(TableScan),
    PhysicalFilter(PhysicalFilter),
    SortMergeJoin(SortMergeJoin),
    HashJoin(HashJoin),
    MergeSort(MergeSort),
}

struct TableScan {
    table_name: String,
}

struct PhysicalFilter {
    child: GroupId,
    predicate: GroupId,
}


struct SortMergeJoin {
    left: GroupId,
    right: GroupId,
    condition: GroupId,
    sort_expr: GroupId,
}

struct HashJoin {
    left: GroupId,
    right: GroupId,
    condition: GroupId,
}

struct MergeSort {
    child: GroupId,
    sort_expr: GroupId,
}

