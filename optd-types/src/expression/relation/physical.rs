use crate::GroupId;

/// A type representing different kinds of physical expressions in the memo table.
pub enum PhysicalExpr {
    TableScan(TableScan),
    Filter(PhysicalFilter),
    SortMergeJoin(SortMergeJoin),
    HashJoin(HashJoin),
    MergeSort(MergeSort),
}

struct TableScan;

struct PhysicalFilter;

struct SortMergeJoin;

struct HashJoin;

struct MergeSort;
