use crate::GroupId;

/// A type representing different kinds of physical expressions in the memo table.
pub enum PhysicalExpr {
    TableScan(TableScan),
    Filter(PhysicalFilter),
    HashJoin(HashJoin),
}

struct TableScan;

struct PhysicalFilter;

struct HashJoin;
