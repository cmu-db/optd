/// A type representing different kinds of physical expressions in the memo table.
pub enum PhysicalExpr {
    TableScan(TableScan),
    Filter(PhysicalFilter),
    HashJoin(HashJoin),
}

pub struct TableScan;

pub struct PhysicalFilter;

pub struct HashJoin;
