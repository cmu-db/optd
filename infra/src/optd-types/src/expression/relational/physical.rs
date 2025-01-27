/// A type representing different kinds of physical expressions in the memo table.
pub enum PhysicalExpr {
    TableScan(TableScanExpr),
    Filter(PhysicalFilterExpr),
    HashJoin(HashJoinExpr),
}

pub struct TableScanExpr;

pub struct PhysicalFilterExpr;

pub struct HashJoinExpr;
