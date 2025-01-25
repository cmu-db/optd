/// A type representing different kinds of logical expressions in the memo table.
pub enum LogicalExpr {
    Scan(LogicalScan),
    Filter(LogicalFilter),
    Join(LogicalJoin),
}

pub struct LogicalScan;

pub struct LogicalFilter;

pub struct LogicalJoin;
