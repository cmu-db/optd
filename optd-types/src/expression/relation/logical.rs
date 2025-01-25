use crate::GroupId;

/// A type representing different kinds of logical expressions in the memo table.
pub enum LogicalExpr {
    Scan(LogicalScan),
    Filter(LogicalFilter),
    Join(LogicalJoin),
}

struct LogicalScan;

struct LogicalFilter;

struct LogicalJoin;
