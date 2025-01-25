/// A type representing different kinds of logical expressions in the memo table.
pub enum LogicalExpr {
    Scan(LogicalScanExpr),
    Filter(LogicalFilterExpr),
    Join(LogicalJoinExpr),
}

pub struct LogicalScanExpr;

pub struct LogicalFilterExpr;

pub struct LogicalJoinExpr;
