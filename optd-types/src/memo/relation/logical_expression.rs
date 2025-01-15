use crate::GroupId;

/// A type representing different kinds of logical expressions / operators.
pub enum LogicalExpression {
    Scan(Scan),
    Filter(Filter),
    Join(Join),
    Sort(Sort),
}

struct Scan {
    table_name: String,
}

struct Filter {
    child: GroupId,
    predicate: GroupId,
}

struct Join {
    left: GroupId,
    right: GroupId,
    condition: GroupId,
}

struct Sort {
    child: GroupId,
    sort_expr: GroupId,
}
