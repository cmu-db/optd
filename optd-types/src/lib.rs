mod expression;
mod memo;

pub struct GroupId(usize);

pub struct LogicalOperator;

pub struct PhysicalOperator;

pub struct ScalarOperator;

/// A type representing a tree of logical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree.
///
/// TODO Make this an actual tree with the correct modeling of types.
pub enum PartialLogicalPlan {
    LogicalOperator(LogicalOperator),
    ScalarOperator(ScalarOperator),
    GroupId(GroupId),
}

/// A type representing a tree of logical nodes, physical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree, and that physical nodes cannot have children
/// that are logical nodes.
///
/// TODO Make this an actual tree with the correct modeling of types.
pub enum PartialPhysicalPlan {
    LogicalOperator(LogicalOperator),
    PhysicalOperator(PhysicalOperator),
    ScalarOperator(ScalarOperator),
    GroupId(GroupId),
}

/// An in-memory tree of logical operators. Used as the input / entrypoint of the optimizer.
///
/// TODO The variants are just placeholders. The actual type will look more similar to
/// https://docs.rs/substrait/latest/substrait/proto/rel/enum.RelType.html
pub enum LogicalPlan {
    Scan(String),
    Filter(Box<LogicalPlan>),
    Sort(Box<LogicalPlan>),
    Join(Box<LogicalPlan>),
}

/// An in-memory tree of physical operators. Used as the final materialized output of the optimizer.
///
/// TODO The variants are just placeholders. The actual type will look more similar to
/// https://docs.rs/substrait/latest/substrait/proto/rel/enum.RelType.html
pub enum PhysicalPlan {
    TableScan(String),
    IndexScan(String),
    PhysicalFilter(Box<PhysicalPlan>),
    NestedLoopJoin(Box<PhysicalPlan>),
    SortMergeJoin(Box<PhysicalPlan>),
    HashJoin(Box<PhysicalPlan>),
    MergeSort(Box<PhysicalPlan>),
}
