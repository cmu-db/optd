mod memo;
pub use memo::MemoNode;

/// A type representing a transformation or implementation rule for query operators.
///
/// TODO The variants are just placeholders.
pub enum Rule {
    Transformation,
    Implementation
}

pub struct GroupId(usize);

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
