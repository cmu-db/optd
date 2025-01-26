pub mod logical_operators;
pub mod physical_operators;
pub mod scalar_operators;

pub use logical_operators::*;
pub use physical_operators::*;

use chrono::NaiveDateTime;

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum Group {
    Relational(RelGroup),
    Scalar(ScalarGroup),
}

impl Group {
    pub fn explore_status(&self) -> ExploreStatus {
        match self {
            Group::Relational(group) => group.explore_status,
            Group::Scalar(group) => group.explore_status,
        }
    }
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum GroupId {
    Relational(RelGroupId),
    Scalar(ScalarGroupId),
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct RelGroupId(i64);

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct ScalarGroupId(i64);

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct RelGroup {
    pub id: RelGroupId,
    pub explore_status: ExploreStatus,
    pub created_at: NaiveDateTime,
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct ScalarGroup {
    pub id: ScalarGroupId,
    pub explore_status: ExploreStatus,
    pub created_at: NaiveDateTime,
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum ExploreStatus {
    Unexplored,
    Exploring,
    Explored,
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum ExprId {
    Logical(LogicalExprId),
    Physical(PhysicalExprId),
}
#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalExprId(i64);
#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalExprId(i64);

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum Expr {
    Logical(LogicalExpr),
    Physical(PhysicalExpr),
    Scalar(ScalarExpr),
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum LogicalExprKind {
    Project,
    Filter,
    Join,
    Scan,
    Sort,
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum ScalarExprKind {
    Constant,
    ColumnRef,
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum PhysicalOperatorKind {
    Filter,
    Project,
    TableScan,
    NLJoin,
    MergeJoin,
    Sort,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalExprRecord {
    group_id: RelGroupId,
    kind: LogicalExprKind,
    created_at: NaiveDateTime,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum LogicalExpr {
    Scan(LogicalScan),
    Filter(LogicalFilter),
    Join(LogicalJoin),
    Sort(LogicalSort),
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum PhysicalExpr {
    TableScan(PhysicalTableScan),
    IndexScan(PhysicalIndexScan),
    NLJoin(PhysicalNLJoin),
    MergeJoin(PhysicalMergeJoin),
    Project(PhysicalProject),
    Filter(PhysicalFilter),
    Sort(PhysicalSort),
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct ScalarGroupIdList(Vec<ScalarGroupId>);

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalExprRecord {
    group_id: RelGroupId,
    kind: PhysicalOperatorKind,
    total_cost: Cost,
    created_at: NaiveDateTime,
    physical_props: PhysicalProperties,
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalProperties(Vec<PhysicalProperty>);

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub enum PhysicalProperty {
    SortProp(SortProp),
    Partitioal(PartitionProp),
}

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct SortProp;

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PartitionProp;

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct ScalarExpr;

#[derive(PartialEq, Copy, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct Cost(u64);
