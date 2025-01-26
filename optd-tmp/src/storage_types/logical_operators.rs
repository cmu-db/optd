use super::{JoinType, RelGroupId, ScalarGroupId, ScalarGroupIdList};

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalScan {
    /// The name of the table to scan.
    /// TODO: Embed catalog information here.
    pub table_name: String,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalProject {
    /// The child group id.
    pub child: RelGroupId,
    /// The projection list (variable length)
    /// (e.g. SELECT 2 * (<colA> + 1), <colB> FROM <table>).
    pub fields: ScalarGroupIdList,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalJoin {
    /// The type of join to perform.
    pub join_type: JoinType,
    /// The group on the left side of the join.
    pub left: RelGroupId,
    /// The group on the right side of the join.
    pub right: RelGroupId,
    /// The join condition
    /// (e.g. <colA> = <colB>).
    pub join_cond: ScalarGroupId,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalFilter {
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3).
    pub predicate: ScalarGroupId,
}


#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct LogicalSort {
    pub child: RelGroupId,
    /// Sort expression
    /// (e.g. ORDER BY <colA> ASC, <colB> DESC).
    pub expr: ScalarGroupIdList,
}