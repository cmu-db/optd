
use super::{JoinType, RelGroupId, ScalarGroupId, ScalarGroupIdList};

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalTableScan {
    /// The name of the table to scan.
    /// TODO: Embed catalog information here.
    pub table_name: String,
}


#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalIndexScan {
    /// The name of the table to scan.
    /// TODO: Embed catalog information here.
    pub table_name: String,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalProject {
    /// The child group id.
    pub child: RelGroupId,
    /// The projection list (variable length)
    /// (e.g. SELECT 2 * (<colA> + 1), <colB> FROM <table>).
    pub fields: ScalarGroupIdList,
}

/// Nested loop join.
#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalNLJoin {
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

/// (e.g. t1.A = t2.A and t1.B = t2.B will turn into
/// left is sorted on [t1.A, t1.B], right is sorted on [t2.A, t2.B])
/// todo(alexis): understand this
#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalMergeJoin {
    /// The type of join to perform.
    pub join_type: JoinType,
    /// The group on the left side of the join.
    pub left: RelGroupId,
    /// The group on the right side of the join.
    pub right: RelGroupId,
    /// Join keys from the left child.
    pub left_join_keys: ScalarGroupIdList,
    /// Join keys from the right child.
    pub right_join_keys: ScalarGroupIdList,  
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalFilter {
    /// The child group id.
    pub child: RelGroupId,
    /// The filter predicate (e.g. <colA> > 3).
    pub predicate: ScalarGroupId,
}

#[derive(PartialEq, Clone, Eq, PartialOrd, Ord, Debug)]
pub struct PhysicalSort {
    pub child: RelGroupId,
    /// Sort expression
    /// (e.g. ORDER BY <colA> ASC, <colB> DESC).
    pub expr: ScalarGroupIdList,
}