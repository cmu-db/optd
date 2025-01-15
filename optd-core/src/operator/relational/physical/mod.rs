//! Type definitions of physical operators in optd.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::filter::Filter;
use join::{hash_join::HashJoin, nl_join::NLJoin, sort_merge_join::SortMergeJoin};
use project::project::Project;
use scan::table_scan::TableScan;

/// Each variant of `PhysicalOperator` represents a specific kind of physical operator.
///
/// This type is generic over two types:
/// - `RelLink`: Specifies whether the children relations are other physical operators or a group
///   id.
/// - `ScalarLink`: Specifies whether the children scalars are other scalar operators or a group id.
///
/// This makes it possible to reuse the `PhysicalOperator` type in [`PhysicalPlan`]
/// and [`PhysicalExpr`].
///
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PhysicalExpr`]: crate::expression::relational::PhysicalExpr
#[derive(Clone)]
pub enum PhysicalOperator<RelLink, ScalarLink> {
    TableScan(TableScan<ScalarLink>),
    Filter(Filter<RelLink, ScalarLink>),
    Project(Project<RelLink, ScalarLink>),
    HashJoin(HashJoin<RelLink, ScalarLink>),
    NLJoin(NLJoin<RelLink, ScalarLink>),
    SortMergeJoin(SortMergeJoin<RelLink, ScalarLink>),
}
