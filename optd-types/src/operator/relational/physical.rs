//! Type representations of physical operators in query plans and expressions.

pub mod filter;
pub mod join;
pub mod project;
pub mod scan;

use filter::filter::FilterOperator;
use join::hash_join::HashJoinOperator;
use join::merge_join::MergeJoinOperator;
use join::nl_join::NLJoinOperator;
use project::project::ProjectOperator;
use scan::table_scan::TableScanOperator;

/// A type representing a physical operator in an output physical query execution plan.
///
/// Each variant represents a specific physical implementation:
/// - TableScan: Base table access
/// - Filter: Row filtering
/// - Project: Column selection/transformation
/// - Join implementations:
///   - HashJoin: Hash-based join
///   - MergeJoin: Sort-merge join
///   - NLJoin: Nested loop join
///
/// Generic over two types:
/// - `RelLink`: For relational operator children
/// - `ScalarLink`: For scalar expression children
///
/// Valid in [`PhysicalPlan`] and [`PhysicalExpression`].
pub enum PhysicalOperator<RelLink, ScalarLink> {
    FilterOperator(FilterOperator<RelLink, ScalarLink>),
    HashJoinOperator(HashJoinOperator<RelLink, ScalarLink>),
    MergeJoinOperator(MergeJoinOperator<RelLink, ScalarLink>),
    NLJoinOperator(NLJoinOperator<RelLink, ScalarLink>),
    ProjectOperator(ProjectOperator<RelLink, ScalarLink>),
    TableScanOperator(TableScanOperator),
}
