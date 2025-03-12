use crate::engine::utils::streams::ValueStream;
use optd_dsl::analyzer::hir::{Goal, GroupId, Value};

/// Defines operations for expanding references in the query plan.
///
/// This trait serves as a bridge between the evaluation engine and the optimizer,
/// providing access to materialized expressions and properties stored in the memo.
#[trait_variant::make(Send)]
#[allow(dead_code)]
pub trait Expander: Clone + Send + Sync + 'static {
    /// Retrieves all logical expressions in a group.
    ///
    /// Returns a stream of logical expressions that belong to the specified group.
    fn expand_all_exprs(&self, group_id: GroupId) -> ValueStream;

    /// Retrieves optimal physical implementations for a goal.
    ///
    /// Returns a stream of physical plans that satisfy the specified goal,
    /// with each new plan having lower cost than previous plans.
    fn expand_optimized_expr(&self, physical_goal: &Goal) -> ValueStream;

    /// Retrieves logical properties for a group.
    ///
    /// Returns the derived logical properties (schema, statistics, etc.) for the group.
    async fn retrieve_properties(&self, group_id: GroupId) -> Value;
}
