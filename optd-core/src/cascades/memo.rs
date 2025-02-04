//! Memo table interface for query optimization.
//!
//! The memo table is a core data structure that stores expressions and their logical equivalences
//! during query optimization. It serves two main purposes:
//!
//! - Avoiding redundant optimization by memoizing already explored expressions
//! - Grouping logically equivalent expressions together to enable rule-based optimization
//!

use super::{
    expressions::{LogicalExpression, LogicalExpressionId, ScalarExpression, ScalarExpressionId},
    groups::{RelationalGroupId, ScalarGroupId},
};
use anyhow::Result;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: RelationalGroupId,
    ) -> Result<Vec<(LogicalExpressionId, LogicalExpression)>>;

    // Returns the group id of new group if merge happened.
    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: RelationalGroupId,
    ) -> Result<RelationalGroupId>;

    // Returns the group id of group if already exists, otherwise creates a new group.
    async fn add_logical_expr(&self, logical_expr: &LogicalExpression)
        -> Result<RelationalGroupId>;

    async fn get_all_scalar_exprs_in_group(
        &self,
        group_id: ScalarGroupId,
    ) -> Result<Vec<(ScalarExpressionId, ScalarExpression)>>;

    // Returns the group id of new group if merge happened.
    async fn add_scalar_expr_to_group(
        &self,
        scalar_expr: &ScalarExpression,
        group_id: ScalarGroupId,
    ) -> Result<ScalarGroupId>;

    // Returns the group id of group if already exists, otherwise creates a new group.
    async fn add_scalar_expr(&self, scalar_expr: &ScalarExpression) -> Result<ScalarGroupId>;

    // Merges two relational groups and returns the new group id.
    async fn merge_relation_group(
        &self,
        from: RelationalGroupId,
        to: RelationalGroupId,
    ) -> Result<RelationalGroupId>;

    // Merges two scalar groups and returns the new group id.
    async fn merge_scalar_group(
        &self,
        from: ScalarGroupId,
        to: ScalarGroupId,
    ) -> Result<ScalarGroupId>;
}
