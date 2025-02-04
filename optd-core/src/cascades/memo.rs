//! Memo table interface for query optimization.
//!
//! The memo table is a core data structure that stores expressions and their logical equivalences
//! during query optimization. It serves two main purposes:
//!
//! - Avoiding redundant optimization by memoizing already explored expressions
//! - Grouping logically equivalent expressions together to enable rule-based optimization
//!

use super::expressions::{LogicalExpression, ScalarExpression};
use anyhow::Result;

#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    type RelationalGroupId : Copy;
    type ScalarGroupId : Copy;
    type LogicalExpressionId : Copy;
    type ScalarExpressionId: Copy;
    
    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: Self::RelationalGroupId,
    ) -> Result<Vec<(Self::LogicalExpressionId, LogicalExpression)>>;

    // Returns the group id of new group if merge happened.
    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: Self::RelationalGroupId,
    ) -> Result<Self::RelationalGroupId>;

    // Returns the group id of group if already exists, otherwise creates a new group.
    async fn add_logical_expr(&self, logical_expr: &LogicalExpression) -> Self::RelationalGroupId;

    async fn get_all_scalar_exprs_in_group(
        &self,
        group_id: Self::ScalarGroupId,
    ) -> Result<Vec<(Self::ScalarGroupId, ScalarExpression)>>;

    // Returns the group id of new group if merge happened.
    async fn add_scalar_expr_to_group(
        &self,
        scalar_expr: &ScalarExpression,
        group_id: Self::ScalarGroupId,
    ) -> Result<Self::ScalarGroupId>;

    // Returns the group id of group if already exists, otherwise creates a new group.
    async fn add_scalar_expr(&self, scalar_expr: &ScalarExpression) -> Result<Self::ScalarGroupId>;
}
