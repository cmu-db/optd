//! Module for group expansion in the evaluation system.
//!
//! Provides functionality for expanding group references into concrete
//! operator implementations during expression evaluation. Group expansion
//! bridges the gap between abstract group references in the expression
//! tree and their materialized forms in the optimizer's memo structure.
//!
//! The `Expander` trait defines the core interface for performing
//! expansion operations on logical groups, scalar groups, and physical goals.
//! It also provides access to logical properties of groups, which is essential
//! for cost-based optimization decisions.

use crate::driver::{cascades::Driver, memo::Memoize};
use optd_dsl::analyzer::hir::{Goal, GroupId, Value};
use std::sync::Arc;

/// Defines operations for expanding group references into concrete expressions.
///
/// Serves as a bridge between the evaluation engine and the optimizer, allowing
/// access to materialized expressions when encountering group references during
/// evaluation. The trait provides mechanisms to retrieve logical expressions,
/// physical implementations, and logical properties from the memo structure.
#[trait_variant::make(Send)]
pub(crate) trait Expander: Clone + Send + Sync + 'static {
    /// Expands a logical group into all possible logical operator expressions.
    ///
    /// Retrieves all logical expressions associated with the specified group ID
    /// from the memo structure, allowing the evaluation engine to consider all
    /// alternative implementations during optimization.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the logical group to expand
    ///
    /// # Returns
    /// A future that resolves to a vector of Value objects representing all
    /// logical expressions in the group
    async fn expand_all_exprs(&self, group_id: GroupId) -> Vec<Value>;

    /// Expands a physical goal into its optimal physical implementation.
    ///
    /// Retrieves the winning (lowest cost) physical implementation for a given
    /// physical goal from the memo structure, allowing the evaluation engine to
    /// access the optimal implementation during rule application.
    ///
    /// # Parameters
    /// * `physical_goal` - The physical goal to retrieve the optimal implementation for
    ///
    /// # Returns
    /// A future that resolves to a Value representing the optimal physical expression
    async fn expand_winning_expr(&self, physical_goal: &Goal) -> Value;

    /// Expands a logical group into its corresponding logical properties.
    ///
    /// Retrieves the logical properties associated with the specified group ID
    /// from the memo structure, which include information such as schema, cardinality,
    /// and other metadata that inform optimization decisions.
    ///
    /// # Parameters
    /// * `group_id` - The ID of the logical group to retrieve properties for
    ///
    /// # Returns
    /// A future that resolves to a Value representing the logical properties of the group
    async fn expand_properties(&self, group_id: GroupId) -> Value;
}

/// Implementation of the `Expander` trait for the memoized driver.
///
/// This allows the Driver<M> to act as a bridge between the memo-based optimizer
/// and the expression evaluation system. It provides mechanisms to:
/// 1. Retrieve all logical expressions from a group in the memo
/// 2. Obtain the optimal physical implementation for a given physical goal
/// 3. Access the logical properties of a group.
impl<M: Memoize> Expander for Arc<Driver<M>> {
    async fn expand_all_exprs(&self, group_id: GroupId) -> Vec<Value> {
        todo!()
    }

    async fn expand_winning_expr(&self, physical_goal: &Goal) -> Value {
        todo!()
    }

    async fn expand_properties(&self, group_id: GroupId) -> Value {
        todo!()
    }
}
